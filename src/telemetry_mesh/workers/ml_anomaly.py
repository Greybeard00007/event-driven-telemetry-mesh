"""Streaming unsupervised anomaly and drift detection for real-time drilling telemetry."""

from datetime import datetime, timezone
import math
from typing import Dict, List, Optional
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry


class StreamingChannelTracker:
    """Tracks running exponential moving average and variance using Welford's algorithm."""

    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha  # Smoothing factor
        self.mean: Optional[float] = None
        self.variance: float = 0.0
        self.count: int = 0

    def update(self, val: float) -> float:
        """Update tracker with new sample and return current standard score (Z-score)."""
        self.count += 1
        if self.mean is None:
            self.mean = val
            self.variance = 1.0
            return 0.0

        delta = val - self.mean
        # Exponential update
        self.mean += self.alpha * delta
        self.variance = (1.0 - self.alpha) * (self.variance + self.alpha * (delta**2))
        std = math.sqrt(max(self.variance, 1e-4))
        z_score = abs(val - self.mean) / std
        return z_score


class StreamingMlDetector:
    """Multi-channel unsupervised anomaly detector identifying subtle drift and gradual tool wear."""

    def __init__(self, z_threshold: float = 3.5):
        self.z_threshold = z_threshold
        # Per-rig channel trackers: rig_id -> channel -> StreamingChannelTracker
        self._trackers: Dict[str, Dict[str, StreamingChannelTracker]] = {}

    def _get_rig_trackers(self, rig_id: str) -> Dict[str, StreamingChannelTracker]:
        if rig_id not in self._trackers:
            self._trackers[rig_id] = {
                "wob": StreamingChannelTracker(alpha=0.03),
                "torque": StreamingChannelTracker(alpha=0.03),
                "rpm": StreamingChannelTracker(alpha=0.03),
                "spp": StreamingChannelTracker(alpha=0.02),
                "flow_in": StreamingChannelTracker(alpha=0.02),
                "flow_out": StreamingChannelTracker(alpha=0.02),
                "rop": StreamingChannelTracker(alpha=0.04),
            }
        return self._trackers[rig_id]

    def evaluate(self, current: RigTelemetry) -> Optional[AnomalyEvent]:
        """Evaluate telemetry sample for statistical drift anomalies."""
        trackers = self._get_rig_trackers(current.rig_id)

        z_scores = {
            "wob": trackers["wob"].update(current.wob),
            "torque": trackers["torque"].update(current.torque),
            "rpm": trackers["rpm"].update(current.rpm),
            "spp": trackers["spp"].update(current.spp),
            "flow_in": trackers["flow_in"].update(current.flow_in),
            "flow_out": trackers["flow_out"].update(current.flow_out),
            "rop": trackers["rop"].update(current.rop),
        }

        # Need at least 25 samples for statistical stability
        if trackers["wob"].count < 25:
            return None

        # 1. Early Washout Detection: SPP drifting downwards while flow_in is steady
        spp_z = z_scores["spp"]
        spp_mean = trackers["spp"].mean or 0.0
        flow_z = z_scores["flow_in"]
        if spp_z > 2.8 and current.spp < (spp_mean - 150.0) and flow_z < 1.0:
            return AnomalyEvent(
                rig_id=current.rig_id,
                well_id=current.well_id,
                timestamp=current.timestamp,
                anomaly_type=AnomalyType.SENSOR_DRIFT,
                severity=AnomalySeverity.MEDIUM,
                rule_triggered="ML_GRADUAL_SPP_WASHOUT_DRIFT",
                threshold_details={
                    "spp_z_score": round(spp_z, 2),
                    "spp_mean": round(spp_mean, 1),
                    "spp_current": current.spp,
                    "flow_stability_z": round(flow_z, 2),
                },
                current_metrics={"spp": current.spp, "flow_in": current.flow_in},
            )

        # 2. Multi-channel compound anomaly (composite Z > threshold)
        high_z_channels = {ch: round(z, 2) for ch, z in z_scores.items() if z > self.z_threshold}
        if len(high_z_channels) >= 2:
            return AnomalyEvent(
                rig_id=current.rig_id,
                well_id=current.well_id,
                timestamp=current.timestamp,
                anomaly_type=AnomalyType.SENSOR_DRIFT,
                severity=AnomalySeverity.MEDIUM,
                rule_triggered="ML_MULTIVARIATE_DRIFT_DETECTED",
                threshold_details={"divergent_channels": high_z_channels},
                current_metrics={ch: getattr(current, ch) for ch in high_z_channels},
            )

        return None
