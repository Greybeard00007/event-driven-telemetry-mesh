"""Deterministic physics threshold evaluation for drilling telemetry."""

import logging
from typing import Any, Dict, List, Optional
from telemetry_mesh.config import settings
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.sliding_window import RigSlidingWindow

logger = logging.getLogger(__name__)


class ThresholdEngine:
    """Evaluates telemetry records and window statistics against deterministic rules."""

    def __init__(
        self,
        wob_max: Optional[float] = None,
        torque_max: Optional[float] = None,
        spp_max: Optional[float] = None,
        flow_delta_percent: Optional[float] = None,
    ):
        self.wob_max = wob_max or settings.THRESHOLD_WOB_MAX
        self.torque_max = torque_max or settings.THRESHOLD_TORQUE_MAX
        self.spp_max = spp_max or settings.THRESHOLD_SPP_MAX
        self.flow_delta_percent = flow_delta_percent or settings.THRESHOLD_FLOW_DELTA_PERCENT

    def evaluate(
        self,
        current: RigTelemetry,
        window: RigSlidingWindow,
    ) -> Optional[AnomalyEvent]:
        """Evaluate current telemetry and sliding window context for operational anomalies."""
        rpm_stats = window.compute_stats("rpm")
        wob_stats = window.compute_stats("wob")
        torque_stats = window.compute_stats("torque")
        spp_stats = window.compute_stats("spp")
        flow_in_stats = window.compute_stats("flow_in")
        flow_out_stats = window.compute_stats("flow_out")

        # Snapshot of current key metrics
        metrics_snapshot = {
            "wob": current.wob,
            "rpm": current.rpm,
            "torque": current.torque,
            "flow_in": current.flow_in,
            "flow_out": current.flow_out,
            "spp": current.spp,
            "rop": current.rop,
        }

        # Helper to construct AnomalyEvent
        def build_event(
            atype: AnomalyType,
            severity: AnomalySeverity,
            rule: str,
            details: Dict[str, Any],
        ) -> AnomalyEvent:
            recent_readings = [
                {
                    "timestamp": r.timestamp.isoformat(),
                    "wob": r.wob,
                    "rpm": r.rpm,
                    "torque": r.torque,
                    "flow_in": r.flow_in,
                    "flow_out": r.flow_out,
                    "spp": r.spp,
                    "rop": r.rop,
                }
                for r in list(window.get_records())[-8:]
            ]
            return AnomalyEvent(
                rig_id=current.rig_id,
                well_id=current.well_id,
                timestamp=current.timestamp,
                anomaly_type=atype,
                severity=severity,
                rule_triggered=rule,
                threshold_details=details,
                current_metrics=metrics_snapshot,
                window_metrics=recent_readings,
            )

        # 1. Critical Formation Fluid Influx / Kick Detection
        # Positive delta: Flow Out exceeds Flow In by more than threshold percentage
        if current.flow_in > 100.0:
            flow_diff_ratio = (current.flow_out - current.flow_in) / current.flow_in
            if flow_diff_ratio > (self.flow_delta_percent / 100.0):
                return build_event(
                    AnomalyType.KICK_INFLUX,
                    AnomalySeverity.CRITICAL,
                    "FLOW_OUT_EXCEEDS_FLOW_IN_INFLUX_RULE",
                    {
                        "flow_in": current.flow_in,
                        "flow_out": current.flow_out,
                        "delta_percent": round(flow_diff_ratio * 100, 1),
                        "threshold_percent": self.flow_delta_percent,
                    },
                )

        # 2. Lost Circulation
        # Return flow drops below 75% of input flow
        if current.flow_in > 150.0:
            if current.flow_out < (current.flow_in * 0.75):
                return build_event(
                    AnomalyType.LOST_CIRCULATION,
                    AnomalySeverity.HIGH,
                    "FLOW_OUT_COLLAPSE_LOST_CIRCULATION_RULE",
                    {
                        "flow_in": current.flow_in,
                        "flow_out": current.flow_out,
                        "deficit_gpm": round(current.flow_in - current.flow_out, 1),
                    },
                )

        # 3. Pack-off / Annulus Restriction
        # SPP sudden rise above baseline with drop in flow_out
        if spp_stats["count"] >= 5 and flow_in_stats["mean"] > 150.0:
            spp_delta = current.spp - spp_stats["mean"]
            if (current.spp > self.spp_max * 0.9 or spp_delta > 600.0) and (current.flow_out < current.flow_in * 0.9):
                return build_event(
                    AnomalyType.PACK_OFF,
                    AnomalySeverity.HIGH,
                    "SPP_SPIKE_FLOW_RESTRICTION_PACK_OFF_RULE",
                    {
                        "current_spp": current.spp,
                        "mean_spp": spp_stats["mean"],
                        "spp_delta": round(spp_delta, 1),
                        "flow_out_ratio": round(current.flow_out / max(current.flow_in, 1.0), 2),
                    },
                )

        # 4. Mud Motor / Drillstring Stall
        # RPM collapses near zero while torque exceeds limits
        if current.rpm < 12.0 and current.torque > self.torque_max:
            return build_event(
                AnomalyType.MOTOR_STALL,
                AnomalySeverity.HIGH,
                "RPM_COLLAPSE_TORQUE_SPIKE_MOTOR_STALL_RULE",
                {
                    "rpm": current.rpm,
                    "torque": current.torque,
                    "torque_threshold": self.torque_max,
                },
            )

        # 5. Stick-Slip Severe Torsional Vibration
        # High RPM standard deviation with cyclic torque fluctuations
        if rpm_stats["count"] >= 8:
            rpm_variation_coeff = rpm_stats["std"] / max(rpm_stats["mean"], 10.0)
            if rpm_variation_coeff > 0.55 or (rpm_stats["max"] - rpm_stats["min"] > 120.0 and torque_stats["std"] > 6.0):
                return build_event(
                    AnomalyType.STICK_SLIP,
                    AnomalySeverity.MEDIUM if rpm_variation_coeff < 0.75 else AnomalySeverity.HIGH,
                    "RPM_TORSIONAL_VARIANCE_STICK_SLIP_RULE",
                    {
                        "rpm_std": rpm_stats["std"],
                        "rpm_mean": rpm_stats["mean"],
                        "rpm_cv": round(rpm_variation_coeff, 2),
                        "torque_std": torque_stats["std"],
                    },
                )

        # 6. Excessive Weight on Bit (WOB)
        if current.wob > self.wob_max:
            return build_event(
                AnomalyType.EXCESSIVE_WOB,
                AnomalySeverity.MEDIUM if current.wob < self.wob_max * 1.15 else AnomalySeverity.HIGH,
                "WOB_EXCEEDS_MAX_THRESHOLD_RULE",
                {
                    "wob": current.wob,
                    "wob_max_threshold": self.wob_max,
                    "excess_klbf": round(current.wob - self.wob_max, 1),
                },
            )

        # 7. Bit Bounce / Axial Vibration
        if wob_stats["count"] >= 10 and wob_stats["std"] > 14.0 and current.rop < 20.0:
            return build_event(
                AnomalyType.BIT_BOUNCE,
                AnomalySeverity.MEDIUM,
                "WOB_AXIAL_VARIANCE_BIT_BOUNCE_RULE",
                {
                    "wob_std": wob_stats["std"],
                    "wob_mean": wob_stats["mean"],
                    "current_rop": current.rop,
                },
            )

        return None
