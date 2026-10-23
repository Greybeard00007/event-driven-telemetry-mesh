"""Sliding window buffer for rig sensor telemetry context."""

from collections import deque
from datetime import datetime
import math
from typing import Any, Dict, List, Optional
from telemetry_mesh.config import settings
from telemetry_mesh.models.telemetry import RigTelemetry


class RigSlidingWindow:
    """Maintains a rolling window of recent sensor telemetry records for an individual rig."""

    def __init__(self, rig_id: str, max_samples: Optional[int] = None):
        self.rig_id = rig_id
        self.max_samples = max_samples or settings.WINDOW_SIZE_SAMPLES
        self.buffer: deque[RigTelemetry] = deque(maxlen=self.max_samples)

    def append(self, telemetry: RigTelemetry) -> None:
        """Add new sample to rolling window."""
        self.buffer.append(telemetry)

    def get_records(self) -> List[RigTelemetry]:
        """Return list of buffered records in chronological order."""
        return list(self.buffer)

    def size(self) -> int:
        return len(self.buffer)

    def compute_stats(self, channel: str) -> Dict[str, float]:
        """Calculate summary statistics (mean, std, min, max, delta) for a given sensor channel."""
        vals: List[float] = []
        for r in self.buffer:
            v = getattr(r, channel, None)
            if v is not None and isinstance(v, (int, float)):
                vals.append(float(v))

        if not vals:
            return {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0, "delta": 0.0, "count": 0}

        count = len(vals)
        mean_val = sum(vals) / count
        variance = sum((x - mean_val) ** 2 for x in vals) / count if count > 1 else 0.0
        std_val = math.sqrt(variance)
        min_val = min(vals)
        max_val = max(vals)
        delta_val = vals[-1] - vals[0] if count > 1 else 0.0

        return {
            "mean": round(mean_val, 2),
            "std": round(std_val, 2),
            "min": round(min_val, 2),
            "max": round(max_val, 2),
            "delta": round(delta_val, 2),
            "count": count,
        }

    def extract_window_summary(self) -> Dict[str, Any]:
        """Generate high-level statistical snapshot across all core drilling channels."""
        channels = ["wob", "rpm", "torque", "flow_in", "flow_out", "spp", "rop"]
        stats = {ch: self.compute_stats(ch) for ch in channels}
        
        # Recent samples (last 5)
        recent_samples = [
            {
                "timestamp": r.timestamp.isoformat(),
                "depth_md": r.depth_md,
                "wob": r.wob,
                "rpm": r.rpm,
                "torque": r.torque,
                "flow_in": r.flow_in,
                "flow_out": r.flow_out,
                "spp": r.spp,
                "rop": r.rop,
            }
            for r in list(self.buffer)[-5:]
        ]

        return {
            "rig_id": self.rig_id,
            "window_size": len(self.buffer),
            "channels": stats,
            "recent_readings": recent_samples,
        }


class WindowManager:
    """Manages sliding windows across multiple rigs."""

    def __init__(self, max_samples: Optional[int] = None):
        self.max_samples = max_samples or settings.WINDOW_SIZE_SAMPLES
        self._windows: Dict[str, RigSlidingWindow] = {}

    def get_window(self, rig_id: str) -> RigSlidingWindow:
        if rig_id not in self._windows:
            self._windows[rig_id] = RigSlidingWindow(rig_id, self.max_samples)
        return self._windows[rig_id]

    def add(self, telemetry: RigTelemetry) -> None:
        window = self.get_window(telemetry.rig_id)
        window.append(telemetry)
