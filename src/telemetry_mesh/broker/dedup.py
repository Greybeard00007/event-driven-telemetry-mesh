"""Deduplication and idempotency engine for sensor telemetry ticks."""

import hashlib
import time
from typing import Dict, Optional, Set
from telemetry_mesh.models.telemetry import RigTelemetry


class DeduplicationEngine:
    """Sliding-window idempotency filter preventing duplicate processing of identical sensor frames."""

    def __init__(self, window_seconds: float = 60.0, max_entries: int = 50_000):
        self.window_seconds = window_seconds
        self.max_entries = max_entries
        self._seen: Dict[str, float] = {}  # hash -> expiry timestamp

    @staticmethod
    def compute_signature(record: RigTelemetry) -> str:
        """Generate a SHA-256 fingerprint for a telemetry measurement."""
        key = f"{record.rig_id}:{record.timestamp.isoformat()}:{record.depth_md:.2f}:{record.wob:.1f}:{record.rpm:.1f}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def is_duplicate(self, record: RigTelemetry) -> bool:
        """Check if record has already been processed within the dedup window."""
        now = time.time()
        self._cleanup(now)

        sig = self.compute_signature(record)
        if sig in self._seen:
            return True

        if len(self._seen) >= self.max_entries:
            # Force purge oldest 20%
            sorted_items = sorted(self._seen.items(), key=lambda x: x[1])
            for k, _ in sorted_items[: int(self.max_entries * 0.2)]:
                self._seen.pop(k, None)

        self._seen[sig] = now + self.window_seconds
        return False

    def _cleanup(self, now: float) -> None:
        """Periodically purge expired entries."""
        expired = [k for k, exp in self._seen.items() if exp < now]
        for k in expired:
            self._seen.pop(k, None)

    def size(self) -> int:
        return len(self._seen)
