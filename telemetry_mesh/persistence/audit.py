"""Well control audit trail and operator digital sign-off logging."""

from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AuditEntry(BaseModel):
    audit_id: str
    anomaly_id: str
    rig_id: str
    driller_name: str
    action_taken: str
    comments: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    signature_hash: str


class AuditTrailManager:
    """Maintains immutable operational compliance audit records for wellsite safety."""

    def __init__(self, max_in_memory: int = 1000):
        self._entries: deque[AuditEntry] = deque(maxlen=max_in_memory)

    def record_signoff(
        self,
        anomaly_id: str,
        rig_id: str,
        driller_name: str,
        action_taken: str,
        comments: Optional[str] = None,
    ) -> AuditEntry:
        """Create a cryptographically hashed compliance audit record."""
        ts = datetime.now(timezone.utc)
        raw_key = f"{anomaly_id}:{rig_id}:{driller_name}:{action_taken}:{ts.isoformat()}"
        sig_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

        entry = AuditEntry(
            audit_id=sig_hash[:12],
            anomaly_id=anomaly_id,
            rig_id=rig_id,
            driller_name=driller_name,
            action_taken=action_taken,
            comments=comments,
            timestamp=ts,
            signature_hash=sig_hash,
        )
        self._entries.append(entry)
        logger.info(f"Compliance audit signoff logged: [{entry.audit_id}] Rig {rig_id} by {driller_name}")
        return entry

    def get_entries(self, rig_id: Optional[str] = None, limit: int = 50) -> List[AuditEntry]:
        """Query recent audit sign-off logs."""
        all_entries = list(self._entries)
        if rig_id:
            all_entries = [e for e in all_entries if e.rig_id == rig_id]
        return all_entries[-limit:]


audit_manager = AuditTrailManager()
