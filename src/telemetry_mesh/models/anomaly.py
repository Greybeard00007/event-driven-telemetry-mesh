"""Anomaly event and diagnosis models."""

from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field

from telemetry_mesh.models.telemetry import RigTelemetry


class AnomalyType(str, Enum):
    """Rig operational anomaly classifications."""

    STICK_SLIP = "STICK_SLIP"
    PACK_OFF = "PACK_OFF"
    KICK_INFLUX = "KICK_INFLUX"
    LOST_CIRCULATION = "LOST_CIRCULATION"
    BIT_BOUNCE = "BIT_BOUNCE"
    MOTOR_STALL = "MOTOR_STALL"
    EXCESSIVE_WOB = "EXCESSIVE_WOB"
    PUMP_FAILURE = "PUMP_FAILURE"
    SENSOR_DRIFT = "SENSOR_DRIFT"


class AnomalySeverity(str, Enum):
    """Severity ratings for drilling anomalies."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class LLMDiagnosis(BaseModel):
    """Enriched diagnostic assessment produced by local Ollama LLM."""

    model_name: str = Field(default="ollama-default")
    root_cause_analysis: str = Field(..., description="Petroleum engineering explanation of the anomaly")
    drilling_recommendation: str = Field(..., description="Actionable immediate rig crew instructions")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    urgency_level: AnomalySeverity = Field(..., description="Urgency classification")
    preventive_measures: List[str] = Field(default_factory=list, description="Longer term mitigations")
    inferred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_response: Optional[str] = None


class AnomalyEvent(BaseModel):
    """Complete anomaly event including sensor window and LLM diagnosis."""

    anomaly_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rig_id: str
    well_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    rule_triggered: str
    threshold_details: Dict[str, Any] = Field(default_factory=dict)
    current_metrics: Dict[str, float] = Field(default_factory=dict)
    window_metrics: List[Dict[str, Any]] = Field(default_factory=list)
    llm_diagnosis: Optional[LLMDiagnosis] = None
    acknowledged: bool = False

    def to_redis_dict(self) -> Dict[str, str]:
        """Convert anomaly event into string key-value pairs for Redis Streams."""
        return {
            "anomaly_id": self.anomaly_id,
            "rig_id": self.rig_id,
            "well_id": self.well_id,
            "timestamp": self.timestamp.isoformat(),
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "rule_triggered": self.rule_triggered,
            "payload_json": self.model_dump_json(),
        }

    @classmethod
    def from_redis_dict(cls, data: Dict[str | bytes, str | bytes]) -> "AnomalyEvent":
        """Reconstruct AnomalyEvent from Redis Stream fields."""
        normalized: Dict[str, str] = {}
        for k, v in data.items():
            key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            normalized[key_str] = val_str

        if "payload_json" in normalized:
            try:
                return cls.model_validate_json(normalized["payload_json"])
            except Exception:
                pass

        return cls(
            anomaly_id=normalized.get("anomaly_id", str(uuid.uuid4())),
            rig_id=normalized.get("rig_id", "UNKNOWN"),
            well_id=normalized.get("well_id", "UNKNOWN"),
            timestamp=datetime.fromisoformat(normalized.get("timestamp", datetime.now(timezone.utc).isoformat())),
            anomaly_type=AnomalyType(normalized.get("anomaly_type", AnomalyType.STICK_SLIP.value)),
            severity=AnomalySeverity(normalized.get("severity", AnomalySeverity.MEDIUM.value)),
            rule_triggered=normalized.get("rule_triggered", "THRESHOLD_DETECTED"),
        )
