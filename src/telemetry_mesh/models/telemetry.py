"""Telemetry data models for drilling rig sensor readings."""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class RigTelemetry(BaseModel):
    """Normalized real-time drilling sensor measurement."""

    rig_id: str = Field(..., description="Unique identifier for the drilling rig, e.g. RIG-ALPHA")
    well_id: str = Field(..., description="Unique identifier for the wellbore, e.g. WELL-302B")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the sensor measurement",
    )
    depth_md: float = Field(..., ge=0.0, description="Measured depth in feet or meters")
    depth_tvd: Optional[float] = Field(None, ge=0.0, description="True vertical depth")
    wob: float = Field(..., ge=0.0, description="Weight on Bit in kilo-pounds force (klbf)")
    rpm: float = Field(..., ge=0.0, description="Surface drill string rotation speed in RPM")
    torque: float = Field(..., ge=0.0, description="Surface drill string torque in kft-lbf")
    flow_in: float = Field(..., ge=0.0, description="Mud pump flow rate in gallons per minute (gpm)")
    flow_out: float = Field(..., ge=0.0, description="Mud return flow rate in gallons per minute (gpm)")
    spp: float = Field(..., ge=0.0, description="Standpipe pressure in psi")
    rop: float = Field(..., ge=0.0, description="Rate of penetration in ft/hr or m/hr")
    gamma: Optional[float] = Field(None, ge=0.0, description="Gamma ray log reading in API units")
    mud_gas: Optional[float] = Field(None, ge=0.0, description="Mud gas content in ppm")
    active_pit_vol: Optional[float] = Field(None, ge=0.0, description="Active mud pit volume in bbl")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                # Support standard timestamp strings
                return datetime.strptime(v, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return v

    def to_redis_dict(self) -> Dict[str, str]:
        """Convert telemetry instance to a Redis Stream key-value dict (all strings)."""
        return {
            "rig_id": self.rig_id,
            "well_id": self.well_id,
            "timestamp": self.timestamp.isoformat(),
            "depth_md": f"{self.depth_md:.2f}",
            "depth_tvd": f"{self.depth_tvd:.2f}" if self.depth_tvd is not None else "",
            "wob": f"{self.wob:.2f}",
            "rpm": f"{self.rpm:.2f}",
            "torque": f"{self.torque:.2f}",
            "flow_in": f"{self.flow_in:.2f}",
            "flow_out": f"{self.flow_out:.2f}",
            "spp": f"{self.spp:.2f}",
            "rop": f"{self.rop:.2f}",
            "gamma": f"{self.gamma:.2f}" if self.gamma is not None else "",
            "mud_gas": f"{self.mud_gas:.2f}" if self.mud_gas is not None else "",
            "active_pit_vol": f"{self.active_pit_vol:.2f}" if self.active_pit_vol is not None else "",
        }

    @classmethod
    def from_redis_dict(cls, data: Dict[str | bytes, str | bytes]) -> "RigTelemetry":
        """Reconstruct RigTelemetry from Redis Stream fields."""
        normalized: Dict[str, Any] = {}
        for k, v in data.items():
            key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            normalized[key_str] = val_str

        def parse_float(val: Optional[str]) -> Optional[float]:
            if val is None or val == "" or val.lower() == "none":
                return None
            try:
                return float(val)
            except ValueError:
                return None

        return cls(
            rig_id=normalized.get("rig_id", "UNKNOWN"),
            well_id=normalized.get("well_id", "UNKNOWN"),
            timestamp=normalized.get("timestamp", datetime.now(timezone.utc)),
            depth_md=parse_float(normalized.get("depth_md")) or 0.0,
            depth_tvd=parse_float(normalized.get("depth_tvd")),
            wob=parse_float(normalized.get("wob")) or 0.0,
            rpm=parse_float(normalized.get("rpm")) or 0.0,
            torque=parse_float(normalized.get("torque")) or 0.0,
            flow_in=parse_float(normalized.get("flow_in")) or 0.0,
            flow_out=parse_float(normalized.get("flow_out")) or 0.0,
            spp=parse_float(normalized.get("spp")) or 0.0,
            rop=parse_float(normalized.get("rop")) or 0.0,
            gamma=parse_float(normalized.get("gamma")),
            mud_gas=parse_float(normalized.get("mud_gas")),
            active_pit_vol=parse_float(normalized.get("active_pit_vol")),
        )


class TelemetryBatch(BaseModel):
    """Batch of telemetry records submitted via API or stream."""

    source: str = Field(default="api_ingest", description="Source of telemetry batch")
    records: List[RigTelemetry] = Field(..., description="List of rig telemetry records")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
