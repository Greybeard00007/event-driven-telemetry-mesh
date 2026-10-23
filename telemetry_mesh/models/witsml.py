"""WITSML XML models and conversion utilities."""

from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

from telemetry_mesh.models.telemetry import RigTelemetry


class WitsmlCurveInfo(BaseModel):
    """Mnemonic and unit description for a WITSML log curve."""

    mnemonic: str
    unit: Optional[str] = None
    curve_description: Optional[str] = None


class WitsmlLog(BaseModel):
    """Represents a WITSML log object containing curve definitions and data rows."""

    name_well: str
    name_wellbore: str
    name_rig: Optional[str] = None
    index_type: str = "dateTime"
    curves: List[WitsmlCurveInfo] = Field(default_factory=list)
    data_rows: List[str] = Field(default_factory=list)

    @classmethod
    def from_xml(cls, xml_content: str) -> "WitsmlLog":
        """Parse WITSML XML string into a WitsmlLog structure."""
        # Strip default namespaces for easier element access
        content = xml_content.strip()
        root = ET.fromstring(content)

        # Handle namespaces if present
        def find_el(elem: ET.Element, tag_name: str) -> Optional[ET.Element]:
            for child in elem.iter():
                if child.tag.split("}")[-1] == tag_name:
                    return child
            return None

        def find_all_el(elem: ET.Element, tag_name: str) -> List[ET.Element]:
            return [child for child in elem.iter() if child.tag.split("}")[-1] == tag_name]

        log_node = find_el(root, "log")
        if log_node is None:
            log_node = root  # Root itself might be <log>

        name_well_el = find_el(log_node, "nameWell")
        name_wellbore_el = find_el(log_node, "nameWellbore")
        name_rig_el = find_el(log_node, "nameRig")
        index_type_el = find_el(log_node, "indexType")

        name_well = name_well_el.text.strip() if (name_well_el is not None and name_well_el.text) else "WELL-001"
        name_wellbore = name_wellbore_el.text.strip() if (name_wellbore_el is not None and name_wellbore_el.text) else "WB-001"
        name_rig = name_rig_el.text.strip() if (name_rig_el is not None and name_rig_el.text) else "RIG-001"
        index_type = index_type_el.text.strip() if (index_type_el is not None and index_type_el.text) else "dateTime"

        # Extract logCurveInfo
        curves: List[WitsmlCurveInfo] = []
        for curve_el in find_all_el(log_node, "logCurveInfo"):
            mnem_el = find_el(curve_el, "mnemonic")
            unit_el = find_el(curve_el, "unit")
            desc_el = find_el(curve_el, "curveDescription")
            if mnem_el is not None and mnem_el.text:
                curves.append(
                    WitsmlCurveInfo(
                        mnemonic=mnem_el.text.strip().upper(),
                        unit=unit_el.text.strip() if (unit_el is not None and unit_el.text) else None,
                        curve_description=desc_el.text.strip() if (desc_el is not None and desc_el.text) else None,
                    )
                )

        # Extract data rows from <logData><data>
        data_rows: List[str] = []
        log_data_el = find_el(log_node, "logData")
        if log_data_el is not None:
            for d in find_all_el(log_data_el, "data"):
                if d.text:
                    data_rows.append(d.text.strip())

        return cls(
            name_well=name_well,
            name_wellbore=name_wellbore,
            name_rig=name_rig,
            index_type=index_type,
            curves=curves,
            data_rows=data_rows,
        )

    def to_telemetry_records(self) -> List[RigTelemetry]:
        """Convert WITSML log curves and comma-separated rows into RigTelemetry instances."""
        mnemonics = [c.mnemonic for c in self.curves]
        records: List[RigTelemetry] = []

        # Canonical standard mapping
        mapping = {
            "TIME": "timestamp",
            "DATETIME": "timestamp",
            "DATE_TIME": "timestamp",
            "INDEX": "timestamp",
            "MD": "depth_md",
            "DEPTH": "depth_md",
            "DEPT": "depth_md",
            "TVD": "depth_tvd",
            "WOB": "wob",
            "SWOB": "wob",
            "RPM": "rpm",
            "SRPM": "rpm",
            "TORQ": "torque",
            "TORQUE": "torque",
            "STOR": "torque",
            "FLOWIN": "flow_in",
            "FLOW_IN": "flow_in",
            "FLOWRATE": "flow_in",
            "FLOWOUT": "flow_out",
            "FLOW_OUT": "flow_out",
            "ROPA": "rop",
            "ROP": "rop",
            "SPP": "spp",
            "PUMPPRESS": "spp",
            "GR": "gamma",
            "GAMMA": "gamma",
            "GAS": "mud_gas",
            "MUDGAS": "mud_gas",
            "PITVOL": "active_pit_vol",
        }

        col_index: Dict[str, int] = {}
        for idx, mnem in enumerate(mnemonics):
            clean_mnem = mnem.upper().replace(" ", "").replace("-", "")
            for pattern, field_name in mapping.items():
                if clean_mnem == pattern or clean_mnem.startswith(pattern):
                    col_index[field_name] = idx
                    break

        for row in self.data_rows:
            parts = [p.strip() for p in row.split(",")]
            if len(parts) < len(mnemonics):
                continue

            def get_val(field: str) -> Optional[str]:
                if field in col_index and col_index[field] < len(parts):
                    val = parts[col_index[field]]
                    return val if val != "" else None
                return None

            def parse_f(val: Optional[str]) -> Optional[float]:
                if val is None:
                    return None
                try:
                    return float(val)
                except ValueError:
                    return None

            # Timestamp parsing
            raw_ts = get_val("timestamp")
            if raw_ts:
                try:
                    ts = datetime.fromisoformat(raw_ts)
                except ValueError:
                    ts = datetime.now(timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            record = RigTelemetry(
                rig_id=self.name_rig or "RIG-001",
                well_id=self.name_well,
                timestamp=ts,
                depth_md=parse_f(get_val("depth_md")) or 0.0,
                depth_tvd=parse_f(get_val("depth_tvd")),
                wob=parse_f(get_val("wob")) or 0.0,
                rpm=parse_f(get_val("rpm")) or 0.0,
                torque=parse_f(get_val("torque")) or 0.0,
                flow_in=parse_f(get_val("flow_in")) or 0.0,
                flow_out=parse_f(get_val("flow_out")) or 0.0,
                spp=parse_f(get_val("spp")) or 0.0,
                rop=parse_f(get_val("rop")) or 0.0,
                gamma=parse_f(get_val("gamma")),
                mud_gas=parse_f(get_val("mud_gas")),
                active_pit_vol=parse_f(get_val("active_pit_vol")),
            )
            records.append(record)

        return records

    @staticmethod
    def generate_witsml_xml(
        rig_id: str,
        well_id: str,
        records: List[RigTelemetry],
    ) -> str:
        """Create a valid WITSML 1.4.1.1 XML payload from RigTelemetry records."""
        mnemonics = ["TIME", "MD", "WOB", "RPM", "TORQ", "FLOWIN", "FLOWOUT", "SPP", "ROP", "GR"]
        units = ["date", "m", "klbf", "rpm", "kft.lbf", "gpm", "gpm", "psi", "m/h", "gAPI"]

        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<logs xmlns="http://www.witsml.org/schemas/1series" version="1.4.1.1">',
            f'  <log uidWell="{well_id}" uidWellbore="WB-1" uid="LOG-1">',
            f"    <nameWell>{well_id}</nameWell>",
            f"    <nameWellbore>WB-1</nameWellbore>",
            f"    <nameRig>{rig_id}</nameRig>",
            "    <indexType>dateTime</indexType>",
        ]

        for mnem, unit in zip(mnemonics, units):
            lines.append("    <logCurveInfo>")
            lines.append(f"      <mnemonic>{mnem}</mnemonic>")
            lines.append(f"      <unit>{unit}</unit>")
            lines.append(f"      <curveDescription>{mnem} Channel</curveDescription>")
            lines.append("    </logCurveInfo>")

        lines.append("    <logData>")
        for r in records:
            ts_str = r.timestamp.isoformat()
            gr_str = f"{r.gamma:.1f}" if r.gamma is not None else ""
            row_vals = [
                ts_str,
                f"{r.depth_md:.2f}",
                f"{r.wob:.2f}",
                f"{r.rpm:.2f}",
                f"{r.torque:.2f}",
                f"{r.flow_in:.2f}",
                f"{r.flow_out:.2f}",
                f"{r.spp:.2f}",
                f"{r.rop:.2f}",
                gr_str,
            ]
            lines.append(f"      <data>{','.join(row_vals)}</data>")

        lines.append("    </logData>")
        lines.append("  </log>")
        lines.append("</logs>")
        return "\n".join(lines)
