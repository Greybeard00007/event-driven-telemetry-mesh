"""WITSML and Telemetry Ingestion Parsers."""

import json
import logging
from typing import Any, Dict, List, Union
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.models.witsml import WitsmlLog

logger = logging.getLogger(__name__)


class TelemetryParser:
    """Parses WITSML XML, JSON arrays, and micro-batches into RigTelemetry objects."""

    @staticmethod
    def parse_witsml_xml(xml_content: str) -> List[RigTelemetry]:
        """Parse WITSML 1.3.1 / 1.4.1.1 XML log string into a list of RigTelemetry records."""
        try:
            witsml_log = WitsmlLog.from_xml(xml_content)
            return witsml_log.to_telemetry_records()
        except Exception as e:
            logger.error(f"Failed to parse WITSML XML payload: {e}")
            raise ValueError(f"Invalid WITSML XML structure: {e}") from e

    @staticmethod
    def parse_json_payload(data: Union[str, bytes, Dict[str, Any], List[Dict[str, Any]]]) -> List[RigTelemetry]:
        """Parse JSON telemetry string or dict/list structure."""
        if isinstance(data, (str, bytes)):
            try:
                parsed = json.loads(data)
            except Exception as e:
                raise ValueError(f"Malformed JSON payload: {e}") from e
        else:
            parsed = data

        records: List[RigTelemetry] = []

        if isinstance(parsed, dict):
            # Check if wrapped in "records", "data", or "telemetry"
            if "records" in parsed and isinstance(parsed["records"], list):
                target_list = parsed["records"]
            elif "data" in parsed and isinstance(parsed["data"], list):
                target_list = parsed["data"]
            else:
                target_list = [parsed]
        elif isinstance(parsed, list):
            target_list = parsed
        else:
            raise ValueError(f"Expected dict or list JSON payload, got {type(parsed)}")

        for item in target_list:
            if not isinstance(item, dict):
                continue
            records.append(RigTelemetry.model_validate(item))

        return records
