"""Utility script to seed synthetic WITSML XML or JSON telemetry into the ingestion mesh."""

import argparse
import asyncio
from datetime import datetime, timezone
import json
import sys
import httpx

from telemetry_mesh.models.anomaly import AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.models.witsml import WitsmlLog


def generate_records(rig_id: str, well_id: str, count: int = 20) -> list[RigTelemetry]:
    records = []
    base_depth = 8500.0
    for i in range(count):
        records.append(
            RigTelemetry(
                rig_id=rig_id,
                well_id=well_id,
                timestamp=datetime.now(timezone.utc),
                depth_md=base_depth + i * 0.5,
                depth_tvd=(base_depth + i * 0.5) * 0.92,
                wob=28.0 + (i % 5),
                rpm=120.0 + (i % 10),
                torque=15.0 + (i % 3),
                flow_in=620.0,
                flow_out=618.0,
                spp=2950.0,
                rop=45.0,
                gamma=65.0,
                mud_gas=120.0,
                active_pit_vol=450.0,
            )
        )
    return records


async def main():
    parser = argparse.ArgumentParser(description="Seed synthetic telemetry batches into Telemetry Mesh")
    parser.add_argument("--url", default="http://localhost:8000", help="Mesh API base URL")
    parser.add_argument("--format", choices=["json", "witsml"], default="json", help="Payload format")
    parser.add_argument("--rig", default="RIG-TITAN", help="Target rig identifier")
    parser.add_argument("--count", type=int, default=15, help="Number of records to generate")
    args = parser.parse_args()

    records = generate_records(args.rig, f"{args.rig}-WELL", count=args.count)

    async with httpx.AsyncClient(timeout=10.0) as client:
        if args.format == "json":
            payload = {"records": [r.model_dump(mode="json") for r in records]}
            print(f"Sending JSON batch of {len(records)} records to {args.url}/api/v1/telemetry/batch...")
            resp = await client.post(f"{args.url}/api/v1/telemetry/batch", json=payload)
        else:
            xml_data = WitsmlLog.generate_witsml_xml(args.rig, f"{args.rig}-WELL", records)
            print(f"Sending WITSML XML payload with {len(records)} records to {args.url}/api/v1/telemetry/witsml...")
            resp = await client.post(
                f"{args.url}/api/v1/telemetry/witsml",
                content=xml_data.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
            )

        print(f"Response status: {resp.status_code}")
        print(f"Response body: {resp.text}")


if __name__ == "__main__":
    asyncio.run(main())
