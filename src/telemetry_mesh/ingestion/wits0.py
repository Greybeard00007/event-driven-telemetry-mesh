"""WITS Level 0 (Wellsite Information Transfer Specification) ASCII Stream Parser & TCP Listener."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Callable, Dict, List, Optional
from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)

# Standard WITS Level 0 Record 01 Mappings (General Time-Based Record)
WITS0_REC01_CHANNELS = {
    "0108": "depth_md",      # Bit depth / measured depth (feet or meters)
    "0110": "wob",           # Weight on Bit (klbf)
    "0111": "torque",        # Torque (kft-lbf)
    "0112": "rpm",           # Rotary Speed (RPM)
    "0113": "rop",           # Rate of Penetration (ft/hr)
    "0114": "spp",           # Standpipe Pressure (psi)
    "0117": "flow_in",       # Mud Flow In (gpm)
    "0118": "flow_out",      # Mud Flow Out (gpm)
    "0121": "gamma",         # Gamma Ray (API)
    "0124": "mud_gas",       # Mud Gas (ppm)
    "0126": "active_pit_vol",# Active Mud Pit Volume (bbl)
}


class Wits0Parser:
    """Parses WITS Level 0 ASCII data stream packets into RigTelemetry objects."""

    @staticmethod
    def parse_packet(packet_text: str, rig_id: str = "WITS-RIG-1", well_id: str = "WELL-WITS") -> Optional[RigTelemetry]:
        """Parse a single WITS Level 0 packet (delimited by && and !!).

        Example:
        &&
        01088520.5
        011028.4
        011114.9
        0112122
        011345.0
        01142940
        0117620
        0118619
        012165.0
        !!
        """
        lines = [line.strip() for line in packet_text.splitlines() if line.strip()]
        if not lines:
            return None

        data_fields: Dict[str, float] = {}

        for line in lines:
            if line in ("&&", "!!"):
                continue
            if len(line) < 5:
                continue

            code = line[:4]
            value_str = line[4:].strip()

            if code in WITS0_REC01_CHANNELS:
                field_name = WITS0_REC01_CHANNELS[code]
                try:
                    data_fields[field_name] = float(value_str)
                except ValueError:
                    pass

        if not data_fields:
            return None

        return RigTelemetry(
            rig_id=rig_id,
            well_id=well_id,
            timestamp=datetime.now(timezone.utc),
            depth_md=data_fields.get("depth_md", 0.0),
            wob=data_fields.get("wob", 0.0),
            rpm=data_fields.get("rpm", 0.0),
            torque=data_fields.get("torque", 0.0),
            flow_in=data_fields.get("flow_in", 0.0),
            flow_out=data_fields.get("flow_out", 0.0),
            spp=data_fields.get("spp", 0.0),
            rop=data_fields.get("rop", 0.0),
            gamma=data_fields.get("gamma"),
            mud_gas=data_fields.get("mud_gas"),
            active_pit_vol=data_fields.get("active_pit_vol"),
        )

    @staticmethod
    def format_packet(telemetry: RigTelemetry) -> str:
        """Serialize a RigTelemetry instance into standard WITS Level 0 ASCII packet format."""
        lines = [
            "&&",
            f"0108{telemetry.depth_md:.2f}",
            f"0110{telemetry.wob:.2f}",
            f"0111{telemetry.torque:.2f}",
            f"0112{telemetry.rpm:.1f}",
            f"0113{telemetry.rop:.2f}",
            f"0114{telemetry.spp:.1f}",
            f"0117{telemetry.flow_in:.1f}",
            f"0118{telemetry.flow_out:.1f}",
        ]
        if telemetry.gamma is not None:
            lines.append(f"0121{telemetry.gamma:.1f}")
        if telemetry.mud_gas is not None:
            lines.append(f"0124{telemetry.mud_gas:.1f}")
        lines.append("!!")
        return "\r\n".join(lines) + "\r\n"


class Wits0TcpServer:
    """Asynchronous TCP socket server that ingests real-time WITS0 serial/network feeds."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9191, on_record_callback: Optional[Callable] = None):
        self.host = host
        self.port = port
        self.on_record_callback = on_record_callback
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self.total_packets_received: int = 0

    async def start(self) -> None:
        """Start listening for incoming WITS0 TCP client streams."""
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._running = True
        logger.info(f"WITS0 TCP Server listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop TCP server."""
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        logger.info("WITS0 TCP Server stopped.")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        logger.info(f"WITS0 client connected from {peer}")
        buffer = ""

        try:
            while self._running:
                data = await reader.read(1024)
                if not data:
                    break

                text = data.decode("ascii", errors="replace")
                buffer += text

                # Process complete packets bounded by && and !!
                while "&&" in buffer and "!!" in buffer:
                    start_idx = buffer.find("&&")
                    end_idx = buffer.find("!!", start_idx)
                    if end_idx == -1:
                        break

                    packet_str = buffer[start_idx : end_idx + 2]
                    buffer = buffer[end_idx + 2 :]

                    record = Wits0Parser.parse_packet(packet_str)
                    if record and self.on_record_callback:
                        self.total_packets_received += 1
                        cb_res = self.on_record_callback(record)
                        if asyncio.iscoroutine(cb_res):
                            await cb_res
        except Exception as e:
            logger.debug(f"WITS0 client handler exception: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"WITS0 client {peer} disconnected.")
