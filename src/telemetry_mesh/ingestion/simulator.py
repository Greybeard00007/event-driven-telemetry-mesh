"""Multi-rig WITSML telemetry simulator with realistic drilling physics and anomaly injection."""

import asyncio
from datetime import datetime, timezone
import logging
import math
import random
import time
from typing import Dict, List, Optional
from telemetry_mesh.config import settings
from telemetry_mesh.models.anomaly import AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)


class RigState:
    """Maintains state and physical variables for an individual simulated drilling rig."""

    def __init__(self, rig_id: str, well_id: str, start_depth: float = 8500.0):
        self.rig_id = rig_id
        self.well_id = well_id
        self.depth_md = start_depth
        self.depth_tvd = start_depth * 0.92

        # Baseline physical setpoints
        self.base_wob = 28.0        # klbf
        self.base_rpm = 120.0       # rpm
        self.base_torque = 14.5     # kft-lbf
        self.base_flow_in = 620.0   # gpm
        self.base_spp = 2950.0      # psi
        self.base_rop = 45.0        # ft/hr
        self.base_gamma = 65.0      # gAPI
        self.base_gas = 120.0       # ppm
        self.base_pit_vol = 450.0   # bbl

        # Active anomaly injection state
        self.active_anomaly: Optional[AnomalyType] = None
        self.anomaly_end_time: float = 0.0
        self.tick_count: int = 0

    def inject_anomaly(self, anomaly_type: AnomalyType, duration_seconds: float = 20.0) -> None:
        """Trigger an intentional anomaly state for a specified duration."""
        self.active_anomaly = anomaly_type
        self.anomaly_end_time = time.time() + duration_seconds
        logger.info(f"[{self.rig_id}] Injected anomaly '{anomaly_type.value}' for {duration_seconds}s")

    def clear_anomaly(self) -> None:
        """Clear any active anomaly."""
        self.active_anomaly = None
        self.anomaly_end_time = 0.0

    def generate_next(self, delta_seconds: float) -> RigTelemetry:
        """Generate the next realistic telemetry reading."""
        self.tick_count += 1
        now = time.time()

        # Check if anomaly expired
        if self.active_anomaly and now > self.anomaly_end_time:
            logger.info(f"[{self.rig_id}] Anomaly '{self.active_anomaly.value}' finished; returning to baseline.")
            self.active_anomaly = None

        # Natural sensor noise and gentle oscillation
        noise_wob = random.gauss(0, 0.8)
        noise_rpm = random.gauss(0, 1.5)
        noise_torque = random.gauss(0, 0.4)
        noise_flow = random.gauss(0, 2.5)
        noise_spp = random.gauss(0, 15.0)

        # Baseline calculations
        wob = max(2.0, self.base_wob + noise_wob)
        rpm = max(10.0, self.base_rpm + noise_rpm)
        torque = max(1.0, self.base_torque + noise_torque)
        flow_in = max(100.0, self.base_flow_in + noise_flow)
        flow_out = max(100.0, flow_in + random.gauss(0, 3.0))  # Normally balanced
        spp = max(500.0, self.base_spp + noise_spp)
        rop = max(5.0, self.base_rop + random.gauss(0, 2.0))
        gamma = max(10.0, self.base_gamma + 10.0 * math.sin(self.tick_count * 0.05) + random.gauss(0, 1.5))
        mud_gas = max(10.0, self.base_gas + random.gauss(0, 5.0))
        pit_vol = self.base_pit_vol + random.gauss(0, 0.5)

        # Apply anomaly effects if active
        if self.active_anomaly == AnomalyType.STICK_SLIP:
            # Torsional stick-slip oscillation: bit stops (RPM->0, Torque surges), then slips (RPM->220, Torque drops)
            phase = math.sin(self.tick_count * 0.8)
            if phase < 0:
                # Stick phase
                rpm = max(0.0, 15.0 + random.uniform(0, 10))
                torque = min(48.0, 32.0 + random.uniform(4, 12))
            else:
                # Slip phase
                rpm = min(250.0, 210.0 + random.uniform(10, 30))
                torque = max(2.0, 6.0 + random.uniform(0, 3))
            wob = wob + random.uniform(2, 6)

        elif self.active_anomaly == AnomalyType.KICK_INFLUX:
            # Formation fluid enters borehole: Flow Out surges > Flow In, SPP drops, pit volume increases, gas spikes
            flow_out = flow_in * (1.28 + random.uniform(0.02, 0.08))
            spp = max(800.0, self.base_spp - 380.0 + noise_spp)
            mud_gas = mud_gas * 6.5
            pit_vol += 12.0

        elif self.active_anomaly == AnomalyType.PACK_OFF:
            # Annulus blocked: SPP spikes, Flow Out drops, torque spikes
            spp = self.base_spp + 1100.0 + random.uniform(50, 200)
            flow_out = flow_in * (0.72 - random.uniform(0.02, 0.06))
            torque = self.base_torque + 9.0

        elif self.active_anomaly == AnomalyType.LOST_CIRCULATION:
            # Mud lost into fractured formation: Flow Out collapses, pit volume drops
            flow_out = flow_in * (0.45 + random.uniform(-0.05, 0.05))
            pit_vol -= 25.0

        elif self.active_anomaly == AnomalyType.EXCESSIVE_WOB:
            # Driller applied dangerous weight on bit
            wob = 64.0 + random.uniform(2, 8)
            torque = self.base_torque + 11.0
            rop = max(2.0, self.base_rop * 0.3)

        elif self.active_anomaly == AnomalyType.MOTOR_STALL:
            # Mud motor stalled out
            rpm = random.uniform(0, 5)
            torque = 42.0 + random.uniform(1, 4)
            spp = self.base_spp + 700.0

        elif self.active_anomaly == AnomalyType.BIT_BOUNCE:
            # High frequency axial vibration
            wob = max(1.0, self.base_wob + 24.0 * math.sin(self.tick_count * 2.5))
            rop = max(1.0, self.base_rop * 0.4)

        # Depth increments based on ROP (ft/hr -> ft/sec)
        depth_inc = (rop / 3600.0) * delta_seconds
        self.depth_md += depth_inc
        self.depth_tvd += depth_inc * 0.92

        return RigTelemetry(
            rig_id=self.rig_id,
            well_id=self.well_id,
            timestamp=datetime.now(timezone.utc),
            depth_md=round(self.depth_md, 2),
            depth_tvd=round(self.depth_tvd, 2),
            wob=round(wob, 2),
            rpm=round(rpm, 1),
            torque=round(torque, 2),
            flow_in=round(flow_in, 1),
            flow_out=round(flow_out, 1),
            spp=round(spp, 1),
            rop=round(rop, 2),
            gamma=round(gamma, 1),
            mud_gas=round(mud_gas, 1),
            active_pit_vol=round(pit_vol, 1),
        )


class TelemetrySimulator:
    """Manages multi-rig continuous telemetry generation."""

    def __init__(
        self,
        num_rigs: int = 3,
        frequency_hz: float = 5.0,
        auto_anomalies: bool = True,
        anomaly_interval_sec: int = 45,
    ):
        self.frequency_hz = frequency_hz
        self.interval = 1.0 / frequency_hz if frequency_hz > 0 else 0.2
        self.auto_anomalies = auto_anomalies
        self.anomaly_interval_sec = anomaly_interval_sec
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self.rigs: Dict[str, RigState] = {}
        rig_names = ["RIG-TITAN", "RIG-ORION", "RIG-APEX", "RIG-VALIANT", "RIG-HORIZON"]
        for i in range(min(num_rigs, len(rig_names))):
            rig_name = rig_names[i]
            well_name = f"WELL-00{i+1}"
            self.rigs[rig_name] = RigState(rig_id=rig_name, well_id=well_name, start_depth=8000.0 + i * 1500.0)

    def get_rig(self, rig_id: str) -> Optional[RigState]:
        return self.rigs.get(rig_id)

    def inject_anomaly_into_rig(self, rig_id: str, anomaly_type: AnomalyType, duration_sec: float = 20.0) -> bool:
        """Inject anomaly into a specific rig."""
        rig = self.get_rig(rig_id)
        if rig:
            rig.inject_anomaly(anomaly_type, duration_sec)
            return True
        return False

    def generate_single_batch(self) -> List[RigTelemetry]:
        """Generate one reading from every active rig."""
        return [rig.generate_next(self.interval) for rig in self.rigs.values()]

    async def run_loop(self, on_batch_callback) -> None:
        """Continuous simulation loop invoking a callback on each batch."""
        self._running = True
        logger.info(f"TelemetrySimulator started across {len(self.rigs)} rigs at {self.frequency_hz} Hz.")
        last_auto_anomaly_time = time.time()

        while self._running:
            try:
                start_t = time.time()

                # Trigger periodic auto anomaly if enabled
                if self.auto_anomalies and (time.time() - last_auto_anomaly_time > self.anomaly_interval_sec):
                    chosen_rig = random.choice(list(self.rigs.values()))
                    chosen_anomaly = random.choice([
                        AnomalyType.STICK_SLIP,
                        AnomalyType.KICK_INFLUX,
                        AnomalyType.PACK_OFF,
                        AnomalyType.EXCESSIVE_WOB,
                        AnomalyType.LOST_CIRCULATION,
                    ])
                    chosen_rig.inject_anomaly(chosen_anomaly, duration_seconds=18.0)
                    last_auto_anomaly_time = time.time()

                batch = self.generate_single_batch()
                await on_batch_callback(batch)

                elapsed = time.time() - start_t
                sleep_time = max(0.001, self.interval - elapsed)
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in simulator loop: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            self._task = None
