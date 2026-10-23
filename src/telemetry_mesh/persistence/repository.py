"""Repository for TimescaleDB persistence and in-memory caching."""

from collections import deque
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import asyncpg

from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType, LLMDiagnosis
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.db import get_db_pool

logger = logging.getLogger(__name__)


class TelemetryRepository:
    """Handles structured metrics storage in TimescaleDB with in-memory caching."""

    def __init__(self, pool: Optional[asyncpg.Pool] = None, max_cache_per_rig: int = 500):
        self.pool = pool
        self.max_cache_per_rig = max_cache_per_rig
        self._cache: Dict[str, deque[RigTelemetry]] = {}
        self._anomalies_cache: deque[AnomalyEvent] = deque(maxlen=200)

    async def _ensure_pool(self) -> Optional[asyncpg.Pool]:
        if self.pool is None:
            self.pool = await get_db_pool()
        return self.pool

    async def save_telemetry_batch(self, records: List[RigTelemetry]) -> int:
        """Persist a micro-batch of telemetry records."""
        if not records:
            return 0

        # Always update in-memory cache for fast dashboard queries
        for r in records:
            if r.rig_id not in self._cache:
                self._cache[r.rig_id] = deque(maxlen=self.max_cache_per_rig)
            self._cache[r.rig_id].append(r)

        pool = await self._ensure_pool()
        if pool is None:
            return len(records)

        # Batch insert into TimescaleDB
        query = """
            INSERT INTO rig_telemetry (
                time, rig_id, well_id, depth_md, depth_tvd, wob, rpm, torque,
                flow_in, flow_out, spp, rop, gamma, mud_gas, active_pit_vol
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            );
        """
        rows = [
            (
                r.timestamp,
                r.rig_id,
                r.well_id,
                r.depth_md,
                r.depth_tvd,
                r.wob,
                r.rpm,
                r.torque,
                r.flow_in,
                r.flow_out,
                r.spp,
                r.rop,
                r.gamma,
                r.mud_gas,
                r.active_pit_vol,
            )
            for r in records
        ]

        try:
            async with pool.acquire() as conn:
                await conn.executemany(query, rows)
            return len(records)
        except Exception as e:
            logger.error(f"Error persisting telemetry batch to TimescaleDB: {e}")
            return len(records)

    async def save_anomaly_event(self, anomaly: AnomalyEvent) -> bool:
        """Persist an evaluated anomaly and its LLM diagnostic reasoning."""
        self._anomalies_cache.append(anomaly)

        pool = await self._ensure_pool()
        if pool is None:
            return True

        query = """
            INSERT INTO anomaly_events (
                time, anomaly_id, rig_id, well_id, anomaly_type, severity,
                rule_triggered, threshold_details, current_metrics, window_metrics,
                llm_root_cause, llm_recommendation, llm_confidence, llm_urgency,
                llm_preventive_measures, acknowledged
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            );
        """

        llm = anomaly.llm_diagnosis
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    query,
                    anomaly.timestamp,
                    anomaly.anomaly_id,
                    anomaly.rig_id,
                    anomaly.well_id,
                    anomaly.anomaly_type.value,
                    anomaly.severity.value,
                    anomaly.rule_triggered,
                    json.dumps(anomaly.threshold_details),
                    json.dumps(anomaly.current_metrics),
                    json.dumps(anomaly.window_metrics),
                    llm.root_cause_analysis if llm else None,
                    llm.drilling_recommendation if llm else None,
                    llm.confidence_score if llm else None,
                    llm.urgency_level.value if llm else None,
                    json.dumps(llm.preventive_measures) if llm else None,
                    anomaly.acknowledged,
                )
            return True
        except Exception as e:
            logger.error(f"Error persisting anomaly event to TimescaleDB: {e}")
            return False

    async def get_recent_telemetry(
        self,
        rig_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[RigTelemetry]:
        """Fetch recent telemetry records, checking TimescaleDB or in-memory cache."""
        pool = await self._ensure_pool()
        if pool is not None:
            try:
                where_clause = "WHERE rig_id = $1" if rig_id else ""
                query = f"""
                    SELECT time, rig_id, well_id, depth_md, depth_tvd, wob, rpm, torque,
                           flow_in, flow_out, spp, rop, gamma, mud_gas, active_pit_vol
                    FROM rig_telemetry
                    {where_clause}
                    ORDER BY time DESC
                    LIMIT {limit};
                """
                async with pool.acquire() as conn:
                    if rig_id:
                        rows = await conn.fetch(query, rig_id)
                    else:
                        rows = await conn.fetch(query)

                records = [
                    RigTelemetry(
                        timestamp=r["time"],
                        rig_id=r["rig_id"],
                        well_id=r["well_id"],
                        depth_md=r["depth_md"],
                        depth_tvd=r["depth_tvd"],
                        wob=r["wob"],
                        rpm=r["rpm"],
                        torque=r["torque"],
                        flow_in=r["flow_in"],
                        flow_out=r["flow_out"],
                        spp=r["spp"],
                        rop=r["rop"],
                        gamma=r["gamma"],
                        mud_gas=r["mud_gas"],
                        active_pit_vol=r["active_pit_vol"],
                    )
                    for r in rows
                ]
                return list(reversed(records))
            except Exception as e:
                logger.warning(f"TimescaleDB query failed ({e}), falling back to cache.")

        # In-memory cache fallback
        if rig_id:
            items = list(self._cache.get(rig_id, []))
            return items[-limit:]
        else:
            all_items: List[RigTelemetry] = []
            for q in self._cache.values():
                all_items.extend(list(q))
            all_items.sort(key=lambda x: x.timestamp)
            return all_items[-limit:]

    async def get_recent_anomalies(
        self,
        rig_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[AnomalyEvent]:
        """Fetch recent anomalies."""
        anomalies = list(self._anomalies_cache)
        if rig_id:
            anomalies = [a for a in anomalies if a.rig_id == rig_id]
        return anomalies[-limit:]

    def get_tracked_rigs(self) -> List[str]:
        """Return list of rig IDs currently tracked."""
        return list(self._cache.keys())
