"""FastAPI REST and WebSocket route handlers."""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from telemetry_mesh.api.prometheus import generate_prometheus_metrics
from telemetry_mesh.api.websocket_manager import ws_manager
from telemetry_mesh.config import settings
from telemetry_mesh.ingestion.etp import EtpSession
from telemetry_mesh.ingestion.parser import TelemetryParser
from telemetry_mesh.ingestion.validator import TelemetryValidator
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.audit import audit_manager

logger = logging.getLogger(__name__)

router = APIRouter()


class AnomalyInjectionRequest(BaseModel):
    rig_id: str = Field(..., examples=["RIG-TITAN"])
    anomaly_type: AnomalyType = Field(..., examples=[AnomalyType.STICK_SLIP])
    duration_seconds: float = Field(default=20.0, ge=1.0, le=300.0)


class AnomalyAcknowledgeRequest(BaseModel):
    driller_name: str = Field(..., examples=["John Doe (Senior Toolpusher)"])
    action_taken: str = Field(..., examples=["Spaced out string and shut down mud pumps per API RP 53"])
    comments: Optional[str] = Field(None, examples=["Confirmed zero annular flow. Recorded SIDPP = 450 psi."])


class ReplayDlqRequest(BaseModel):
    dlq_id: str
    target_stream: Optional[str] = None


class IngestionResponse(BaseModel):
    status: str
    accepted: int
    rejected: int
    stream_message_ids: List[str]
    rejected_reasons: List[str] = Field(default_factory=list)


@router.post("/api/v1/telemetry/batch", response_model=IngestionResponse)
async def ingest_telemetry_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
):
    """High-throughput JSON batch ingestion endpoint."""
    producer = getattr(request.app.state, "producer", None)
    if not producer:
        raise HTTPException(status_code=503, detail="Producer pipeline not initialized")

    try:
        raw_records = TelemetryParser.parse_json_payload(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid telemetry payload: {e}")

    valid, invalid = TelemetryValidator.filter_valid(raw_records)
    rejected_reasons = [f"Record for {r.rig_id}: {'; '.join(reasons)}" for r, reasons in invalid]

    # Publish valid records to Redis Streams
    msg_ids = await producer.publish_batch_direct(valid)

    return IngestionResponse(
        status="success",
        accepted=len(valid),
        rejected=len(invalid),
        stream_message_ids=msg_ids,
        rejected_reasons=rejected_reasons,
    )


@router.post("/api/v1/telemetry/witsml", response_model=IngestionResponse)
async def ingest_witsml_xml(
    request: Request,
):
    """WITSML 1.3.1 / 1.4.1.1 XML stream ingestion endpoint."""
    producer = getattr(request.app.state, "producer", None)
    if not producer:
        raise HTTPException(status_code=503, detail="Producer pipeline not initialized")

    body_bytes = await request.body()
    xml_str = body_bytes.decode("utf-8")

    try:
        records = TelemetryParser.parse_witsml_xml(xml_str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"WITSML parsing error: {e}")

    valid, invalid = TelemetryValidator.filter_valid(records)
    rejected_reasons = [f"Record for {r.rig_id}: {'; '.join(reasons)}" for r, reasons in invalid]

    msg_ids = await producer.publish_batch_direct(valid)

    return IngestionResponse(
        status="success",
        accepted=len(valid),
        rejected=len(invalid),
        stream_message_ids=msg_ids,
        rejected_reasons=rejected_reasons,
    )


@router.get("/api/v1/telemetry/recent")
async def get_recent_telemetry(
    request: Request,
    rig_id: Optional[str] = Query(None, description="Optional rig ID filter"),
    limit: int = Query(50, ge=1, le=500),
):
    """Retrieve recently ingested telemetry records."""
    repository = getattr(request.app.state, "repository", None)
    if not repository:
        return []
    records = await repository.get_recent_telemetry(rig_id=rig_id, limit=limit)
    return [r.model_dump(mode="json") for r in records]


@router.get("/api/v1/anomalies")
async def get_recent_anomalies(
    request: Request,
    rig_id: Optional[str] = Query(None, description="Optional rig ID filter"),
    limit: int = Query(20, ge=1, le=100),
):
    """Retrieve recent anomaly events with LLM root-cause diagnosis."""
    repository = getattr(request.app.state, "repository", None)
    if not repository:
        return []
    anomalies = await repository.get_recent_anomalies(rig_id=rig_id, limit=limit)
    return [a.model_dump(mode="json") for a in anomalies]


@router.post("/api/v1/simulator/inject")
async def inject_simulator_anomaly(
    request: Request,
    payload: AnomalyInjectionRequest,
):
    """Inject a deliberate operational anomaly into the simulated rig feed."""
    simulator = getattr(request.app.state, "simulator", None)
    if not simulator:
        raise HTTPException(status_code=400, detail="Simulator is not running")

    success = simulator.inject_anomaly_into_rig(
        rig_id=payload.rig_id,
        anomaly_type=payload.anomaly_type,
        duration_sec=payload.duration_seconds,
    )
    if not success:
        available_rigs = list(simulator.rigs.keys())
        raise HTTPException(
            status_code=404,
            detail=f"Rig '{payload.rig_id}' not found. Available rigs: {available_rigs}",
        )

    return {
        "status": "injected",
        "rig_id": payload.rig_id,
        "anomaly_type": payload.anomaly_type.value,
        "duration_seconds": payload.duration_seconds,
    }


@router.get("/api/v1/health")
async def health_check(request: Request):
    """Comprehensive health check across broker, persistence, workers, and Ollama."""
    from telemetry_mesh.persistence.db import is_db_available

    redis_ok = False
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            redis_ok = False

    worker = getattr(request.app.state, "worker", None)
    worker_stats = {
        "running": worker._running if worker else False,
        "processed_records": worker.processed_records_count if worker else 0,
        "anomalies_detected": worker.anomalies_detected_count if worker else 0,
    }

    producer = getattr(request.app.state, "producer", None)
    producer_stats = {
        "total_published": producer.total_published if producer else 0,
        "queue_size": producer.queue_size if producer else 0,
    }

    return {
        "status": "healthy" if redis_ok else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {
            "redis_broker": "connected" if redis_ok else "disconnected",
            "timescaledb": "connected" if is_db_available() else "in-memory-fallback",
            "ollama_inference": "ready" if settings.OLLAMA_ENABLED else "disabled",
            "worker_node": "active" if worker_stats["running"] else "idle",
        },
        "worker": worker_stats,
        "producer": producer_stats,
        "active_websockets": ws_manager.client_count,
    }


@router.get("/api/v1/metrics")
async def get_metrics(request: Request):
    """Pipeline performance and queue depth metrics."""
    producer = getattr(request.app.state, "producer", None)
    worker = getattr(request.app.state, "worker", None)
    consumer = getattr(request.app.state, "consumer", None)

    stream_stats = {}
    if consumer:
        stream_stats = await consumer.get_stream_stats()

    return {
        "published_total": producer.total_published if producer else 0,
        "batches_total": producer.total_batches if producer else 0,
        "processed_total": worker.processed_records_count if worker else 0,
        "anomalies_total": worker.anomalies_detected_count if worker else 0,
        "reclaimed_total": worker.reclaimed_count if worker else 0,
        "stream_stats": stream_stats,
    }


@router.get("/metrics")
async def get_prometheus_metrics(request: Request):
    """Expose Prometheus formatted metrics for scrapers."""
    producer = getattr(request.app.state, "producer", None)
    worker = getattr(request.app.state, "worker", None)
    consumer = getattr(request.app.state, "consumer", None)
    simulator = getattr(request.app.state, "simulator", None)

    stream_stats = {}
    if consumer:
        try:
            stream_stats = await consumer.get_stream_stats()
        except Exception:
            pass

    metrics_text = generate_prometheus_metrics(
        published_total=producer.total_published if producer else 0,
        processed_total=worker.processed_records_count if worker else 0,
        anomalies_total=worker.anomalies_detected_count if worker else 0,
        duplicates_dropped=worker.duplicates_dropped_count if worker else 0,
        dlq_total=worker.dlq_routed_count if worker else 0,
        stream_length=stream_stats.get("length", 0),
        consumer_pending=stream_stats.get("pending", 0),
        active_rigs=len(simulator.rigs) if simulator else 1,
        active_websockets=ws_manager.client_count,
    )
    return Response(content=metrics_text, media_type="text/plain; version=0.0.4; charset=utf-8")


@router.post("/api/v1/anomalies/{anomaly_id}/acknowledge")
async def acknowledge_anomaly(
    request: Request,
    anomaly_id: str,
    payload: AnomalyAcknowledgeRequest,
):
    """Log an operator compliance sign-off and mitigation action for an anomaly."""
    repository = getattr(request.app.state, "repository", None)
    anomalies = await repository.get_recent_anomalies(limit=100) if repository else []

    target_anomaly = None
    for a in anomalies:
        if a.anomaly_id == anomaly_id:
            target_anomaly = a
            a.acknowledged = True
            break

    rig_id = target_anomaly.rig_id if target_anomaly else "UNKNOWN"
    audit_entry = audit_manager.record_signoff(
        anomaly_id=anomaly_id,
        rig_id=rig_id,
        driller_name=payload.driller_name,
        action_taken=payload.action_taken,
        comments=payload.comments,
    )
    return {"status": "acknowledged", "audit_entry": audit_entry.model_dump(mode="json")}


@router.get("/api/v1/audit")
async def get_audit_trail(
    rig_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
):
    """Query operational compliance audit logs."""
    entries = audit_manager.get_entries(rig_id=rig_id, limit=limit)
    return [e.model_dump(mode="json") for e in entries]


@router.get("/api/v1/dlq")
async def get_dlq_info(request: Request):
    """Inspect Dead-Letter Queue status and recent messages."""
    worker = getattr(request.app.state, "worker", None)
    if not worker or not worker.dlq:
        return {"status": "inactive", "messages": []}

    stats = await worker.dlq.get_dlq_stats()
    messages = await worker.dlq.inspect_dlq(count=15)
    return {"stats": stats, "recent_messages": messages}


@router.post("/api/v1/dlq/replay")
async def replay_dlq(request: Request, payload: ReplayDlqRequest):
    """Replay a dead-letter message back into processing stream."""
    worker = getattr(request.app.state, "worker", None)
    if not worker or not worker.dlq:
        raise HTTPException(status_code=503, detail="DLQ not initialized")

    target = payload.target_stream or settings.REDIS_STREAM_RAW
    success = await worker.dlq.replay_dlq_message(payload.dlq_id, target)
    if not success:
        raise HTTPException(status_code=404, detail="DLQ message not found or payload corrupt")
    return {"status": "replayed", "dlq_id": payload.dlq_id, "target_stream": target}


@router.websocket("/ws/etp")
async def websocket_etp_endpoint(websocket: WebSocket, request: Request):
    """Energistics Transfer Protocol (ETP v1.2) WebSocket streaming adapter."""
    await websocket.accept()
    session = EtpSession(websocket)
    producer = getattr(request.app.state, "producer", None)

    async def on_telemetry(records: List[RigTelemetry]):
        if producer:
            await producer.publish_batch_direct(records)

    try:
        while True:
            raw_msg = await websocket.receive_text()
            await session.handle_message(raw_msg, on_telemetry=on_telemetry)
    except WebSocketDisconnect:
        logger.info(f"ETP WebSocket client disconnected: {session.session_id}")
    except Exception as e:
        logger.debug(f"ETP WebSocket session error: {e}")


@router.websocket("/ws/live")
async def websocket_endpoint(
    websocket: WebSocket,
    rig_id: Optional[str] = Query(None),
):
    """WebSocket feed streaming real-time sensor ticks and Ollama anomaly alerts."""
    await ws_manager.connect(websocket, rig_id=rig_id)
    try:
        while True:
            # Client can send control messages (e.g., ping or filter updates)
            data = await websocket.receive_text()
            # Echo ping keepalives
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as e:
        logger.debug(f"WebSocket client error: {e}")
        await ws_manager.disconnect(websocket)
