"""Asynchronous Ollama LLM worker pool for drilling anomaly root-cause inference."""

import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Optional
import httpx

from telemetry_mesh.config import settings
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType, LLMDiagnosis
from telemetry_mesh.workers.rag_knowledge import DrillingKnowledgeBase

logger = logging.getLogger(__name__)


class OllamaWorkerPool:
    """Manages asynchronous inference requests to a local Ollama LLM instance with RAG and model cascading."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        model_name: Optional[str] = None,
        triage_model_name: Optional[str] = "llama3.2:1b",
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
        fallback_mock: Optional[bool] = None,
    ):
        self.base_url = (base_url or settings.OLLAMA_BASE_URL).rstrip("/")
        self.model_name = model_name or settings.OLLAMA_MODEL
        self.triage_model_name = triage_model_name or "llama3.2:1b"
        self.timeout = timeout_seconds or settings.OLLAMA_TIMEOUT_SECONDS
        self.fallback_mock = fallback_mock if fallback_mock is not None else settings.OLLAMA_FALLBACK_MOCK
        self.semaphore = asyncio.Semaphore(concurrency or settings.OLLAMA_CONCURRENCY)
        self.client: Optional[httpx.AsyncClient] = None
        self.knowledge_base = DrillingKnowledgeBase()

    async def get_client(self) -> httpx.AsyncClient:
        """Lazy initialization of httpx client."""
        if self.client is None or self.client.is_closed:
            self.client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=3.0),
            )
        return self.client

    async def close(self) -> None:
        """Close HTTP client."""
        if self.client and not self.client.is_closed:
            await self.client.aclose()
            self.client = None

    def _build_prompt(self, anomaly: AnomalyEvent) -> str:
        """Construct a structured engineering prompt with window sensor context and RAG SOP knowledge."""
        summary = {
            "anomaly_type": anomaly.anomaly_type.value,
            "rule_triggered": anomaly.rule_triggered,
            "threshold_details": anomaly.threshold_details,
            "current_metrics": anomaly.current_metrics,
            "recent_sensor_window": anomaly.window_metrics[-6:] if anomaly.window_metrics else [],
        }

        # RAG retrieval of relevant well control SOPs
        query = f"{anomaly.anomaly_type.value} {anomaly.rule_triggered}"
        relevant_sops = self.knowledge_base.retrieve(query, top_k=1)
        sop_context = "\n---\n".join([f"[{s['title']}]:\n{s['content']}" for s in relevant_sops])

        return (
            "You are a Senior Petroleum Wellsite Drilling Optimization Engineer. "
            "Analyze the following real-time rig sensor anomaly window and provide an immediate engineering diagnosis.\n\n"
            f"Rig: {anomaly.rig_id} | Well: {anomaly.well_id} | Time: {anomaly.timestamp.isoformat()}\n"
            f"Sensor Context:\n{json.dumps(summary, indent=2)}\n\n"
            f"Company Standard Operating Procedures (SOPs):\n{sop_context}\n\n"
            "Respond ONLY with a valid JSON object matching this schema:\n"
            "{\n"
            '  "root_cause_analysis": "Concise physical root cause explanation",\n'
            '  "drilling_recommendation": "Step-by-step immediate rig crew instructions aligned with SOPs",\n'
            '  "confidence_score": 0.95,\n'
            '  "urgency_level": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW",\n'
            '  "preventive_measures": ["measure 1", "measure 2"]\n'
            "}"
        )

    def _generate_domain_fallback(self, anomaly: AnomalyEvent) -> LLMDiagnosis:
        """Domain-heuristic fallback when Ollama is unreachable or times out."""
        atype = anomaly.anomaly_type

        if atype == AnomalyType.KICK_INFLUX:
            root_cause = (
                "Formation pore pressure exceeded effective hydrostatic pressure, initiating an influx of "
                "gas or reservoir fluids into the wellbore annular space. Observed delta flow out > flow in."
            )
            recommendation = (
                "1. Immediately space out drill string to place tool joint above rotary table.\n"
                "2. Stop rotary and shut down mud pumps.\n"
                "3. Close Annular Preventer (Hard Shut-in procedure).\n"
                "4. Monitor and record SIDPP (Shut-In Drill Pipe Pressure) and SICP (Shut-In Casing Pressure).\n"
                "5. Notify Well Control Superintendent and prepare Driller's Method kill sheet."
            )
            urgency = AnomalySeverity.CRITICAL
            confidence = 0.96
            preventive = [
                "Increase active mud weight (MW) before resuming operations",
                "Perform flow check after drilling breaks",
                "Calibrate pit volume totalizer (PVT) sensors",
            ]

        elif atype == AnomalyType.STICK_SLIP:
            root_cause = (
                "Severe torsional resonance: drill bit stalls on formation face (stick phase) building torsional strain, "
                "followed by rapid release (slip phase) with RPM surging up to 2x setpoint and violent torque cycling."
            )
            recommendation = (
                "1. Reduce Weight on Bit (WOB) by 25-40% immediately.\n"
                "2. Increase surface RPM by 15-25 RPM to exit harmonic resonance zone.\n"
                "3. Engage SoftTorque / automated stick-slip mitigation rotary controller if available.\n"
                "4. Pick up off bottom to verify torque baseline."
            )
            urgency = AnomalySeverity.HIGH
            confidence = 0.92
            preventive = [
                "Optimize drill string BHA stabilization layout",
                "Add drilling torque-reduction lubricating additives to mud",
                "Maintain consistent depth of cut (DOC)",
            ]

        elif atype == AnomalyType.PACK_OFF:
            root_cause = (
                "Cuttings bed accumulation or borehole wall instability choking the annular space, "
                "resulting in sharp standpipe pressure (SPP) escalation and reduced surface return flow."
            )
            recommendation = (
                "1. Immediately stop downhole drill string advancement.\n"
                "2. Reduce mud pump rate to prevent exceeding formation fracture gradient.\n"
                "3. Work drill pipe reciprocation (ream up and down) while monitoring torque.\n"
                "4. Pump high-viscosity weighted pill to sweep and clear cuttings pack."
            )
            urgency = AnomalySeverity.HIGH
            confidence = 0.89
            preventive = [
                "Increase annular velocity / flow rate if hole geometry permits",
                "Improve mud rheology and yield point (YP) for enhanced cuttings transport",
            ]

        elif atype == AnomalyType.MOTOR_STALL:
            root_cause = (
                "Positive Displacement Motor (PDM) differential pressure limit exceeded, stalling downhole rotor."
            )
            recommendation = (
                "1. Pick off bottom immediately until torque drops to off-bottom baseline.\n"
                "2. Verify standpipe pressure returns to normal circulating pressure.\n"
                "3. Tag bottom gently with reduced WOB."
            )
            urgency = AnomalySeverity.HIGH
            confidence = 0.94
            preventive = ["Monitor differential pressure gauge across motor", "Avoid sudden weight transfers"]

        elif atype == AnomalyType.LOST_CIRCULATION:
            root_cause = (
                "Downhole mud pressure exceeded formation fracture gradient or penetrated naturally fractured / cavernous zone."
            )
            recommendation = (
                "1. Stop drilling and pull bit off bottom.\n"
                "2. Reduce pump rate and observe fluid level in annulus.\n"
                "3. Prepare Lost Circulation Material (LCM) pill (fiber/nut plug blend)."
            )
            urgency = AnomalySeverity.HIGH
            confidence = 0.91
            preventive = ["Lower Equivalent Circulating Density (ECD)", "Monitor surge and swab pressures"]

        else:
            root_cause = f"Sensor threshold breach detected for rule {anomaly.rule_triggered}."
            recommendation = "Verify sensor calibration, inspect telemetry channel continuity, and verify rig parameters."
            urgency = anomaly.severity
            confidence = 0.80
            preventive = ["Routine MWD sensor verification and surface instrumentation check"]

        return LLMDiagnosis(
            model_name="petroleum-heuristic-fallback",
            root_cause_analysis=root_cause,
            drilling_recommendation=recommendation,
            confidence_score=confidence,
            urgency_level=urgency,
            preventive_measures=preventive,
            inferred_at=datetime.now(timezone.utc),
            raw_response="[Fallback Domain Heuristic Triggered]",
        )

    async def diagnose_anomaly(self, anomaly: AnomalyEvent) -> LLMDiagnosis:
        """Execute LLM inference via Ollama or fallback to domain heuristic with model cascading."""
        if not settings.OLLAMA_ENABLED:
            return self._generate_domain_fallback(anomaly)

        # Model cascading: Critical anomalies use full deep reasoning model; low/medium use fast triage model
        selected_model = self.model_name if anomaly.severity in (AnomalySeverity.CRITICAL, AnomalySeverity.HIGH) else self.triage_model_name

        async with self.semaphore:
            prompt = self._build_prompt(anomaly)
            client = await self.get_client()

            try:
                logger.info(f"Dispatching anomaly '{anomaly.anomaly_type.value}' ({anomaly.anomaly_id[:8]}) to Ollama ({selected_model}).")
                resp = await client.post(
                    "/api/generate",
                    json={
                        "model": selected_model,
                        "prompt": prompt,
                        "stream": False,
                        "format": "json",
                        "options": {
                            "temperature": 0.2,
                            "top_p": 0.9,
                        },
                    },
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    raw_text = payload.get("response", "")
                    parsed = json.loads(raw_text)

                    # Extract urgency
                    raw_urgency = parsed.get("urgency_level", "MEDIUM").upper()
                    try:
                        urgency = AnomalySeverity(raw_urgency)
                    except ValueError:
                        urgency = AnomalySeverity.MEDIUM

                    return LLMDiagnosis(
                        model_name=self.model_name,
                        root_cause_analysis=parsed.get("root_cause_analysis", "Drilling parameter anomaly identified."),
                        drilling_recommendation=parsed.get("drilling_recommendation", "Review operational guidelines."),
                        confidence_score=float(parsed.get("confidence_score", 0.85)),
                        urgency_level=urgency,
                        preventive_measures=parsed.get("preventive_measures", []),
                        inferred_at=datetime.now(timezone.utc),
                        raw_response=raw_text,
                    )
                else:
                    logger.warning(f"Ollama returned HTTP status {resp.status_code}: {resp.text}")

            except httpx.ConnectError:
                logger.debug(f"Ollama connection refused at {self.base_url}. Using domain heuristic fallback.")
            except httpx.TimeoutException:
                logger.warning(f"Ollama inference timed out after {self.timeout}s. Using domain heuristic fallback.")
            except Exception as e:
                logger.warning(f"Ollama inference error ({e}). Using domain heuristic fallback.")

            if self.fallback_mock:
                return self._generate_domain_fallback(anomaly)
            else:
                raise RuntimeError("Ollama inference failed and fallback is disabled.")
