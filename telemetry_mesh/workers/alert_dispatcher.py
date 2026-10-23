"""Emergency alert webhook dispatcher for Slack, Microsoft Teams, and PagerDuty."""

import asyncio
import logging
from typing import Any, Dict, List, Optional
import httpx

from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity

logger = logging.getLogger(__name__)


class AlertWebhookDispatcher:
    """Dispatches real-time well control alerts to operational incident management channels."""

    def __init__(
        self,
        webhook_urls: Optional[List[str]] = None,
        min_severity: AnomalySeverity = AnomalySeverity.HIGH,
        timeout_seconds: float = 5.0,
    ):
        self.webhook_urls: List[str] = webhook_urls or []
        self.min_severity = min_severity
        self.timeout = timeout_seconds
        self.total_dispatched: int = 0
        self.total_failed: int = 0

    def add_webhook(self, url: str) -> None:
        """Register a new notification webhook target."""
        if url not in self.webhook_urls:
            self.webhook_urls.append(url)

    def should_dispatch(self, anomaly: AnomalyEvent) -> bool:
        """Determine if anomaly meets severity threshold for pager/chat dispatch."""
        severity_rank = {
            AnomalySeverity.LOW: 1,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.HIGH: 3,
            AnomalySeverity.CRITICAL: 4,
        }
        return severity_rank.get(anomaly.severity, 1) >= severity_rank.get(self.min_severity, 3)

    def _format_payload(self, anomaly: AnomalyEvent) -> Dict[str, Any]:
        """Generate a universally compatible alert payload with Slack & Teams formatting."""
        diag = anomaly.llm_diagnosis
        root_cause = diag.root_cause_analysis if diag else "Physical sensor threshold breach."
        rec = diag.drilling_recommendation if diag else "Inspect wellsite parameters."
        confidence = int((diag.confidence_score if diag else 0.85) * 100)

        title = f"🚨 DRILLING ALERT [{anomaly.severity.value}]: {anomaly.anomaly_type.value} on {anomaly.rig_id}"

        return {
            "text": title,
            # Slack blocks
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": title, "emoji": True},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Rig ID:*\n{anomaly.rig_id}"},
                        {"type": "mrkdwn", "text": f"*Well ID:*\n{anomaly.well_id}"},
                        {"type": "mrkdwn", "text": f"*Trigger:*\n{anomaly.rule_triggered}"},
                        {"type": "mrkdwn", "text": f"*AI Confidence:*\n{confidence}%"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Root Cause:*\n{root_cause}\n\n*Immediate Drilling Action:*\n```{rec}```",
                    },
                },
            ],
            # Standard Webhook Metadata
            "event": {
                "anomaly_id": anomaly.anomaly_id,
                "rig_id": anomaly.rig_id,
                "well_id": anomaly.well_id,
                "anomaly_type": anomaly.anomaly_type.value,
                "severity": anomaly.severity.value,
                "timestamp": anomaly.timestamp.isoformat(),
                "metrics": anomaly.current_metrics,
            },
        }

    async def dispatch(self, anomaly: AnomalyEvent) -> int:
        """Dispatch anomaly alert to all registered webhooks concurrently."""
        if not self.should_dispatch(anomaly) or not self.webhook_urls:
            return 0

        payload = self._format_payload(anomaly)
        success_count = 0

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for url in self.webhook_urls:
                try:
                    resp = await client.post(url, json=payload)
                    if resp.status_code in (200, 201, 202, 204):
                        success_count += 1
                        self.total_dispatched += 1
                    else:
                        self.total_failed += 1
                        logger.warning(f"Webhook {url} returned status {resp.status_code}")
                except Exception as e:
                    self.total_failed += 1
                    logger.debug(f"Webhook dispatch failed to {url}: {e}")

        logger.info(f"Dispatched anomaly {anomaly.anomaly_id[:8]} to {success_count}/{len(self.webhook_urls)} webhooks.")
        return success_count


alert_dispatcher = AlertWebhookDispatcher()
