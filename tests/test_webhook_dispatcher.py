"""Tests for Alert Webhook Dispatcher."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType, LLMDiagnosis
from telemetry_mesh.workers.alert_dispatcher import AlertWebhookDispatcher


def test_alert_dispatcher_severity_filter():
    """Test dispatcher only alerts on HIGH and CRITICAL anomalies."""
    dispatcher = AlertWebhookDispatcher(min_severity=AnomalySeverity.HIGH)

    low_event = AnomalyEvent(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        anomaly_type=AnomalyType.STICK_SLIP,
        severity=AnomalySeverity.LOW,
        rule_triggered="TEST_RULE",
    )
    assert dispatcher.should_dispatch(low_event) is False

    high_event = AnomalyEvent(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        anomaly_type=AnomalyType.PACK_OFF,
        severity=AnomalySeverity.HIGH,
        rule_triggered="TEST_RULE",
    )
    assert dispatcher.should_dispatch(high_event) is True

    crit_event = AnomalyEvent(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        anomaly_type=AnomalyType.KICK_INFLUX,
        severity=AnomalySeverity.CRITICAL,
        rule_triggered="TEST_RULE",
    )
    assert dispatcher.should_dispatch(crit_event) is True


def test_alert_payload_formatting():
    """Test payload contains Slack block kit and diagnostic fields."""
    dispatcher = AlertWebhookDispatcher()

    diag = LLMDiagnosis(
        model_name="deepseek-r1:8b",
        root_cause_analysis="Formation influx detected via annular delta flow.",
        drilling_recommendation="Space out and shut in annular BOP immediately.",
        confidence_score=0.97,
        urgency_level=AnomalySeverity.CRITICAL,
    )

    event = AnomalyEvent(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        anomaly_type=AnomalyType.KICK_INFLUX,
        severity=AnomalySeverity.CRITICAL,
        rule_triggered="INFLUX_RULE",
        llm_diagnosis=diag,
    )

    payload = dispatcher._format_payload(event)
    assert "text" in payload
    assert "blocks" in payload
    assert "event" in payload
    assert payload["event"]["anomaly_type"] == "KICK_INFLUX"
    assert "DRILLING ALERT" in payload["text"]
