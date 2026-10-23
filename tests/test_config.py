"""Tests for application settings and configuration defaults."""

from telemetry_mesh.config import Settings


def test_default_settings():
    """Verify default configuration boundaries and broker settings."""
    cfg = Settings()
    assert cfg.APP_NAME == "event-driven-telemetry-mesh"
    assert cfg.API_PORT == 8000
    assert cfg.REDIS_STREAM_RAW == "telemetry:stream:raw"
    assert cfg.REDIS_STREAM_ANOMALIES == "telemetry:stream:anomalies"
    assert cfg.REDIS_BATCH_SIZE == 50
    assert cfg.THRESHOLD_WOB_MAX == 55.0
    assert cfg.THRESHOLD_RPM_MAX == 220.0
    assert cfg.SIMULATOR_RIGS == 3


def test_custom_settings_override():
    """Verify settings can be overridden via constructor arguments."""
    custom = Settings(
        APP_ENV="test",
        API_PORT=9000,
        THRESHOLD_WOB_MAX=60.0,
        SIMULATOR_RIGS=5,
    )
    assert custom.APP_ENV == "test"
    assert custom.API_PORT == 9000
    assert custom.THRESHOLD_WOB_MAX == 60.0
    assert custom.SIMULATOR_RIGS == 5
