"""Configuration settings for Event-Driven Telemetry Mesh."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    APP_NAME: str = "event-driven-telemetry-mesh"
    APP_ENV: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    API_PORT: int = 8000
    API_HOST: str = "0.0.0.0"

    # Redis Streams Broker
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_STREAM_RAW: str = "telemetry:stream:raw"
    REDIS_STREAM_ANOMALIES: str = "telemetry:stream:anomalies"
    REDIS_CONSUMER_GROUP: str = "telemetry:workers:group"
    REDIS_CONSUMER_NAME: Optional[str] = None
    REDIS_STREAM_MAX_LEN: int = 100_000
    REDIS_BATCH_SIZE: int = 50
    REDIS_BLOCK_MS: int = 1000
    REDIS_CLAIM_MIN_IDLE_MS: int = 10_000

    # TimescaleDB / PostgreSQL Persistence
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/telemetry_db"
    DB_POOL_MIN_SIZE: int = 5
    DB_POOL_MAX_SIZE: int = 20
    DB_ENABLED: bool = True

    # Ollama AI Inference Worker Pool
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "deepseek-r1:8b"
    OLLAMA_TIMEOUT_SECONDS: float = 15.0
    OLLAMA_ENABLED: bool = True
    OLLAMA_FALLBACK_MOCK: bool = True
    OLLAMA_CONCURRENCY: int = 4

    # Sliding Window & Buffer
    WINDOW_SIZE_SAMPLES: int = 30
    WINDOW_MAX_TIME_SPAN_SECONDS: int = 120

    # Deterministic Rig Safety Thresholds
    THRESHOLD_WOB_MAX: float = 55.0  # klbf (Weight on Bit)
    THRESHOLD_WOB_MIN: float = 5.0   # klbf (while drilling)
    THRESHOLD_RPM_MAX: float = 220.0 # RPM
    THRESHOLD_RPM_MIN: float = 30.0  # RPM (when rotating)
    THRESHOLD_TORQUE_MAX: float = 35.0 # kft-lbf
    THRESHOLD_SPP_MAX: float = 4800.0  # psi (Standpipe pressure)
    THRESHOLD_FLOW_IN_MAX: float = 950.0 # gpm
    THRESHOLD_FLOW_DELTA_PERCENT: float = 15.0 # % discrepancy between flow in and flow out
    THRESHOLD_STICK_SLIP_RPM_RATIO: float = 0.6 # RPM variance coefficient

    # Simulator Configuration
    SIMULATOR_RIGS: int = 3
    SIMULATOR_FREQUENCY_HZ: float = 5.0
    SIMULATOR_AUTO_ANOMALY_INTERVAL_SEC: int = 45


settings = Settings()
