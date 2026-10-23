-- TimescaleDB initialization script
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS rig_telemetry (
    time TIMESTAMPTZ NOT NULL,
    rig_id VARCHAR(64) NOT NULL,
    well_id VARCHAR(64) NOT NULL,
    depth_md DOUBLE PRECISION NOT NULL,
    depth_tvd DOUBLE PRECISION,
    wob DOUBLE PRECISION NOT NULL,
    rpm DOUBLE PRECISION NOT NULL,
    torque DOUBLE PRECISION NOT NULL,
    flow_in DOUBLE PRECISION NOT NULL,
    flow_out DOUBLE PRECISION NOT NULL,
    spp DOUBLE PRECISION NOT NULL,
    rop DOUBLE PRECISION NOT NULL,
    gamma DOUBLE PRECISION,
    mud_gas DOUBLE PRECISION,
    active_pit_vol DOUBLE PRECISION
);

SELECT create_hypertable('rig_telemetry', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_rig_telemetry_rig_time ON rig_telemetry (rig_id, time DESC);

CREATE TABLE IF NOT EXISTS anomaly_events (
    time TIMESTAMPTZ NOT NULL,
    anomaly_id UUID NOT NULL,
    rig_id VARCHAR(64) NOT NULL,
    well_id VARCHAR(64) NOT NULL,
    anomaly_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    rule_triggered VARCHAR(128) NOT NULL,
    threshold_details JSONB,
    current_metrics JSONB,
    window_metrics JSONB,
    llm_root_cause TEXT,
    llm_recommendation TEXT,
    llm_confidence DOUBLE PRECISION,
    llm_urgency VARCHAR(32),
    llm_preventive_measures JSONB,
    acknowledged BOOLEAN DEFAULT FALSE
);

SELECT create_hypertable('anomaly_events', 'time', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_rig_time ON anomaly_events (rig_id, time DESC);
