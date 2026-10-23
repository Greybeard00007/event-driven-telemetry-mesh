-- TimescaleDB Schema for Event-Driven Telemetry Mesh

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. High-frequency Rig Sensor Telemetry Table
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

-- Convert to TimescaleDB Hypertable partitioned by time (1-day chunk interval)
SELECT create_hypertable('rig_telemetry', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Indexes for fast query retrieval
CREATE INDEX IF NOT EXISTS idx_rig_telemetry_rig_time ON rig_telemetry (rig_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_rig_telemetry_well_time ON rig_telemetry (well_id, time DESC);

-- 2. Anomaly Events & LLM Inference Table
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
CREATE INDEX IF NOT EXISTS idx_anomaly_events_type_time ON anomaly_events (anomaly_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_severity ON anomaly_events (severity, time DESC);

-- 3. Continuous Aggregate: 1-Minute Downsampled Telemetry View
CREATE MATERIALIZED VIEW IF NOT EXISTS rig_telemetry_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    rig_id,
    AVG(wob) AS avg_wob,
    MAX(wob) AS max_wob,
    AVG(rpm) AS avg_rpm,
    AVG(torque) AS avg_torque,
    AVG(flow_in) AS avg_flow_in,
    AVG(flow_out) AS avg_flow_out,
    AVG(spp) AS avg_spp,
    AVG(rop) AS avg_rop,
    MAX(depth_md) AS max_depth_md
FROM rig_telemetry
GROUP BY bucket, rig_id
WITH NO DATA;

-- Refresh policy for continuous aggregate (every 1 minute)
SELECT add_continuous_aggregate_policy('rig_telemetry_1m',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);
