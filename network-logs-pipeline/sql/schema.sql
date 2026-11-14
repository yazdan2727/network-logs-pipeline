
-- =====================================================
-- 1. PIPELINE BATCHES
-- =====================================================

CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id            VARCHAR(64) PRIMARY KEY,
    file_name           TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    batch_status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    total_records       BIGINT DEFAULT 0,
    valid_records       BIGINT DEFAULT 0,
    rejected_records    BIGINT DEFAULT 0,
    duplicate_records   BIGINT DEFAULT 0,

    bytes_median        BIGINT,

    CONSTRAINT chk_batch_status
        CHECK (batch_status IN (
            'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
        ))
);


-- =====================================================
-- 2. RAW NETWORK LOGS
-- =====================================================

CREATE TABLE IF NOT EXISTS raw_network_logs (
    batch_id            VARCHAR(64) NOT NULL,
    source_record_id    BIGINT NOT NULL,

    -- Preserve original CSV values without modification
    timestamp           TEXT,
    src_ip              TEXT,
    dst_ip              TEXT,
    port                TEXT,
    bytes               TEXT,
    protocol            TEXT,
    status              TEXT,
    label               TEXT,
    anomaly_type        TEXT,

    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (batch_id, source_record_id),

    FOREIGN KEY (batch_id)
        REFERENCES pipeline_batches(batch_id)
);


-- =====================================================
-- 3. REJECTED NETWORK LOGS
-- =====================================================

CREATE TABLE IF NOT EXISTS rejected_network_logs (
    batch_id            VARCHAR(64) NOT NULL,
    source_record_id    BIGINT NOT NULL,

    rejection_reason    VARCHAR(50) NOT NULL,
    rejected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    original_record     JSONB NOT NULL,

    PRIMARY KEY (batch_id, source_record_id),

    FOREIGN KEY (batch_id)
        REFERENCES pipeline_batches(batch_id),

    CONSTRAINT chk_rejection_reason
        CHECK (rejection_reason IN (
            'INVALID_TIMESTAMP',
            'TIMESTAMP_OUT_OF_RANGE',
            'MISSING_REQUIRED_FIELD',
            'INVALID_IP',
            'INVALID_PORT',
            'NEGATIVE_BYTES',
            'DUPLICATE_RECORD'
        ))
);


-- =====================================================
-- 4. STAGING NETWORK LOGS
-- =====================================================

CREATE TABLE IF NOT EXISTS stg_network_logs (
    batch_id            VARCHAR(64) NOT NULL,
    source_record_id    BIGINT NOT NULL,

    timestamp           TIMESTAMPTZ NOT NULL,
    src_ip              INET NOT NULL,
    dst_ip              INET NOT NULL,
    port                INTEGER NOT NULL,
    bytes               BIGINT NOT NULL,

    protocol            TEXT,
    status              TEXT NOT NULL DEFAULT 'UNKNOWN',

    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (batch_id, source_record_id),

    FOREIGN KEY (batch_id)
        REFERENCES pipeline_batches(batch_id),

    CONSTRAINT chk_port
        CHECK (port BETWEEN 1 AND 65535),

    CONSTRAINT chk_bytes
        CHECK (bytes >= 0),

    CONSTRAINT uq_stg_composite_key
        UNIQUE (batch_id, timestamp, src_ip, dst_ip, port)
);


-- =====================================================
-- 5. DAILY ANALYTICS MART
-- =====================================================

CREATE TABLE IF NOT EXISTS mart_daily_network_stats (
    event_date          DATE NOT NULL,
    src_ip              INET NOT NULL,

    total_events        BIGINT NOT NULL,
    total_bytes         BIGINT NOT NULL,
    avg_bytes           DOUBLE PRECISION,
    error_events        BIGINT NOT NULL,

    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_date, src_ip),

    CONSTRAINT chk_mart_counts
        CHECK (
            total_events >= 0
            AND total_bytes >= 0
            AND error_events >= 0
        )
);


-- =====================================================
-- 6. INDEXES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_raw_batch
    ON raw_network_logs(batch_id);

CREATE INDEX IF NOT EXISTS idx_stg_timestamp
    ON stg_network_logs(timestamp);

CREATE INDEX IF NOT EXISTS idx_stg_src_ip
    ON stg_network_logs(src_ip);

CREATE INDEX IF NOT EXISTS idx_stg_batch
    ON stg_network_logs(batch_id);

CREATE INDEX IF NOT EXISTS idx_rejected_reason
    ON rejected_network_logs(rejection_reason);

CREATE INDEX IF NOT EXISTS idx_mart_src_ip
    ON mart_daily_network_stats(src_ip);
