-- Minimal schema for notification / module integration tests.
-- Column shapes follow backend-service Liquibase (db.changelog-1.0, 1.17, 1.38, 1.47, 1.52).

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    name VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS modules (
    id UUID PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS targets (
    id UUID PRIMARY KEY,
    configuration JSONB,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by UUID NOT NULL,
    description VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    type VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    updated_by UUID
);

CREATE TABLE IF NOT EXISTS module_targets (
    id UUID PRIMARY KEY,
    module_id UUID NOT NULL REFERENCES modules(id),
    target_id UUID NOT NULL REFERENCES targets(id),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS module_tenant_mapping (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    module_id UUID NOT NULL REFERENCES modules(id),
    customer_id UUID,
    enabled BOOLEAN NOT NULL,
    config TEXT,
    module_tenant_config JSONB
);

CREATE TABLE IF NOT EXISTS alert_notification_checkpoint (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    checkpoint_value JSONB NOT NULL,
    alert_type VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS log_source (
    id UUID PRIMARY KEY,
    configuration JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by UUID,
    customer_id UUID,
    description VARCHAR(512) DEFAULT '',
    device VARCHAR(30) DEFAULT '',
    log_type VARCHAR(30) DEFAULT '',
    name VARCHAR(100) NOT NULL,
    replay_source BOOLEAN NOT NULL DEFAULT FALSE,
    reputation VARCHAR(255) DEFAULT '',
    scope VARCHAR(255) DEFAULT '',
    status VARCHAR(255) NOT NULL DEFAULT 'ACTIVE',
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    timestamp_override_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    timezone_normalization_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by UUID,
    vendor VARCHAR(30) DEFAULT '',
    version VARCHAR(36) DEFAULT '',
    data_plane_id UUID,
    advanced_configuration JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS import_request (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(512),
    import_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(512) NOT NULL,
    file_storage VARCHAR(50),
    file_type VARCHAR(20) NOT NULL,
    has_headers BOOLEAN,
    import_config JSONB,
    stats JSONB,
    error_message TEXT,
    retries INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITHOUT TIME ZONE,
    completed_at TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT uq_import_request_tenant_name UNIQUE (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS idx_import_request_tenant_created ON import_request (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_import_request_tenant_status ON import_request (tenant_id, status);

CREATE TABLE IF NOT EXISTS change_flag_acks (
    id UUID PRIMARY KEY,
    action VARCHAR(255),
    entity_id VARCHAR(255),
    entity_type VARCHAR(255),
    entity_version VARCHAR(255),
    error TEXT,
    process_status VARCHAR(255),
    request_id VARCHAR(255),
    status VARCHAR(255),
    tenant_id VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE,
    service_name VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_change_flag_acks_process_status_and_timestamp
    ON change_flag_acks (process_status, timestamp);

CREATE TABLE IF NOT EXISTS change_flag_requests (
    request_id VARCHAR(255) PRIMARY KEY,
    action VARCHAR(255),
    entity_id VARCHAR(255),
    entity_type VARCHAR(255),
    is_processed BOOLEAN,
    tenant_id VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE,
    body TEXT,
    data_plane_id VARCHAR(255),
    entity_name VARCHAR(255)
);
