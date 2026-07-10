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
