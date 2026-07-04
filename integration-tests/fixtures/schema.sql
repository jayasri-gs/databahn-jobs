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
