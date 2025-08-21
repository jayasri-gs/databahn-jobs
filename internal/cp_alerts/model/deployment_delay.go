package model

import (
	"time"

	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/google/uuid"
)

// EntityType represents different types of entities that can be in deploying state
type EntityType string

const (
	EntityTypeSource             EntityType = "source"
	EntityTypeDestination        EntityType = "destination"
	EntityTypeInsightRule        EntityType = "insight_rule"
	EntityTypeVCRule             EntityType = "vc_rule"
	EntityTypeEnrichment         EntityType = "enrichment"
	EntityTypeRouteProcessor     EntityType = "route_processor"
	EntityTypeLookup             EntityType = "lookup"
	EntityTypeDataTransformation EntityType = "data_transformation"
)

// DeployingEntity represents an entity that has been in deploying state for too long
type DeployingEntity struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Type        EntityType `json:"type"`
	TenantID    uuid.UUID  `json:"tenant_id"`
	DataPlaneID string     `json:"data_plane_id"`
	UpdatedAt   time.Time  `json:"updated_at"`
	alerts_async.NoSecondaryEntityId
}

// GetEntityId implements AlertEntity interface - returns tenant ID as primary entity ID
func (e DeployingEntity) GetEntityId() string {
	return e.ID.String()
}

// GetEntityName implements AlertEntity interface
func (e DeployingEntity) GetEntityName() string {
	return e.Name
}

// GetDataPlaneId implements AlertEntity interface
func (e DeployingEntity) GetDataPlaneId() string {
	return e.DataPlaneID
}

// GetTenantId implements AlertEntity interface
func (e DeployingEntity) GetTenantId() string {
	return e.TenantID.String()
}

// HealthyEntity represents an entity that is no longer in deploying state
type HealthyEntity struct {
	EntityId   string
	EntityType EntityType
}
