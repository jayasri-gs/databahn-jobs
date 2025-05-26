package alerts_common

import "github.com/google/uuid"

// Deprecated: This is deprecated and will be removed soon.
type AlertEntityObject struct {
	EntityId         uuid.UUID
	EntityTenantUUId uuid.UUID
	EntityName       string
	DataPlaneId      uuid.UUID
}

// Deprecated: This is deprecated and will be removed soon.
type AlertBaseObjectV2 struct {
	EntityId         uuid.UUID
	EntityTenantUUId uuid.UUID
	EntityName       string
	DataPlaneId      uuid.UUID
	AlertType        string
	ErrorCode        string
}
