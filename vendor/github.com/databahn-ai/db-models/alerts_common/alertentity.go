package alerts_common

import "github.com/google/uuid"

type AlertEntityObject struct {
	EntityId         uuid.UUID
	EntityTenantUUId uuid.UUID
	EntityName       string
	DataPlaneId      uuid.UUID
}

type AlertBaseObjectV2 struct {
	EntityId         uuid.UUID
	EntityTenantUUId uuid.UUID
	EntityName       string
	DataPlaneId      uuid.UUID
	AlertType        string
	ErrorCode        string
}
