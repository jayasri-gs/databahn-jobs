package alerts_common

import "github.com/google/uuid"

type AlertEntityObject struct {
	EntityId         uuid.UUID
	EntityTenantUUId uuid.UUID
	EntityName       string
}
