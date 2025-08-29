package model

import (
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/db-models/alerts_async"
	"time"
)

type InactiveDestination struct {
	Destination   *destination.Destination
	LastEventTime time.Time
	AlertDuration time.Duration
	CheckedAt     time.Time
	alerts_async.NoSecondaryEntityId
}

func (id InactiveDestination) GetEntityId() string    { return id.Destination.ID.String() }
func (id InactiveDestination) GetEntityName() string  { return id.Destination.Name }
func (id InactiveDestination) GetTenantId() string    { return id.Destination.TenantID.String() }
func (id InactiveDestination) GetDataPlaneId() string { return id.Destination.DataPlaneId.String() }

func NewInactiveDestination(dest *destination.Destination, lastEventTime time.Time, alertDuration time.Duration, checkedAt time.Time) *InactiveDestination {
	return &InactiveDestination{
		Destination:   dest,
		LastEventTime: lastEventTime,
		AlertDuration: alertDuration,
		CheckedAt:     checkedAt,
	}
}
