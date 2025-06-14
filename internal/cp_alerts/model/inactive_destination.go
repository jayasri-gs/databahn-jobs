package model

import (
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"time"
)

type InactiveDestination struct {
	Destination   *destination.Destination
	LastEventTime time.Time
}

func (id InactiveDestination) GetEntityId() string    { return id.Destination.ID.String() }
func (id InactiveDestination) GetEntityName() string  { return id.Destination.Name }
func (id InactiveDestination) GetTenantId() string    { return id.Destination.TenantID.String() }
func (id InactiveDestination) GetDataPlaneId() string { return id.Destination.DataPlaneId.String() }

func NewInactiveDestination(dest *destination.Destination, lastEventTime time.Time) *InactiveDestination {
	return &InactiveDestination{
		Destination:   dest,
		LastEventTime: lastEventTime,
	}
}
