package model

import (
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"time"
)

type InActiveSource struct {
	Source        *source.Source
	LastEventTime time.Time
	AlertDuration time.Duration
	CheckedAt     time.Time
}

func (ias InActiveSource) GetEntityId() string {
	return ias.Source.ID.String()
}
func (ias InActiveSource) GetEntityName() string {
	return ias.Source.Name
}
func (ias InActiveSource) GetDataPlaneId() string {
	return ias.Source.DataPlaneId.String()
}
func (ias InActiveSource) GetTenantId() string {
	return ias.Source.TenantID.String()
}

func (ias InActiveSource) InactivityDurationStr() string {
	d := ias.CheckedAt.Sub(ias.LastEventTime)
	return util.HumanReadableDuration(d)
}

func (ias InActiveSource) AlertConfigDurationStr() string {
	return util.HumanReadableDuration(ias.AlertDuration)
}

func NewInActiveSource(source *source.Source, lastEventTime time.Time, alertDuration time.Duration) *InActiveSource {
	return &InActiveSource{
		Source:        source,
		LastEventTime: lastEventTime,
		AlertDuration: alertDuration,
		CheckedAt:     time.Now().UTC(),
	}
}
