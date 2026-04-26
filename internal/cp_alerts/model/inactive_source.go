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
	FleetId       string
	FleetName     string
	AgentId       string
	AgentName     string
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

// GetSecondaryEntityId implements alerts_async.AlertEntity. It is copied onto alert.SecondaryEntityId
// and folded into the stable alert id hash when non-empty (see db-models/alerts_async buildId).
// Per-fleet / per-agent inactivity rows set this so distinct alerts can exist for the same source;
// CLOUD and other scopes, or single-fleet / single-agent rows, leave it empty.
func (ias InActiveSource) GetSecondaryEntityId() string {
	switch ias.Source.Scope {
	case "FLEET":
		return ias.FleetId
	case "AGENT":
		return ias.AgentId
	default:
		return ""
	}
}

func (ias InActiveSource) IsFleetScoped() bool {
	return ias.FleetId != ""
}

func (ias InActiveSource) IsAgentScoped() bool {
	return ias.AgentId != ""
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

func NewInActiveSourceWithFleet(source *source.Source, lastEventTime time.Time, alertDuration time.Duration, fleetId, fleetName string) *InActiveSource {
	return &InActiveSource{
		Source:        source,
		LastEventTime: lastEventTime,
		AlertDuration: alertDuration,
		CheckedAt:     time.Now().UTC(),
		FleetId:       fleetId,
		FleetName:     fleetName,
	}
}

func NewInActiveSourceWithAgent(source *source.Source, lastEventTime time.Time, alertDuration time.Duration, agentId, agentName string) *InActiveSource {
	return &InActiveSource{
		Source:        source,
		LastEventTime: lastEventTime,
		AlertDuration: alertDuration,
		CheckedAt:     time.Now().UTC(),
		AgentId:       agentId,
		AgentName:     agentName,
	}
}
