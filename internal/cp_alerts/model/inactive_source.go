package model

import (
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"time"
)

// InactiveAgentInfo holds agent info with last event time for grouped alerts
type InactiveAgentInfo struct {
	AgentId       string
	AgentName     string
	LastEventTime time.Time
}

type InActiveSource struct {
	Source                    *source.Source
	LastEventTime             time.Time
	AlertDuration             time.Duration
	CheckedAt                 time.Time
	FleetId                   string
	FleetName                 string
	GroupedInactiveAgents     []InactiveAgentInfo // for grouped agent alerts with last event times
	IsGroupedAgentAlert       bool                // indicates this is a grouped alert
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
// Per-fleet inactivity rows set this so distinct alerts can exist for the same source per fleet.
// AGENT-scoped sources use grouped alerts (one per source), so secondary entity ID is empty.
func (ias InActiveSource) GetSecondaryEntityId() string {
	if ias.Source.Scope == "FLEET" {
		return ias.FleetId
	}
	return ""
}

func (ias InActiveSource) IsFleetScoped() bool {
	return ias.FleetId != ""
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

func NewInActiveSourceWithGroupedAgents(
	source *source.Source,
	lastEventTime time.Time,
	alertDuration time.Duration,
	inactiveAgents []InactiveAgentInfo) *InActiveSource {
	return &InActiveSource{
		Source:                source,
		LastEventTime:         lastEventTime,
		AlertDuration:         alertDuration,
		CheckedAt:             time.Now().UTC(),
		GroupedInactiveAgents: inactiveAgents,
		IsGroupedAgentAlert:   true,
	}
}
