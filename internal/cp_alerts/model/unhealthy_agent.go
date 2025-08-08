package model

import (
	"fmt"
	"github.com/databahn-ai/db-models/alerts_async"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/util"
)

type UnhealthyAgent struct {
	Agent           *agent.Agent
	HealthCheckTime time.Duration
	CheckedAt       time.Time
	LastHeartbeatAt time.Time
	alerts_async.NoSecondaryEntityId
}

type HealthyAgent struct {
	Agent *agent.Agent
}

func NewUnhealthyAgent(agent *agent.Agent, healthCheckTime time.Duration) *UnhealthyAgent {
	return &UnhealthyAgent{
		Agent:           agent,
		HealthCheckTime: healthCheckTime,
		CheckedAt:       time.Now().UTC(),
		LastHeartbeatAt: agent.HeartbeatAt,
	}
}

func (ua UnhealthyAgent) GetEntityId() string {
	return ua.Agent.ID.String()
}
func (ua UnhealthyAgent) GetEntityName() string {
	return ua.Agent.Name
}
func (ua UnhealthyAgent) GetDataPlaneId() string {
	return ua.Agent.DataPlaneId.String()
}
func (ua UnhealthyAgent) GetTenantId() string {
	return ua.Agent.TenantId.String()
}

func (ua UnhealthyAgent) GetHealthCheckTime() time.Duration {
	return ua.HealthCheckTime
}

func (ua UnhealthyAgent) HealthCheckTimeStr() string {
	minutes := int(ua.HealthCheckTime.Minutes())
	if minutes == 1 {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", minutes)
}

func (ua UnhealthyAgent) LastHeartbeatTimeStr() string {
	return util.HumanReadableTimeWithZone(ua.LastHeartbeatAt)
}

func (ua UnhealthyAgent) CheckedTimeStr() string {
	return util.HumanReadableTimeWithZone(ua.CheckedAt)
}
