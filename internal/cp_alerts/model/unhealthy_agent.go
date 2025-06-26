package model

import (
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
)

type UnhealthyAgent struct {
	Agent           *agent.Agent
	HealthCheckTime time.Duration
}

type HealthyAgent struct {
	Agent *agent.Agent
}

func NewUnhealthyAgent(agent *agent.Agent, healthCheckTime time.Duration) *UnhealthyAgent {
	return &UnhealthyAgent{
		Agent:           agent,
		HealthCheckTime: healthCheckTime,
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
