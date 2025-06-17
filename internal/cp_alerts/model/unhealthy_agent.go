package model

import "github.com/databahn-ai/databahn-jobs/internal/store/agent"

type UnhealthyAgent struct {
	Agent *agent.Agent
}

type HealthyAgent struct {
	Agent *agent.Agent
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
