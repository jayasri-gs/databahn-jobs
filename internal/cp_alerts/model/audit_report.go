package model

type AuditReportAlert struct {
	EntityId          string
	EntityName        string
	EntityTenantId    string
	EntityDataPlaneId string
}

func (id AuditReportAlert) GetEntityId() string    { return id.EntityId }
func (id AuditReportAlert) GetEntityName() string  { return id.EntityName }
func (id AuditReportAlert) GetTenantId() string    { return id.EntityTenantId }
func (id AuditReportAlert) GetDataPlaneId() string { return "entity-dataplane-id" }
