package model

import "github.com/databahn-ai/databahn-jobs/internal/store/fleet"

type UnhealthyFleet struct {
	FleetNode *fleet.Node
}
type UnhealthyFleetConnector struct {
	FleetConnector *fleet.Connector
}

type UnhealthyFleetComponents struct {
	FleetComponent *fleet.Components
}

func (uf UnhealthyFleet) GetEntityId() string {
	return uf.FleetNode.Id.String()
}
func (uf UnhealthyFleet) GetEntityName() string {
	return uf.FleetNode.Name
}
func (uf UnhealthyFleet) GetTenantId() string {
	return uf.FleetNode.TenantId.String()
}
func (uf UnhealthyFleet) GetDataPlaneId() string {
	return "DataPlaneId not applicable for Fleet Node"
}

func (ufc UnhealthyFleetComponents) GetEntityId() string {
	return ufc.FleetComponent.Id.String()
}
func (ufc UnhealthyFleetComponents) GetEntityName() string { return "Fleet Component Name" }
func (ufc UnhealthyFleetComponents) GetTenantId() string {
	return ufc.FleetComponent.TenantId.String()
}
func (ufc UnhealthyFleetComponents) GetDataPlaneId() string {
	return "DataPlaneId not applicable for Fleet Component"
}

func (ufc UnhealthyFleetConnector) GetEntityId() string {
	return ufc.FleetConnector.ID.String()
}
func (ufc UnhealthyFleetConnector) GetEntityName() string {
	return ufc.FleetConnector.Name
}
func (ufc UnhealthyFleetConnector) GetTenantId() string {
	return ufc.FleetConnector.TenantID.String()
}
func (ufc UnhealthyFleetConnector) GetDataPlaneId() string {
	return "DataPlaneId not applicable for Fleet Connector"
}
