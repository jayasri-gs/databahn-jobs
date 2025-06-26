package model

import (
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/fleet"
)

type UnhealthyFleet struct {
	FleetNode       *fleet.Node
	HealthCheckTime time.Duration
}

type HealthyFleet struct {
	FleetNode *fleet.Node
}
type UnhealthyFleetConnector struct {
	FleetConnector  *fleet.Connector
	HealthCheckTime time.Duration
}
type HealthyFleetConnector struct {
	FleetConnector *fleet.Connector
}
type UnhealthyFleetComponents struct {
	FleetComponent  *fleet.Components
	HealthCheckTime time.Duration
}

func NewUnhealthyFleet(fleetNode *fleet.Node, healthCheckTime time.Duration) *UnhealthyFleet {
	return &UnhealthyFleet{
		FleetNode:       fleetNode,
		HealthCheckTime: healthCheckTime,
	}
}

func NewUnhealthyFleetConnector(fleetConnector *fleet.Connector, healthCheckTime time.Duration) *UnhealthyFleetConnector {
	return &UnhealthyFleetConnector{
		FleetConnector:  fleetConnector,
		HealthCheckTime: healthCheckTime,
	}
}

func NewUnhealthyFleetComponents(fleetComponent *fleet.Components, healthCheckTime time.Duration) *UnhealthyFleetComponents {
	return &UnhealthyFleetComponents{
		FleetComponent:  fleetComponent,
		HealthCheckTime: healthCheckTime,
	}
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

func (uf UnhealthyFleet) GetHealthCheckTime() time.Duration {
	return uf.HealthCheckTime
}

func (uf UnhealthyFleet) HealthCheckTimeStr() string {
	minutes := int(uf.HealthCheckTime.Minutes())
	if minutes == 1 {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", minutes)
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

func (ufc UnhealthyFleetComponents) GetHealthCheckTime() time.Duration {
	return ufc.HealthCheckTime
}

func (ufc UnhealthyFleetComponents) HealthCheckTimeStr() string {
	minutes := int(ufc.HealthCheckTime.Minutes())
	if minutes == 1 {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", minutes)
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

func (ufc UnhealthyFleetConnector) GetHealthCheckTime() time.Duration {
	return ufc.HealthCheckTime
}

func (ufc UnhealthyFleetConnector) HealthCheckTimeStr() string {
	minutes := int(ufc.HealthCheckTime.Minutes())
	if minutes == 1 {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", minutes)
}
