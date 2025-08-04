package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/databahn-ai/databahn-jobs/internal/store/fleet"
	"github.com/databahn-ai/databahn-jobs/internal/util"
)

type UnhealthyFleet struct {
	FleetNode       *fleet.Node
	HealthCheckTime time.Duration
	CheckedAt       time.Time
	LastHeartbeatAt time.Time
}

type HealthyFleet struct {
	FleetNode *fleet.Node
}
type UnhealthyFleetConnector struct {
	FleetConnector  *fleet.Connector
	HealthCheckTime time.Duration
	CheckedAt       time.Time
	LastHeartbeatAt time.Time
}
type HealthyFleetConnector struct {
	FleetConnector *fleet.Connector
}
type UnhealthyFleetComponents struct {
	FleetComponent  *fleet.Components
	FleetNode       *fleet.Node
	Fleet           *fleet.Fleet
	HealthCheckTime time.Duration
	CheckedAt       time.Time
	LastHeartbeatAt time.Time
}

func NewUnhealthyFleet(fleetNode *fleet.Node, healthCheckTime time.Duration) *UnhealthyFleet {
	// Parse the heartbeat string to time.Time
	var lastHeartbeatAt time.Time
	if fleetNode.HeartbeatAt != "" {
		if parsed, err := time.Parse(time.RFC3339, fleetNode.HeartbeatAt); err == nil {
			lastHeartbeatAt = parsed
		} else {
			// If parsing fails, use zero time
			lastHeartbeatAt = time.Time{}
		}
	}

	return &UnhealthyFleet{
		FleetNode:       fleetNode,
		HealthCheckTime: healthCheckTime,
		CheckedAt:       time.Now().UTC(),
		LastHeartbeatAt: lastHeartbeatAt,
	}
}

func NewUnhealthyFleetConnector(fleetConnector *fleet.Connector, healthCheckTime time.Duration) *UnhealthyFleetConnector {
	return &UnhealthyFleetConnector{
		FleetConnector:  fleetConnector,
		HealthCheckTime: healthCheckTime,
		CheckedAt:       time.Now().UTC(),
		LastHeartbeatAt: fleetConnector.HeartbeatAt,
	}
}

func NewUnhealthyFleetComponents(fleetComponent *fleet.Components, fleetNode *fleet.Node, fleet *fleet.Fleet, healthCheckTime time.Duration) *UnhealthyFleetComponents {
	return &UnhealthyFleetComponents{
		FleetComponent:  fleetComponent,
		FleetNode:       fleetNode,
		Fleet:           fleet,
		HealthCheckTime: healthCheckTime,
		CheckedAt:       time.Now().UTC(),
		LastHeartbeatAt: fleetComponent.HeartbeatAt,
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
func (uf UnhealthyFleet) GetDataPlaneId() string { return uuid.Nil.String() }

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

func (uf UnhealthyFleet) LastHeartbeatTimeStr() string {
	return util.HumanReadableTimeWithZone(uf.LastHeartbeatAt)
}

func (uf UnhealthyFleet) CheckedTimeStr() string {
	return util.HumanReadableTimeWithZone(uf.CheckedAt)
}

func (ufc UnhealthyFleetComponents) GetEntityId() string {
	return ufc.FleetComponent.Id.String()
}
func (ufc UnhealthyFleetComponents) GetEntityName() string {
	if ufc.Fleet != nil && ufc.FleetNode != nil {
		return fmt.Sprintf("%s in fleet %s (node: %s)", ufc.FleetComponent.ServiceName, ufc.Fleet.Name, ufc.FleetNode.Name)
	}
	if ufc.Fleet != nil {
		return fmt.Sprintf("%s in fleet %s", ufc.FleetComponent.ServiceName, ufc.Fleet.Name)
	}
	if ufc.FleetNode != nil {
		return fmt.Sprintf("%s in fleet (node: %s)", ufc.FleetComponent.ServiceName, ufc.FleetNode.Name)
	}
	// Fallback if neither fleet nor fleet node is available
	return fmt.Sprintf("%s in fleet (unknown)", ufc.FleetComponent.ServiceName)
}
func (ufc UnhealthyFleetComponents) GetTenantId() string {
	return ufc.FleetComponent.TenantId.String()
}
func (ufc UnhealthyFleetComponents) GetDataPlaneId() string { return uuid.Nil.String() }

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

func (ufc UnhealthyFleetComponents) LastHeartbeatTimeStr() string {
	return util.HumanReadableTimeWithZone(ufc.LastHeartbeatAt)
}

func (ufc UnhealthyFleetComponents) CheckedTimeStr() string {
	return util.HumanReadableTimeWithZone(ufc.CheckedAt)
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
func (ufc UnhealthyFleetConnector) GetDataPlaneId() string { return uuid.Nil.String() }

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

func (ufc UnhealthyFleetConnector) LastHeartbeatTimeStr() string {
	return util.HumanReadableTimeWithZone(ufc.LastHeartbeatAt)
}

func (ufc UnhealthyFleetConnector) CheckedTimeStr() string {
	return util.HumanReadableTimeWithZone(ufc.CheckedAt)
}
