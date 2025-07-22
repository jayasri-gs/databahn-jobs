package model

type DeviceInventoryAlerts struct {
	DeviceClass *DeviceClass
}

// SourceDeviceInventoryAlert represents a consolidated alert for a source with multiple devices
type SourceDeviceInventoryAlert struct {
	SourceID       string        `json:"source_id"`
	SourceName     string        `json:"source_name"`
	TenantId       string        `json:"tenant_id"`
	TenantName     string        `json:"tenant_name"`
	DataPlaneId    string        `json:"data_plane_id"`
	TopDevices     []DeviceClass `json:"top_devices"`
	TotalCount     int           `json:"total_count"`
	RemainingCount int           `json:"remaining_count"`
}

type DeviceClass struct {
	Hostname    string `json:"hostname"`
	MinTime     int64  `json:"min_time"`
	MaxTime     int64  `json:"max_time"`
	SourceID    string `json:"source_id"`
	TenantId    string `json:"tenant_id"`
	Reputation  string `json:"reputation"`
	TenantName  string `json:"tenant_name"`
	SourceName  string `json:"source_name"`
	DataPlaneId string `json:"data_plane_id"`
}

func (dia DeviceInventoryAlerts) GetEntityId() string {
	return dia.DeviceClass.SourceID
}

func (dia DeviceInventoryAlerts) GetEntityName() string {
	return dia.DeviceClass.Hostname
}

func (dia DeviceInventoryAlerts) GetDataPlaneId() string {
	return dia.DeviceClass.DataPlaneId
}
func (dia DeviceInventoryAlerts) GetTenantId() string {
	return dia.DeviceClass.TenantId
}

// GetEntityId returns the source ID for source-level alerts
func (sdia SourceDeviceInventoryAlert) GetEntityId() string {
	return sdia.SourceID
}

// GetEntityName returns the source name for source-level alerts
func (sdia SourceDeviceInventoryAlert) GetEntityName() string {
	return sdia.SourceName
}

// GetDataPlaneId returns the data plane ID for source-level alerts
func (sdia SourceDeviceInventoryAlert) GetDataPlaneId() string {
	return sdia.DataPlaneId
}

// GetTenantId returns the tenant ID for source-level alerts
func (sdia SourceDeviceInventoryAlert) GetTenantId() string {
	return sdia.TenantId
}

func NewDeviceInventoryAlerts(devices *DeviceClass) *DeviceInventoryAlerts {
	return &DeviceInventoryAlerts{
		DeviceClass: devices,
	}
}

// NewSourceDeviceInventoryAlert creates a new source-level alert with top 5 devices
func NewSourceDeviceInventoryAlert(sourceID, sourceName, tenantId, tenantName, dataPlaneId string, devices []DeviceClass) *SourceDeviceInventoryAlert {
	totalCount := len(devices)
	topDevices := devices
	remainingCount := 0

	// Take only top 5 devices if there are more
	if totalCount > 5 {
		topDevices = devices[:5]
		remainingCount = totalCount - 5
	}

	return &SourceDeviceInventoryAlert{
		SourceID:       sourceID,
		SourceName:     sourceName,
		TenantId:       tenantId,
		TenantName:     tenantName,
		DataPlaneId:    dataPlaneId,
		TopDevices:     topDevices,
		TotalCount:     totalCount,
		RemainingCount: remainingCount,
	}
}
