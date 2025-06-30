package model

type DeviceInventoryAlerts struct {
	DeviceClass *DeviceClass
}

type DeviceClass struct {
	Hostname   string `json:"hostname"`
	MinTime    int64  `json:"min_time"`
	MaxTime    int64  `json:"max_time"`
	SourceID   string `json:"source_id"`
	TenantId   string `json:"tenant_id"`
	Reputation string `json:"reputation"`
	TenantName string `json:"tenant_name"`
	SourceName string `json:"source_name"`
}

func (dia DeviceInventoryAlerts) GetEntityId() string {
	return dia.DeviceClass.SourceID
}

func (dia DeviceInventoryAlerts) GetEntityName() string {
	return dia.DeviceClass.Hostname
}

func (dia DeviceInventoryAlerts) GetDataPlaneId() string {
	return "DataplaneId"
}
func (dia DeviceInventoryAlerts) GetTenantId() string {
	return dia.DeviceClass.TenantId
}

func NewDeviceInventoryAlerts(devices *DeviceClass) *DeviceInventoryAlerts {
	return &DeviceInventoryAlerts{
		DeviceClass: devices,
	}
}
