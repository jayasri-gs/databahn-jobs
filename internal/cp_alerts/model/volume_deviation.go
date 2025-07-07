package model

import (
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
)

// VolumeDeviationAlert represents a volume deviation alert entity
type VolumeDeviationAlert struct {
	Tenant             *tenant.Tenant
	TodayIngestion     float64
	TodayDelivery      float64
	YesterdayIngestion float64
	YesterdayDelivery  float64
	IngestionDeviation float64
	DeliveryDeviation  float64
	IsBelow50Percent   bool
	IsAboveStdDev      bool
	AlertType          VolumeDeviationAlertType
	DetectionTime      time.Time
}

type VolumeDeviationAlertType string

const (
	VolumeDeviationBelow50Percent VolumeDeviationAlertType = "below_50_percent"
	VolumeDeviationAboveStdDev    VolumeDeviationAlertType = "above_std_dev"
)

func (vda VolumeDeviationAlert) GetEntityId() string {
	return vda.Tenant.Id.String()
}

func (vda VolumeDeviationAlert) GetEntityName() string {
	return vda.Tenant.Name
}

func (vda VolumeDeviationAlert) GetDataPlaneId() string {
	// For volume deviation alerts, we use the tenant ID as the data plane ID
	// since this is a tenant-level alert
	return vda.Tenant.Id.String()
}

func (vda VolumeDeviationAlert) GetTenantId() string {
	return vda.Tenant.Id.String()
}

func NewVolumeDeviationAlert(
	tenant *tenant.Tenant,
	todayIngestion, todayDelivery, yesterdayIngestion, yesterdayDelivery float64,
	ingestionDeviation, deliveryDeviation float64,
	isBelow50Percent, isAboveStdDev bool,
	alertType VolumeDeviationAlertType,
) *VolumeDeviationAlert {
	return &VolumeDeviationAlert{
		Tenant:             tenant,
		TodayIngestion:     todayIngestion,
		TodayDelivery:      todayDelivery,
		YesterdayIngestion: yesterdayIngestion,
		YesterdayDelivery:  yesterdayDelivery,
		IngestionDeviation: ingestionDeviation,
		DeliveryDeviation:  deliveryDeviation,
		IsBelow50Percent:   isBelow50Percent,
		IsAboveStdDev:      isAboveStdDev,
		AlertType:          alertType,
		DetectionTime:      time.Now().UTC(),
	}
}
