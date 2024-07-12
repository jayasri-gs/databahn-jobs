package tenant

import "github.com/google/uuid"

type Digest struct {
	TenantId             uuid.UUID          `json:"tenant_id"`
	Name                 string             `json:"name"`
	IngestionHealth      string             `json:"ingestion_health"`
	DeliveryHealth       string             `json:"device_health"`
	TotalIngestionEvents float64            `json:"total_ingestion_events"`
	TotalIngestionSize   float64            `json:"total_ingestion_size"`
	AverageEPS           int64              `json:"average_eps"`
	DeliveryBreakdown    map[string]float64 `json:"delivery_breakdown"`
	SensitiveData        map[string]float64 `json:"sensitive_data"`
	ReductionPercentage  map[string]int     `json:"reduction_percentage"`
	Alerts               []string           `json:"alerts"`
}
