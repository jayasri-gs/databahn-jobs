package model

import (
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/google/uuid"
)

// VCNoReductionAlert represents an alert when volume controller doesn't perform any reduction
type VCNoReductionAlert struct {
	Tenant            *tenant.Tenant
	Pipeline          *pipeline.PipelineWithMappings
	SourceID          uuid.UUID
	DestinationID     uuid.UUID
	SourceName        string
	DestinationName   string
	SourceDataPlaneID uuid.UUID
	TotalIngested     float64
	TotalDelivered    float64
	ReductionPercent  float64
	CheckStartTime    time.Time
	CheckEndTime      time.Time
	DetectionTime     time.Time
}

func (vra VCNoReductionAlert) GetEntityId() string {
	return vra.SourceID.String()
}

func (vra VCNoReductionAlert) GetEntityName() string {
	return fmt.Sprintf("%s to %s", vra.SourceName, vra.DestinationName)
}

func (vra VCNoReductionAlert) GetDataPlaneId() string {
	return vra.SourceDataPlaneID.String()
}

func (vra VCNoReductionAlert) GetTenantId() string {
	return vra.Tenant.Id.String()
}

func NewVCNoReductionAlert(
	tenant *tenant.Tenant,
	pipelineWithMappings *pipeline.PipelineWithMappings,
	sourceDataPlaneID uuid.UUID,
	totalIngested, totalDelivered, reductionPercent float64,
	checkStartTime, checkEndTime time.Time,
) *VCNoReductionAlert {
	return &VCNoReductionAlert{
		Tenant:            tenant,
		Pipeline:          pipelineWithMappings,
		SourceID:          pipelineWithMappings.LogSourceID,
		DestinationID:     pipelineWithMappings.DestinationID,
		SourceName:        pipelineWithMappings.SourceName,
		DestinationName:   pipelineWithMappings.DestinationName,
		SourceDataPlaneID: sourceDataPlaneID,
		TotalIngested:     totalIngested,
		TotalDelivered:    totalDelivered,
		ReductionPercent:  reductionPercent,
		CheckStartTime:    checkStartTime,
		CheckEndTime:      checkEndTime,
		DetectionTime:     time.Now().UTC(),
	}
}
