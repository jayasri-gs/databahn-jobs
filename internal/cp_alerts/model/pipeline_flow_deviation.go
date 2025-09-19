package model

import (
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/google/uuid"
)

// PipelineStage represents a single stage in the pipeline flow
type PipelineStage struct {
	StageID       string  `json:"stage_id"`       // current stage identifier
	ComponentName string  `json:"component_name"` // component name used in statistics
	InputEvents   float64 `json:"input_events"`   // events coming into this stage
	OutputEvents  float64 `json:"output_events"`  // events going out of this stage
}

// PipelineFlowDeviationAlert represents an alert when there's significant deviation in pipeline event flow
type PipelineFlowDeviationAlert struct {
	Tenant            *tenant.Tenant
	Pipeline          *pipeline.PipelineWithMappings
	SourceID          uuid.UUID
	DestinationID     uuid.UUID
	SourceName        string
	DestinationName   string
	SourceDataPlaneID uuid.UUID
	DeviationStage    PipelineStage
	CheckStartTime    time.Time
	CheckEndTime      time.Time
	DetectionTime     time.Time
}

func (pfa PipelineFlowDeviationAlert) GetEntityId() string {
	return pfa.SourceID.String()
}

func (pfa PipelineFlowDeviationAlert) GetEntityName() string {
	return fmt.Sprintf("%s pipeline stage %s", pfa.Pipeline.Pipeline.Name, pfa.DeviationStage.StageID)
}

func (pfa PipelineFlowDeviationAlert) GetDataPlaneId() string {
	return pfa.SourceDataPlaneID.String()
}

func (pfa PipelineFlowDeviationAlert) GetTenantId() string {
	return pfa.Tenant.Id.String()
}

// GetSecondaryEntityId satisfies the alerts_async.AlertEntity interface
func (pfa PipelineFlowDeviationAlert) GetSecondaryEntityId() string {
	return pfa.DeviationStage.StageID
}

func NewPipelineFlowDeviationAlert(
	tenant *tenant.Tenant,
	pipelineWithMappings *pipeline.PipelineWithMappings,
	sourceDataPlaneID uuid.UUID,
	deviationStage PipelineStage,
	checkStartTime, checkEndTime time.Time,
) *PipelineFlowDeviationAlert {
	return &PipelineFlowDeviationAlert{
		Tenant:            tenant,
		Pipeline:          pipelineWithMappings,
		SourceID:          pipelineWithMappings.LogSourceID,
		DestinationID:     pipelineWithMappings.DestinationID,
		SourceName:        pipelineWithMappings.SourceName,
		DestinationName:   pipelineWithMappings.DestinationName,
		SourceDataPlaneID: sourceDataPlaneID,
		DeviationStage:    deviationStage,
		CheckStartTime:    checkStartTime,
		CheckEndTime:      checkEndTime,
		DetectionTime:     time.Now().UTC(),
	}
}
