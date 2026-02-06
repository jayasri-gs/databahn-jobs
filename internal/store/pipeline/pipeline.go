package pipeline

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Pipeline represents a pipeline in the database
type Pipeline struct {
	ID            uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	CreatedAt     time.Time `gorm:"type:timestamp" json:"created_at"`
	CustomerID    uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	Description   string    `gorm:"type:varchar(512)" json:"description"`
	DeviceType    string    `gorm:"type:varchar(100)" json:"device_type"`
	LogSourceType string    `gorm:"type:varchar(100)" json:"log_source_type"`
	Name          string    `gorm:"type:varchar(255)" json:"name"`
	Status        string    `gorm:"type:varchar(50)" json:"status"`
	TenantID      uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	UpdatedAt     time.Time `gorm:"type:timestamp" json:"updated_at"`
	Vendor        string    `gorm:"type:varchar(100)" json:"vendor"`
	DataPlaneID   uuid.UUID `gorm:"type:uuid" json:"data_plane_id"`
}

// PipelineLogSourceMapping represents the mapping between pipelines and log sources
type PipelineLogSourceMapping struct {
	CustomerID  uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	TenantID    uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	LogSourceID uuid.UUID `gorm:"type:uuid" json:"log_source_id"`
	PipelineID  uuid.UUID `gorm:"type:uuid" json:"pipeline_id"`
}

// PipelineDestinationMapping represents the mapping between pipelines and destinations
type PipelineDestinationMapping struct {
	CustomerID     uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	FormatOverride string    `gorm:"type:varchar(100)" json:"format_override"`
	TenantID       uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	DestinationID  uuid.UUID `gorm:"type:uuid" json:"destination_id"`
	PipelineID     uuid.UUID `gorm:"type:uuid" json:"pipeline_id"`
}

// PipelineWithMappings represents a pipeline with its source and destination mappings
type PipelineWithMappings struct {
	Pipeline        Pipeline
	LogSourceID     uuid.UUID
	DestinationID   uuid.UUID
	SourceName      string
	DestinationName string
}

func (p *Pipeline) TableName() string {
	return "pipelines"
}

func (plsm *PipelineLogSourceMapping) TableName() string {
	return "pipeline_log_sources_mapping"
}

func (pdm *PipelineDestinationMapping) TableName() string {
	return "pipeline_destinations_mapping"
}

// GetActivePipelinesWithMappings gets all active pipelines for a tenant along with their source and destination mappings
func GetActivePipelinesWithMappings(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) ([]PipelineWithMappings, error) {
	var results []PipelineWithMappings

	query := `
		SELECT 
			p.id, p.name, p.status, p.tenant_id, p.data_plane_id, p.created_at, p.updated_at,
			pls.log_source_id, pd.destination_id,
			ls.name as source_name, d.name as destination_name
		FROM pipelines p
		JOIN pipeline_log_sources_mapping pls ON p.id = pls.pipeline_id
		JOIN pipeline_destinations_mapping pd ON p.id = pd.pipeline_id
		JOIN log_source ls ON pls.log_source_id = ls.id
		JOIN destination d ON pd.destination_id = d.id
		WHERE p.tenant_id = ? AND p.status = 'ACTIVE'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var result PipelineWithMappings
		err := rows.Scan(
			&result.Pipeline.ID,
			&result.Pipeline.Name,
			&result.Pipeline.Status,
			&result.Pipeline.TenantID,
			&result.Pipeline.DataPlaneID,
			&result.Pipeline.CreatedAt,
			&result.Pipeline.UpdatedAt,
			&result.LogSourceID,
			&result.DestinationID,
			&result.SourceName,
			&result.DestinationName,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

// GetActivePipelines gets all active pipelines for a tenant
func GetActivePipelines(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) ([]Pipeline, error) {
	var pipelines []Pipeline
	err := db.WithContext(ctx).Where("tenant_id = ? AND status = ?", tenantID, "ACTIVE").Find(&pipelines).Error
	return pipelines, err
}

// GetPipelineLogSources gets all log source mappings for a pipeline
func GetPipelineLogSources(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID) ([]PipelineLogSourceMapping, error) {
	var mappings []PipelineLogSourceMapping
	err := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID).Find(&mappings).Error
	return mappings, err
}

// GetPipelineDestinations gets all destination mappings for a pipeline
func GetPipelineDestinations(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID) ([]PipelineDestinationMapping, error) {
	var mappings []PipelineDestinationMapping
	err := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID).Find(&mappings).Error
	return mappings, err
}

// HasActiveRules checks if a pipeline has any active volume control rules
func HasActiveRules(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (bool, int64, error) {
	var count int64
	err := db.WithContext(ctx).Table("vc_rule").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID.String(), tenantID.String(), "ACTIVE").
		Count(&count).Error

	return count > 0, count, err
}

// GetPipelinesByDestinationAndStatus gets all pipelines for a specific tenant, destination and status
func GetPipelinesByDestinationAndStatus(ctx context.Context, db *gorm.DB, tenantID, destinationID uuid.UUID, status string) ([]Pipeline, error) {
	var pipelines []Pipeline

	query := `
		SELECT DISTINCT p.*
		FROM pipelines p
		JOIN pipeline_destinations_mapping pd ON p.id = pd.pipeline_id
		WHERE p.tenant_id = ? AND pd.destination_id = ? AND p.status = ?
	`

	err := db.WithContext(ctx).Raw(query, tenantID, destinationID, status).Scan(&pipelines).Error
	if err != nil {
		return nil, err
	}

	return pipelines, nil
}
