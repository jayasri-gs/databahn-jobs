package data_transformation

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	TRANSFORMATION_FUNCTION_TYPE_RENAME TransformationFunctionType = "RENAME"

	TRANSFORMATION_TYPE_SENTINEL = "SENTINEL_OBJECT"
)

// Enum definitions
type Status string
type TransformationType string
type TransformationOutputFormat string
type TransformationDestinationType string
type TransformationFunctionType string

type SourceField struct {
	DatabahnAttribute string `json:"databahnAttribute,omitempty"`
}

type TransformationOperator struct {
	// Define fields for TransformationOperator as needed
}
type RenameField struct {
	Include           *bool                    `json:"include,omitempty"`
	LogAttribute      string                   `json:"logAttribute,omitempty"`
	RenameAttribute   string                   `json:"renameAttribute,omitempty"`
	DatabahnAttribute string                   `json:"databahnAttribute,omitempty"`
	Operators         []TransformationOperator `json:"operators,omitempty"`
}
type DerivedField struct {
	Include       *bool                    `json:"include,omitempty"`
	AttributeName string                   `json:"attributeName,omitempty"`
	SourceField   *SourceField             `json:"sourceField,omitempty"`
	Operators     []TransformationOperator `json:"operators,omitempty"`
}

type TransformationRenameConfig struct {
	RenameFields  []RenameField  `json:"renameFields,omitempty"`
	DerivedFields []DerivedField `json:"derivedFields,omitempty"`
}
type TransformationFunction struct {
	ID                   uuid.UUID                  `gorm:"type:uuid" json:"id"`
	Type                 TransformationFunctionType `gorm:"type:varchar(20)" json:"type"`
	Transformation       *DataTransformation        `gorm:"foreignKey:DataTransformationID" json:"transformation"`
	DataTransformationID uuid.UUID                  `gorm:"type:uuid" json:"data_transformation_id"`
	RenameConfig         json.RawMessage            `gorm:"type:json" json:"rename_config"`
}

type DataTransformation struct {
	ID                     uuid.UUID                  `gorm:"type:uuid" json:"id"`
	Name                   string                     `gorm:"type:varchar(100)" json:"name"`
	Description            string                     `gorm:"type:varchar(512)" json:"description"`
	TenantID               uuid.UUID                  `gorm:"type:uuid" json:"tenant_id"`
	CustomerID             *uuid.UUID                 `gorm:"type:uuid" json:"customer_id"`
	Status                 Status                     `gorm:"type:varchar(20)" json:"status"`
	CreatedAt              time.Time                  `json:"created_at"`
	UpdatedAt              time.Time                  `json:"updated_at"`
	CreatedBy              string                     `gorm:"type:varchar(100)" json:"created_by"`
	UpdatedBy              string                     `gorm:"type:varchar(100)" json:"updated_by"`
	PipelineID             *uuid.UUID                 `gorm:"type:uuid" json:"pipeline_id"`
	TransformationFunction *TransformationFunction    `json:"transformation_function"`
	Type                   TransformationType         `gorm:"type:varchar(20)" json:"type"`
	OutputFormat           TransformationOutputFormat `gorm:"type:varchar(20)" json:"output_format"`
	IncludeRawEvent        bool                       `json:"include_raw_event"`
	DataPlaneID            *uuid.UUID                 `gorm:"type:uuid" json:"data_plane_id"`
}

// TableName returns the table name for DataTransformation
func (dt *DataTransformation) TableName() string {
	return "data_transformation"
}

// TableName returns the table name for TransformationFunction
func (tf *TransformationFunction) TableName() string {
	return "transformation_function"
}

// GetDataTransformationsByTypeAndFunctionTypeByTenant fetches data transformations filtered by tenant ID
func GetDataTransformationsByTypeAndFunctionTypeByTenant(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) ([]DataTransformation, error) {
	var transformations []DataTransformation

	err := db.WithContext(ctx).
		Preload("TransformationFunction").
		Joins("JOIN transformation_function tf ON data_transformation.id = tf.data_transformation_id").
		Where("data_transformation.type = ? AND tf.type = ? AND data_transformation.tenant_id = ?",
			TRANSFORMATION_TYPE_SENTINEL, TRANSFORMATION_FUNCTION_TYPE_RENAME, tenantID).
		Find(&transformations).Error

	if err != nil {
		return nil, err
	}

	return transformations, nil
}

// HasActiveTransformation checks if a pipeline has any active transformation configurations
func HasActiveTransformation(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (bool, int64, error) {
	var count int64
	err := db.WithContext(ctx).Table("data_transformation").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID, tenantID, "ACTIVE").
		Count(&count).Error

	return count > 0, count, err
}
