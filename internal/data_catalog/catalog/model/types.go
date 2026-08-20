package model

import "github.com/google/uuid"

const (
	DispenserDatabahnStorage = "databahnstorage"
	DispenserS3Parquet       = "S3Parquet"
)

// Field represents a row in the data_catalog table.
type Field struct {
	ID        int64     `gorm:"column:id"`
	Name      string    `gorm:"column:name"`
	FieldType string    `gorm:"column:field_type"`
	SourceID  uuid.UUID `gorm:"column:source_id"`
	DestID    uuid.UUID `gorm:"column:destination_id"`
	TenantID  uuid.UUID `gorm:"column:tenant_id"`
}

func (Field) TableName() string { return "data_catalog" }

// SearchConfig is the search_configuration JSON stored per source.
type SearchConfig struct {
	S3Configuration struct {
		AthenaTable           string `json:"athenaTable"`
		S3Location            string `json:"s3Location"`
		DatabahnStorageRegion string `json:"databahnStorageRegion"`
	} `json:"s3Configuration"`
}
