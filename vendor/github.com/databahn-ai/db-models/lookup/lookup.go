package lookup

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type Lookup struct {
	Id               uuid.UUID                     `json:"id"`
	FilePath         string                        `json:"file_path"`
	Name             string                        `json:"name"`
	Description      string                        `json:"description"`
	FileName         string                        `json:"file_name"`
	TenantId         uuid.UUID                     `json:"tenant_id"`
	Type             string                        `json:"type"`
	IndexRequestId   string                        `json:"index_request_id"`
	CacheRequestId   string                        `json:"cache_request_id"`
	CsvConfig        datatypes.JSONType[CsvConfig] `json:"csv_config"`
	IndexName        string                        `json:"index_name"`
	RequestType      string                        `json:"request_type"`
	Status           string                        `json:"status"`
	CreatedAt        time.Time                     `json:"created_at"`
	UpdatedAt        time.Time                     `json:"updated_at"`
	ExcludeCSVHeader bool                          `json:"exclude_csv_header"`
}

type CsvConfig struct {
	Schema    string     `json:"schema"`
	Format    string     `json:"format"`
	CsvFields []CsvField `json:"csv_fields"`
}

type CsvField struct {
	CsvIndex  int    `json:"csv_index"`
	Name      string `json:"name"`
	CsvHeader string `json:"csv_header"`
}
