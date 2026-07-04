package enrichment

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type Enrichment struct {
	Id             uuid.UUID                            `json:"id"`
	Name           string                               `json:"name"`
	Description    string                               `json:"description"`
	TenantId       uuid.UUID                            `json:"tenant_id"`
	LookupId       uuid.UUID                            `json:"lookup_id"`
	SourceId       uuid.UUID                            `json:"source_id"`
	DestinationId  uuid.UUID                            `json:"destination_id"`
	PipelineId     uuid.UUID                            `json:"pipeline_id"`
	CacheRequestId string                               `json:"cache_request_id"`
	Config         datatypes.JSONType[EnrichmentConfig] `json:"config"`
	Status         string                               `json:"status"`
	CreatedAt      time.Time                            `json:"created_at"`
	UpdatedAt      time.Time                            `json:"updated_at"`
	CreatedBy      uuid.UUID                            `json:"created_by"`
	UpdatedBy      uuid.UUID                            `json:"updated_by"`
}

func (e Enrichment) GetEnrichmentAttributes() []string {
	var fields []string
	for _, mapping := range e.Config.Data().Mappings {
		fields = append(fields, mapping.SourceField)
	}
	return fields
}

type EnrichmentConfig struct {
	Schema   string    `json:"schema"`
	Match    Mapping   `json:"match"`
	Mappings []Mapping `json:"mappings"`
}

type Mapping struct {
	SourceField             string `json:"source_field"`
	LookupField             string `json:"lookup_field"`
	SourceFieldExtractRegex string `json:"source_field_extract_regex,omitempty"`
	DefaultEnrichmentValue  string `json:"default_enrichment_value,omitempty"`
}
