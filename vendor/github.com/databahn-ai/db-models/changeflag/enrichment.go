package changeflag

import (
	"github.com/databahn-ai/db-models/enrichment"
	"github.com/databahn-ai/db-models/lookup"
)

// FlagEnrichment change flag model for enrichment
type FlagEnrichment struct {
	Id                   string                      `json:"id"`
	Name                 string                      `json:"name"`
	Description          string                      `json:"description"`
	SourceId             string                      `json:"source_id"`
	DestinationId        string                      `json:"destination_id"`
	PipelineId           string                      `json:"pipeline_id"`
	TenantId             string                      `json:"tenant_id"`
	CacheRequestId       string                      `json:"cache_request_id"`
	Config               enrichment.EnrichmentConfig `json:"config"`
	LookupName           string                      `json:"lookup_name"`
	LookupId             string                      `json:"lookup_id"`
	LookupCsvConfig      lookup.CsvConfig            `json:"lookup_csv_config"`
	ReferencedAttributes []string                    `json:"referenced_attributes"`
}
