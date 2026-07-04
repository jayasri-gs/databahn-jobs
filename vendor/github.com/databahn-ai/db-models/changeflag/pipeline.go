package changeflag

type FlagPipeline struct {
	PipelineId         string      `json:"pipeline_id"`
	TenantId           string      `json:"tenant_id"`
	SourceId           string      `json:"source_id"`
	PipelineNext       string      `json:"pipeline_next"`
	FreeFormConfig     interface{} `json:"free_form_config,omitempty"` // Can be string, map, or null
	IsFreeFormPipeline bool        `json:"is_free_form_pipeline"`
}
