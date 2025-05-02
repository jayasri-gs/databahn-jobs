package changeflag

type FlagPipeline struct {
	PipelineId   string `json:"pipeline_id"`
	TenantId     string `json:"tenant_id"`
	SourceId     string `json:"source_id"`
	PipelineNext string `json:"pipeline_next"`
}
