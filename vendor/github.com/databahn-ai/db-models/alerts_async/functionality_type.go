package alerts_async

type FunctionalityType interface {
	String() string
	privateFunctionalityType()
}

type functionalityTypeEnum struct {
	value string
}

func (f functionalityTypeEnum) String() string {
	return f.value
}

func (f functionalityTypeEnum) privateFunctionalityType() {
}

var (
	CloudSourcePullFailure        = functionalityTypeEnum{value: "cloud_source_pull_failure"}
	CheckpointUpdateFailure       = functionalityTypeEnum{value: "checkpoint_update_failure"}
	SourceConfigParsingFailure    = functionalityTypeEnum{value: "source_config_parsing_failure"}
	SourceConfigProcessingFailure = functionalityTypeEnum{value: "source_config_processing_failure"}
	IngestionChecker = functionalityTypeEnum{value: "ingestion_checker"}
)
