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
	CloudSourcePullFailure         = functionalityTypeEnum{value: "cloud_source_pull_failure"}
	Suppression                    = functionalityTypeEnum{value: "suppression"}
	ConfigurationProcessingFailure = functionalityTypeEnum{value: "configuration_processing_failure"}
	CheckpointUpdateFailure        = functionalityTypeEnum{value: "checkpoint_update_failure"}
	CheckpointParsingFailure       = functionalityTypeEnum{value: "checkpoint_parsing_failure"}
	SourceConfigParsingFailure     = functionalityTypeEnum{value: "source_config_parsing_failure"}
	SourceConfigProcessingFailure  = functionalityTypeEnum{value: "source_config_processing_failure"}
	IngestionChecker               = functionalityTypeEnum{value: "ingestion_checker"}
	HealthCheck                    = functionalityTypeEnum{value: "health_check"}
	HttpUnauthorizedAccess         = functionalityTypeEnum{value: "unauthorized_access"}
	HttpResourceNotFound           = functionalityTypeEnum{value: "resource_not_found"}
	HttpBadGateway                 = functionalityTypeEnum{value: "bad_gateway"}
	HttpBadRequest                 = functionalityTypeEnum{value: "bad_request"}
	HttpForbiddenAccess            = functionalityTypeEnum{value: "forbidden_access"}
	HttpInternalServerError        = functionalityTypeEnum{value: "internal_server_error"}
	HttpResourceMovedPermanently   = functionalityTypeEnum{value: "resource_moved_permanently"}
	HttpResourceMovedTemporarily   = functionalityTypeEnum{value: "resource_moved_temporarily"}
	HttpServiceUnavailable         = functionalityTypeEnum{value: "service_unavailable"}
	ServiceUnavailable             = functionalityTypeEnum{value: "service_unavailable"}
	HttpGatewayTimeout             = functionalityTypeEnum{value: "gateway_timeout"}
	HttpClientError                = functionalityTypeEnum{value: "client_error"}
	HttpServerError                = functionalityTypeEnum{value: "server_error"}
	UnknownError                   = functionalityTypeEnum{value: "unknown_error"}
)
