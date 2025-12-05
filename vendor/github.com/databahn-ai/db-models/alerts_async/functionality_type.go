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
	CloudSourcePullFailure           = functionalityTypeEnum{value: "cloud_source_pull_failure"}
	Suppression                      = functionalityTypeEnum{value: "suppression"}
	ConfigurationProcessingFailure   = functionalityTypeEnum{value: "configuration_processing_failure"}
	DataForwardingFailure            = functionalityTypeEnum{value: "data_forwarding_failure"}
	CheckpointUpdateFailure          = functionalityTypeEnum{value: "checkpoint_update_failure"}
	CheckpointParsingFailure         = functionalityTypeEnum{value: "checkpoint_parsing_failure"}
	SourceConfigParsingFailure       = functionalityTypeEnum{value: "source_config_parsing_failure"}
	SourceConfigProcessingFailure    = functionalityTypeEnum{value: "source_config_processing_failure"}
	IngestionChecker                 = functionalityTypeEnum{value: "ingestion_checker"}
	DeliveryChecker                  = functionalityTypeEnum{value: "delivery_checker"}
	UnparsedChecker                  = functionalityTypeEnum{value: "unparsed_checker"}
	DispenserThresholdChecker        = functionalityTypeEnum{value: "dispenser_threshold_checker"}
	DeviceReputationChecker          = functionalityTypeEnum{value: "device_reputation_checker"}
	DataRatioAlertChecker            = functionalityTypeEnum{value: "data_ratio_alert_checker"}
	VolumeAnomalyAlertChecker        = functionalityTypeEnum{value: "volume_anomaly_alert_checker"}
	VolumeDeviationChecker           = functionalityTypeEnum{value: "volume_deviation_checker"}
	HealthCheck                      = functionalityTypeEnum{value: "health_check"}
	SmartEdgeError                   = functionalityTypeEnum{value: "smart_edge_error"}
	AgentError                       = functionalityTypeEnum{value: "agent_error"}
	HttpUnauthorizedAccess           = functionalityTypeEnum{value: "unauthorized_access"}
	HttpResourceNotFound             = functionalityTypeEnum{value: "resource_not_found"}
	HttpBadGateway                   = functionalityTypeEnum{value: "bad_gateway"}
	HttpBadRequest                   = functionalityTypeEnum{value: "bad_request"}
	HttpForbiddenAccess              = functionalityTypeEnum{value: "forbidden_access"}
	HttpInternalServerError          = functionalityTypeEnum{value: "internal_server_error"}
	HttpResourceMovedPermanently     = functionalityTypeEnum{value: "resource_moved_permanently"}
	HttpResourceMovedTemporarily     = functionalityTypeEnum{value: "resource_moved_temporarily"}
	HttpServiceUnavailable           = functionalityTypeEnum{value: "service_unavailable"}
	ServiceUnavailable               = functionalityTypeEnum{value: "service_unavailable"}
	HttpGatewayTimeout               = functionalityTypeEnum{value: "gateway_timeout"}
	HttpClientError                  = functionalityTypeEnum{value: "client_error"}
	HttpServerError                  = functionalityTypeEnum{value: "server_error"}
	UnknownError                     = functionalityTypeEnum{value: "unknown_error"}
	AuditReportGeneration            = functionalityTypeEnum{value: "audit_report_generation"}
	RateLimitExceeded                = functionalityTypeEnum{value: "rate_limit_exceeded"}
	FileProcessingFailure            = functionalityTypeEnum{value: "file_processing_failure"}
	DataParsingFailure               = functionalityTypeEnum{value: "data_parsing_failure"}
	DataProcessingFailure            = functionalityTypeEnum{value: "data_processing_failure"}
	DataConversionFailure            = functionalityTypeEnum{value: "data_conversion_failure"}
	DeploymentStatus                 = functionalityTypeEnum{value: "deployment_status"}
	LowSuccessRate                   = functionalityTypeEnum{value: "low_success_rate"}
	AgentRunnerError                 = functionalityTypeEnum{value: "agent_runner_error"}
	AgentRunnerUpgrade               = functionalityTypeEnum{value: "agent_runner_upgrade"}
	AgentUsageUploadFailure          = functionalityTypeEnum{value: "agent_usage_upload_failure"}
	VcDropRuleIncrease               = functionalityTypeEnum{value: "vc_drop_rule_increase"}
	VcPipelineDataReduction          = functionalityTypeEnum{value: "vc_pipeline_data_reduction"}
	VcUnmatchedNoRouteProcessor      = functionalityTypeEnum{value: "vc_unmatched_no_route_processor"}
	JobFailure                       = functionalityTypeEnum{value: "job_failure"}
	AgentDiagnosticLogsUploadFailure = functionalityTypeEnum{value: "agent_diagnostic_logs_upload_failure"}
	AgentSilentInput                 = functionalityTypeEnum{value: "agent_silent_input"}
	SchemaLessNormalizationConflict  = functionalityTypeEnum{value: "schema_less_normalization_conflict"}
	TransformationFieldsDrop         = functionalityTypeEnum{value: "transformation_fields_drop"}
)
