package pramaan

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/kafka"
	change_flag "github.com/databahn-ai/db-models/changeflag"
	"github.com/databahn-ai/db-models/enrichment"
	"github.com/databahn-ai/db-models/lookup"
	"github.com/databahn-ai/db-models/rule"
	"github.com/google/uuid"
)

func NewPipelineChangeFlag(t TestLogger, metadata *KafkaMetaData, pipelineNext, action string) (kafka.Message, string) {
	cfPipeline := change_flag.FlagPipeline{
		PipelineId:   metadata.PipelineId,
		TenantId:     metadata.TenantId,
		SourceId:     metadata.SourceId,
		PipelineNext: pipelineNext,
	}
	entity := map[string]interface{}{"entity": cfPipeline}
	entityJson, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Failed to marshal entity: %s", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metadata.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte("pipeline")})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metadata.PipelineId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("pipeline change")})
	message := kafka.Message{Key: []byte(metadata.PipelineId), Message: entityJson, Headers: headers}
	return message, requestId
}

func NewKafkaInputMessage(t *testing.T, metadata *KafkaMetaData, pipelineDone, pipelineNext, body string) kafka.Message {
	var inputHeaders []kafka.Header
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_pipeline_done", Value: []byte(pipelineDone)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_pipeline_next", Value: []byte(pipelineNext)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_device_type", Value: []byte(metadata.DeviceType)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_device_vendor", Value: []byte(metadata.DeviceVendor)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_log_type", Value: []byte(metadata.LogType)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_tenant_id", Value: []byte(metadata.TenantId)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_event_source_id", Value: []byte(metadata.SourceId)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_source_name", Value: []byte(t.Name())})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_event_id", Value: []byte(metadata.EventId)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_edge_ts", Value: []byte(metadata.EdgeTs)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_pipeline_id", Value: []byte(metadata.PipelineId)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "rule_id", Value: []byte(metadata.RuleId)})
	inputHeaders = append(inputHeaders, kafka.Header{Key: "db_suppression_key", Value: []byte(metadata.SuppressionKey)})

	return kafka.Message{Key: []byte(metadata.PipelineId), Message: []byte(body), Headers: inputHeaders}
}

func NewLookupChangeFlag(t TestLogger, tenantId, lookupId, requestId string, action string) (kafka.Message, string) {
	lookup := change_flag.FlagLookup{
		Id:        lookupId,
		TenantId:  tenantId,
		RequestId: requestId,
	}
	entity := map[string]interface{}{"entity": lookup}
	entityJson, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Failed to marshal entity: %s", err)
	}
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(tenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityLookup)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(lookupId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("my lookup")})
	message := kafka.Message{Key: []byte(lookupId), Message: entityJson, Headers: headers}

	return message, requestId
}

func NewGlobalDestinationChangeFlag(t TestLogger, metaData *KafkaMetaData, action string) (kafka.Message, string) {
	eventTypeMap := map[string]bool{"UNPARSED": true, "UNDELIVERED": true}
	globalDestinationConfig := change_flag.GlobalDestinationConfig{
		EventTypeMap: eventTypeMap,
	}
	flagGlobalDestination := change_flag.FlagGlobalDestination{
		TenantId:        metaData.TenantId,
		DestinationId:   metaData.DestinationId,
		DestinationType: metaData.DestinationType,
		Configuration:   globalDestinationConfig,
	}
	entity := map[string]interface{}{"entity": flagGlobalDestination}
	entityJson, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Failed to marshal entity: %s", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityGlobalDestination)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.GlblDstnEntityId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("global dest")})
	message := kafka.Message{Key: []byte(metaData.GlblDstnEntityId), Message: entityJson, Headers: headers}

	return message, requestId
}

func NewSamplingFlagRuleMessage(t TestLogger, metaData *KafkaMetaData, samplingRate int64, priority int, scope, actionType, ruleFilterQuery, action string) (kafka.Message, string) {
	ruleName := "some sampling rule:" + uuid.New().String()
	flagRule := change_flag.FlagRule{
		Id:                   metaData.RuleId,
		Name:                 ruleName,
		Scope:                scope,
		TenantId:             metaData.TenantId,
		PipelineId:           metaData.PipelineId,
		OldPipelineId:        metaData.PipelineId,
		DestinationId:        metaData.DestinationId,
		Priority:             priority,
		ActionType:           actionType,
		Type:                 "Sampling",
		SamplingRate:         samplingRate,
		DestinationType:      metaData.DestinationType,
		EventSourceId:        metaData.SourceId,
		RuleFilterQuery:      ruleFilterQuery,
		ReferencedAttributes: []string{"accountname", "ipaddress"},
	}

	flagRuleEntity := map[string]interface{}{"entity": flagRule}
	flagRuleEntityJson, err := json.Marshal(flagRuleEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagRuleEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityRule)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.RuleId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(ruleName)})
	message := kafka.Message{Key: []byte(metaData.RuleId), Message: flagRuleEntityJson, Headers: headers}

	return message, requestId
}

func NewEnrichmentChangeFlag(t TestLogger, metaData *KafkaMetaData, id, lookupId, cacheRequestId string, match enrichment.Mapping, mappings []enrichment.Mapping, action string) (kafka.Message, string) {
	name := "enrichment " + uuid.New().String()
	flagEnrichment := change_flag.FlagEnrichment{
		Id:             id,
		Name:           name,
		Description:    "",
		SourceId:       metaData.SourceId,
		DestinationId:  metaData.DestinationId,
		PipelineId:     metaData.PipelineId,
		TenantId:       metaData.TenantId,
		CacheRequestId: cacheRequestId,
		Config: enrichment.EnrichmentConfig{
			Schema:   "v1",
			Match:    match,
			Mappings: mappings,
		},
		LookupName:      "lookup",
		LookupId:        lookupId,
		LookupCsvConfig: lookup.CsvConfig{},
	}
	flagEntity := map[string]interface{}{"entity": flagEnrichment}
	flagEntityJson, err := json.Marshal(flagEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityEnrichment)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.RuleId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(name)})
	message := kafka.Message{Key: []byte(id), Message: flagEntityJson, Headers: headers}

	return message, requestId
}

func NewRouterProcessingFlag(t TestLogger, metaData *KafkaMetaData, unmatchedAction, explicitDropAction, secondaryDestType, action string) (kafka.Message, string) {
	flagRouteProcessor := change_flag.FlagRouteProcessor{
		Id:                       metaData.RouteProcessorId,
		UnmatchedAction:          unmatchedAction,
		ExplicitDropAction:       explicitDropAction,
		SecondaryDestinationId:   metaData.SecondaryDestinationId,
		SecondaryDestinationType: secondaryDestType,
		PipelineId:               metaData.PipelineId,
		TenantId:                 metaData.TenantId,
		PrimaryDestinationId:     metaData.DestinationId,
		PrimaryDestinationType:   metaData.DestinationType,
		SourceId:                 metaData.SourceId,
		IsVcRouteProcessor:       true,
	}
	flagRouteProcessorEntity := map[string]interface{}{"entity": flagRouteProcessor}
	flagRouteProcessorEntityJson, err := json.Marshal(flagRouteProcessorEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagRouteProcessorEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityRouteProcessor)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(flagRouteProcessor.Id)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("route processor")})
	message := kafka.Message{Key: []byte(flagRouteProcessor.Id), Message: flagRouteProcessorEntityJson, Headers: headers}

	return message, requestId
}

func NewSuppressionFlagRule(t TestLogger, metaData *KafkaMetaData, eventCount, priority int, scope, actionType, action string, filterQuery string) (kafka.Message, string) {
	suppressionInterval := rule.SuppressionInterval{
		Unit:  "MINUTES",
		Value: 5,
	}

	suppressionConfig := rule.SuppressionConfig{
		Attributes: []string{"accountname", "ipaddress"},
		EventCount: int64(eventCount),
		Interval:   suppressionInterval,
		MaxKeys:    0,
	}

	ruleName := "suppression rule" + uuid.New().String()
	flagRule := change_flag.FlagRule{
		Id:                   metaData.RuleId,
		Name:                 ruleName,
		Scope:                scope,
		TenantId:             metaData.TenantId,
		PipelineId:           metaData.PipelineId,
		OldPipelineId:        metaData.PipelineId,
		DestinationId:        metaData.DestinationId,
		Priority:             priority,
		ActionType:           actionType,
		Type:                 "Suppression",
		DestinationType:      metaData.DestinationType,
		EventSourceId:        metaData.SourceId,
		SuppressionConfig:    suppressionConfig,
		RuleFilterQuery:      filterQuery,
		ReferencedAttributes: []string{"accountname", "ipaddress"},
	}

	flagRuleEntity := map[string]interface{}{"entity": flagRule}
	flagRuleEntityJson, err := json.Marshal(flagRuleEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagRuleEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityRule)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.RuleId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(ruleName)})
	message := kafka.Message{Key: []byte(metaData.RuleId), Message: flagRuleEntityJson, Headers: headers}

	return message, requestId

}

func NewAggregationFlagRuleMessage(t TestLogger, metaData *KafkaMetaData, aggregationInterval int, priority int, ruleFilterQuery, action string, passThrough bool, aggAttributes []string) (kafka.Message, string) {
	aggregationConfig := rule.AggregationConfig{
		GroupByAttributes: aggAttributes,
		Interval: rule.AggregationInterval{
			Value: aggregationInterval,
			Unit:  "MINUTES",
		},
		SelectAttributes: []rule.AggregationSelectAttribute{
			{
				Attribute: "accountname",
				Function:  "COUNT",
				Label:     "accountname",
				Arguments: []string{},
				Implicit:  false,
			},
		},
		OutputMode: rule.OutputMode{
			Stats:       true,
			PassThrough: passThrough,
		},
	}
	ruleName := "aggregation rule:" + uuid.New().String()
	flagRule := change_flag.FlagRule{
		Id:                   metaData.RuleId,
		Name:                 ruleName,
		Scope:                "CLOUD",
		TenantId:             metaData.TenantId,
		PipelineId:           metaData.PipelineId,
		OldPipelineId:        metaData.PipelineId,
		DestinationId:        metaData.DestinationId,
		Priority:             priority,
		ActionType:           "PUBLISH",
		Type:                 "Data Aggregation",
		AggregationConfig:    aggregationConfig,
		DestinationType:      metaData.DestinationType,
		EventSourceId:        metaData.SourceId,
		RuleFilterQuery:      ruleFilterQuery,
		ReferencedAttributes: []string{"accountname", "ipaddress"},
	}

	flagRuleEntity := map[string]interface{}{"entity": flagRule}
	flagRuleEntityJson, err := json.Marshal(flagRuleEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagRuleEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityRule)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.RuleId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(ruleName)})
	message := kafka.Message{Key: []byte(metaData.RuleId), Message: flagRuleEntityJson, Headers: headers}

	return message, requestId
}

func NewFilteringFlagRule(t TestLogger, metaData *KafkaMetaData, priority int, scope, actionType, ruleName, action, filterQuery string) (kafka.Message, string) {
	flagRule := change_flag.FlagRule{
		Id:              metaData.RuleId,
		Name:            ruleName,
		Scope:           scope,
		TenantId:        metaData.TenantId,
		PipelineId:      metaData.PipelineId,
		OldPipelineId:   metaData.PipelineId,
		DestinationId:   metaData.DestinationId,
		Priority:        priority,
		ActionType:      actionType,
		Type:            "Selective Filtering",
		DestinationType: metaData.DestinationType,
		EventSourceId:   metaData.SourceId,
		RuleFilterQuery: filterQuery,
	}

	flagRuleEntity := map[string]interface{}{"entity": flagRule}
	flagRuleEntityJson, err := json.Marshal(flagRuleEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagRuleEntity : %v", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityRule)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.RuleId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(ruleName)})
	message := kafka.Message{Key: []byte(metaData.RuleId), Message: flagRuleEntityJson, Headers: headers}

	return message, requestId

}

func NewTransformationChangeFlag(t TestLogger, metaData *KafkaMetaData, transFunc change_flag.FlagTransformFunction, name, transformationType, transformationOutput, action string, includeRawEvent bool) (kafka.Message, string) {
	changeFlag := change_flag.FlagTransform{
		ID:                   metaData.TransformationId,
		Name:                 name,
		SourceId:             metaData.SourceId,
		DestinationId:        metaData.DestinationId,
		DestinationType:      metaData.DestinationType,
		PipelineId:           metaData.PipelineId,
		TenantId:             metaData.TenantId,
		TransformationType:   transformationType,
		TransformationOutput: transformationOutput,
		Device:               metaData.DeviceType,
		Vendor:               metaData.DeviceVendor,
		LogType:              metaData.LogType,
		IncludeRawEvent:      includeRawEvent,
		Function:             transFunc,
	}

	// Marshal the entity
	changeFlagEntity := map[string]any{"entity": changeFlag}
	changeFlagEntityJson, err := json.Marshal(changeFlagEntity)
	if err != nil {
		t.Fatalf("Failed to marshal entity FlagTransformEntity : %v", err)
	}

	// Create headers
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metaData.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte(constants.EntityTransformer)})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metaData.TransformationId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte(name)})

	message := kafka.Message{
		Key:     []byte(metaData.TransformationId),
		Message: changeFlagEntityJson,
		Headers: headers,
	}

	return message, requestId
}

func NewRandomEvent(t TestLogger, accountName, devicehostname, ipaddress string, customnumber2, deviceprocessid int) string {
	rawevent := fmt.Sprintf(`Jun 25 10:21:48 %s sentinel-agent[4]: [INFO] [AUDIT_SUCCESS] EventID=4985 AlertID=64882052 SessionID=0x3e7 User=%s Domain=test SrcProc=sentinel-agent[2] Severity=INFO Message="The state of a transaction has changed" Path=/opt/sentinelone/agent/2025.05.25/sentinel-agent SIEMID={44846625-5278-4694-B6BA-353B0328C55D} DBConnector=connector-id Category=Security IP=%s`, devicehostname, accountName, ipaddress)
	obj := map[string]interface{}{
		"accountname":       accountName,
		"alertid":           "64882052",
		"baseeventid":       "4985",
		"customnumber2":     customnumber2,
		"customstring6":     "Security",
		"db_connector_id":   "connector-id",
		"devicehostname":    devicehostname,
		"deviceprocessid":   deviceprocessid,
		"deviceseverity":    "INFO",
		"eventtime":         1744161708000,
		"eventtype":         "AUDIT_SUCCESS",
		"filepath":          "/opt/sentinelone/agent/2025.05.25/sentinel-agent",
		"message":           "The state of a transaction has changed",
		"rawevent":          rawevent,
		"sessionid":         "0x3e7",
		"siemid":            "{44846625-5278-4694-B6BA-353B0328C55D}",
		"sourcentdomain":    "test",
		"sourceprocessid":   2,
		"sourceprocessname": "sentinel-agent",
		"ipaddress":         ipaddress,
	}

	json, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Error marshalling object", err)
	}
	return string(json)
}

func NewSourceChangeFlag(t TestLogger, metadata *KafkaMetaData, config map[string]string, advancedConfig change_flag.FlagSourceAdvancedConfig, schemalessEnabled bool, schemaLessConfig *change_flag.SchemaLessConfig, action string) (kafka.Message, string) {
	source := change_flag.FlagSource{
		Id:                metadata.SourceId,
		TenantId:          metadata.TenantId,
		Name:              metadata.SourceName,
		Scope:             metadata.Scope,
		Type:              metadata.SourceType,
		Device:            metadata.DeviceType,
		Vendor:            metadata.DeviceVendor,
		Version:           metadata.SourceVersion,
		Status:            metadata.SourceStatus,
		Config:            config,
		SecretId:          metadata.SourceSecretId,
		PullMechanism:     metadata.PullMechanism,
		AdvancedConfig:    advancedConfig,
		SchemaLessConfig:  schemaLessConfig,
		SchemaLessEnabled: schemalessEnabled,
	}
	entity := map[string]interface{}{"entity": source}
	entityJson, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Failed to marshal entity: %s", err)
	}
	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metadata.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte("source")})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metadata.PipelineId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("source change")})
	message := kafka.Message{Key: []byte(metadata.SourceId), Message: entityJson, Headers: headers}
	return message, requestId
}

// SourceChangeFlagParams holds optional parameters for NewSourceChangeFlagWithOptions
type SourceChangeFlagParams struct {
	Config                 map[string]string
	AdvancedConfig         change_flag.FlagSourceAdvancedConfig
	SchemaLessEnabled      bool
	SchemaLessConfig       *change_flag.SchemaLessConfig
	DeviceInventoryEnabled bool
	DeviceInventoryConfig  *change_flag.DeviceInventoryConfig
}

// SourceChangeFlagOption is a functional option for configuring SourceChangeFlagParams
type SourceChangeFlagOption func(*SourceChangeFlagParams)

// WithConfig sets the config map
func WithConfig(config map[string]string) SourceChangeFlagOption {
	return func(p *SourceChangeFlagParams) {
		p.Config = config
	}
}

// WithAdvancedConfig sets the advanced config
func WithAdvancedConfig(advancedConfig change_flag.FlagSourceAdvancedConfig) SourceChangeFlagOption {
	return func(p *SourceChangeFlagParams) {
		p.AdvancedConfig = advancedConfig
	}
}

// WithSchemaLess sets the schemaless configuration
func WithSchemaLess(enabled bool, config *change_flag.SchemaLessConfig) SourceChangeFlagOption {
	return func(p *SourceChangeFlagParams) {
		p.SchemaLessEnabled = enabled
		p.SchemaLessConfig = config
	}
}

// WithDeviceInventory sets the device inventory enabled flag and config
func WithDeviceInventory(enabled bool, config *change_flag.DeviceInventoryConfig) SourceChangeFlagOption {
	return func(p *SourceChangeFlagParams) {
		p.DeviceInventoryEnabled = enabled
		p.DeviceInventoryConfig = config
	}
}

// NewSourceChangeFlagWithOptions creates a source change flag message using functional options pattern.
// This allows passing multiple optional arguments more effectively.
//
// Example usage:
//
//	msg, reqId := NewSourceChangeFlagWithOptions(t, metadata, "CREATE",
//	    WithConfig(map[string]string{"key": "value"}),
//	    WithAdvancedConfig(advConfig),
//	    WithSchemaLess(true, schemaLessConfig),
//	    WithDeviceInventory(true, &change_flag.DeviceInventoryConfig{
//	        SourceHostNameMappingFields: []string{"hostname", "device_name"},
//	    }),
//	)
func NewSourceChangeFlagWithOptions(t TestLogger, metadata *KafkaMetaData, action string, opts ...SourceChangeFlagOption) (kafka.Message, string) {
	// Apply default values
	params := &SourceChangeFlagParams{
		Config:                 make(map[string]string),
		AdvancedConfig:         change_flag.FlagSourceAdvancedConfig{},
		SchemaLessEnabled:      false,
		SchemaLessConfig:       nil,
		DeviceInventoryEnabled: false,
		DeviceInventoryConfig:  nil,
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	source := change_flag.FlagSource{
		Id:                     metadata.SourceId,
		TenantId:               metadata.TenantId,
		Name:                   metadata.SourceName,
		Scope:                  metadata.Scope,
		Type:                   metadata.SourceType,
		Device:                 metadata.DeviceType,
		Vendor:                 metadata.DeviceVendor,
		Version:                metadata.SourceVersion,
		Status:                 metadata.SourceStatus,
		Config:                 params.Config,
		SecretId:               metadata.SourceSecretId,
		PullMechanism:          metadata.PullMechanism,
		AdvancedConfig:         params.AdvancedConfig,
		SchemaLessConfig:       params.SchemaLessConfig,
		SchemaLessEnabled:      params.SchemaLessEnabled,
		DeviceInventoryEnabled: params.DeviceInventoryEnabled,
		DeviceInventoryConfig:  params.DeviceInventoryConfig,
	}

	entity := map[string]interface{}{"entity": source}
	entityJson, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Failed to marshal entity: %s", err)
	}

	requestId := uuid.New().String()
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "tenant_id", Value: []byte(metadata.TenantId)})
	headers = append(headers, kafka.Header{Key: "schema_version", Value: []byte("V1")})
	headers = append(headers, kafka.Header{Key: "request_id", Value: []byte(requestId)})
	headers = append(headers, kafka.Header{Key: "action", Value: []byte(action)})
	headers = append(headers, kafka.Header{Key: "entity_type", Value: []byte("source")})
	headers = append(headers, kafka.Header{Key: "entity_id", Value: []byte(metadata.PipelineId)})
	headers = append(headers, kafka.Header{Key: "entity_name", Value: []byte("source change")})
	message := kafka.Message{Key: []byte(metadata.SourceId), Message: entityJson, Headers: headers}

	return message, requestId
}
