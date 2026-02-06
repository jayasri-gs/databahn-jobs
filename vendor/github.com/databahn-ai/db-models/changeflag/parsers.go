package changeflag

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
)

func ParseCustomNormalizationFlag(data []byte) (*FlagCustomNormalization, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var customNorm FlagCustomNormalization
	err = decode(flag.Entity, &customNorm)
	if err != nil {
		return nil, err
	}
	err = validateCustomNorm(customNorm)
	if err != nil {
		return nil, err
	}

	return &customNorm, nil
}

func ParseFlagDataReplay(data []byte) (*FlagDataReplay, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var dataReplay FlagDataReplay
	err = decode(flag.Entity, &dataReplay)
	if err != nil {
		return nil, err
	}
	err = validateFlagDataReplay(dataReplay)
	if err != nil {
		return nil, err
	}
	return &dataReplay, nil
}

func ParseFlagDestination(data []byte) (*FlagDestination, error) {
	flagDestination, err := parseDestination(data)
	if err != nil {
		return nil, err
	}
	err = validateDestination(flagDestination)
	if err != nil {
		return nil, err
	}
	return &flagDestination, nil
}

func parseDestination(data []byte) (FlagDestination, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return FlagDestination{}, err
	}

	var flagDestination FlagDestination
	err = decode(flag.Entity, &flagDestination)
	if err != nil {
		return FlagDestination{}, err
	}
	return flagDestination, nil
}

func parseRouteProcessor(data []byte) (FlagRouteProcessor, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return FlagRouteProcessor{}, err
	}

	var rp FlagRouteProcessor
	err = decode(flag.Entity, &rp)
	if err != nil {
		return FlagRouteProcessor{}, err
	}
	return rp, nil
}

func ParseFlagSecondaryGlobalDestination(data []byte) (*FlagSecondaryGlobalDestination, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var flagSecondaryGlobalDestination FlagSecondaryGlobalDestination
	err = decode(flag.Entity, &flagSecondaryGlobalDestination)
	if err != nil {
		return nil, err
	}
	err = validateSecondaryGlobalDestination(flagSecondaryGlobalDestination)
	if err != nil {
		return nil, err
	}

	return &flagSecondaryGlobalDestination, nil
}

func ParseFlagEnrichment(data []byte) (*FlagEnrichment, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var enrichment FlagEnrichment
	err = decode(flag.Entity, &enrichment)
	if err != nil {
		return nil, err
	}
	err = validateEnrichment(enrichment)
	if err != nil {
		return nil, err
	}
	return &enrichment, nil
}

func ParseFlagLookup(data []byte) (*FlagLookup, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}
	var flagLookup FlagLookup
	err = decode(flag.Entity, &flagLookup)
	if err != nil {
		return nil, err
	}
	err = validateLookup(flagLookup)
	if err != nil {
		return nil, err
	}
	return &flagLookup, nil
}

func ParseFlagPipeline(data []byte) (*FlagPipeline, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var pipeline FlagPipeline
	err = decode(flag.Entity, &pipeline)
	if err != nil {
		return nil, err
	}
	err = validatePipeline(pipeline)
	if err != nil {
		return nil, err
	}
	return &pipeline, nil
}

func ParseFlagRouteProcessor(data []byte) (*FlagRouteProcessor, error) {
	rp, err := parseRouteProcessor(data)
	if err != nil {
		return nil, err
	}
	err = validateRouteProcessorFlag(rp)
	if err != nil {
		return nil, err
	}
	return &rp, nil
}

func ParseFlagRule(data []byte) (*FlagRule, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}
	var flagRule FlagRule
	err = decode(flag.Entity, &flagRule)
	if err != nil {
		return nil, err
	}
	err = validateRule(flagRule)
	if err != nil {
		return nil, err
	}
	return &flagRule, nil
}

func ParseFlagRuleInsights(data []byte) (*FlagInsightRule, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var ruleInsight FlagInsightRule
	err = decode(flag.Entity, &ruleInsight)
	if err != nil {
		return nil, err
	}
	err = validateRuleInsight(ruleInsight)
	if err != nil {
		return nil, err
	}
	return &ruleInsight, nil
}

func ParseFlagSensitiveData(data []byte) (*FlagSensitiveDataConfig, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var sensitiveData FlagSensitiveDataConfig
	err = decode(flag.Entity, &sensitiveData)
	if err != nil {
		return nil, err
	}
	err = validateSensitiveData(sensitiveData)
	return &sensitiveData, err
}

func ParseFlagSource(data []byte) (*FlagSource, error) {
	source, err := parseSource(data)
	if err != nil {
		return nil, err
	}
	err = validateSource(source)
	if err != nil {
		return nil, err
	}
	return &source, nil
}

func parseSource(data []byte) (FlagSource, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return FlagSource{}, err
	}

	var source FlagSource
	err = decode(flag.Entity, &source)
	if err != nil {
		return FlagSource{}, err
	}
	return source, err
}

func ParseFlagTransform(data []byte) (*FlagTransform, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var transform FlagTransform
	err = decode(flag.Entity, &transform)
	if err != nil {
		return nil, err
	}
	err = validateTransform(transform)
	if err != nil {
		return nil, err
	}

	return &transform, nil
}

func decode(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			stringToUUIDHookFunc(),
			toTimeHookFunc(),
		),
		TagName: "json",
		Result:  &output,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}

func stringToUUIDHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(uuid.UUID{}) {
			return data, nil
		}

		return uuid.Parse(data.(string))
	}
}

func toTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			return time.Parse(time.RFC3339, data.(string))
		case reflect.Float64:
			return time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			return time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
	}
}

func validateCustomNorm(customNorm FlagCustomNormalization) error {
	if customNorm.Id == "" {
		return errors.New("custom normalization is invalid, missing custom normalization id")
	}
	if customNorm.TenantId == "" {
		return errors.New("custom normalization is invalid, missing tenant id")
	}
	if customNorm.SourceId == "" {
		return errors.New("custom normalization is invalid, missing source id")
	}
	if customNorm.TransformLogic == "" {
		return errors.New("custom normalization is invalid, missing transform logic")
	}
	return nil
}

func validateDestination(destination FlagDestination) error {
	if destination.ID == "" {
		return errors.New("destination is invalid, missing destination id")
	}
	if destination.TenantUUID == "" {
		return errors.New("destination is invalid, missing tenant id")
	}
	if destination.DestinationType == "" {
		return errors.New("destination is invalid, missing destination type")
	}
	return nil
}

func validateSecondaryGlobalDestination(secondaryGlobalDestination FlagSecondaryGlobalDestination) error {
	if secondaryGlobalDestination.ID == "" {
		return errors.New("destination is invalid, missing destination id")
	}
	if secondaryGlobalDestination.TenantUUID == "" {
		return errors.New("destination is invalid, missing tenant id")
	}
	if secondaryGlobalDestination.DestinationType == "" {
		return errors.New("destination is invalid, missing destination type")
	}
	if !secondaryGlobalDestination.IsSecondaryGlobalDestination {
		return errors.New("destination is invalid,  IsSecondaryGlobalDestination is set to false")
	}
	return nil
}

func validateEnrichment(enrichment FlagEnrichment) error {
	if enrichment.Id == "" {
		return errors.New("enrichment is invalid, missing enrichment id")
	}
	if enrichment.TenantId == "" {
		return errors.New("enrichment is invalid, missing tenant id")
	}
	if enrichment.SourceId == "" {
		return errors.New("enrichment is invalid, missing source id")
	}
	if enrichment.DestinationId == "" {
		return errors.New("enrichment is invalid, missing destination id")
	}
	if enrichment.PipelineId == "" {
		return errors.New("enrichment is invalid, missing pipeline id")
	}
	if enrichment.CacheRequestId == "" {
		return errors.New("enrichment is invalid, missing cache request id")
	}
	if enrichment.LookupId == "" {
		return errors.New("enrichment is invalid, missing lookup id")
	}
	return nil
}

func validateFlagDataReplay(dataReplay FlagDataReplay) error {
	if dataReplay.Source == "" {
		return errors.New("dataReplay is invalid, missing source id")
	}
	if dataReplay.TenantId == "" {
		return errors.New("dataReplay is invalid, missing tenant id")
	}
	if dataReplay.DataStore == "" || dataReplay.DataStore == "s3" {
		if dataReplay.BucketName == "" {
			return errors.New("dataReplay is invalid, missing  BucketName")
		}
		if dataReplay.Region == "" {
			return errors.New("dataReplay is invalid, missing Region")
		}
		// Check authentication type
		var authType string
		if dataReplay.AdditionalConfig != nil {
			if authTypeVal, exists := dataReplay.AdditionalConfig["auth_type"]; exists {
				authType = authTypeVal
			}
		}

		if authType == "role_based" {
			// For role-based auth, validate role_arn instead
			var roleArn string
			if dataReplay.AdditionalConfig != nil {
				if roleArnVal, exists := dataReplay.AdditionalConfig["role_arn"]; exists {
					roleArn = roleArnVal
				}
			}
			if roleArn == "" {
				return errors.New("dataReplay is invalid, missing role_arn for role-based authentication")
			}
		} else {
			// For key-based auth (default), validate access keys
			if dataReplay.AccessKeyId == "" {
				return errors.New("dataReplay is invalid, missing AccessKeyId")
			}
			if dataReplay.SecretAccessKey == "" {
				return errors.New("dataReplay is invalid, missing  SecretAccessKey")
			}
		}
	}
	if dataReplay.FileName == "" {
		return errors.New("dataReplay is invalid, missing FileName")
	}
	return nil
}

func validateLookup(lookup FlagLookup) error {
	if lookup.Id == "" {
		return errors.New("lookup is invalid, missing lookup id")
	}
	if lookup.TenantId == "" {
		return errors.New("lookup is invalid, missing tenant id")
	}
	if lookup.RequestId == "" {
		return errors.New("lookup is invalid, missing lookup type")
	}
	return nil
}

func validatePipeline(pipeline FlagPipeline) error {
	if pipeline.PipelineId == "" {
		return errors.New("pipeline is invalid, missing pipeline id")
	}
	if pipeline.TenantId == "" {
		return errors.New("pipeline is invalid, missing tenant id")
	}
	if pipeline.SourceId == "" {
		return errors.New("pipeline is invalid, missing source id")
	}
	return nil
}

func validateRouteProcessorFlag(rp FlagRouteProcessor) error {
	if rp.PipelineId == "" {
		return errors.New("route processor is invalid, missing pipeline id")
	}
	if rp.TenantId == "" {
		return errors.New("route processor is invalid, missing tenant id")
	}
	if rp.SourceId == "" {
		return errors.New("route processor is invalid, missing source id")
	}

	if rp.IsVcRouteProcessor {
		if rp.UnmatchedAction == "" || (rp.UnmatchedAction != "PUBLISH" && rp.UnmatchedAction != "DROP") {
			return errors.New("route processor is invalid, invalid unmatched action : " + rp.UnmatchedAction)
		}
		if rp.ExplicitDropAction == "" || (rp.ExplicitDropAction != "PUBLISH" && rp.ExplicitDropAction != "DROP") {
			return errors.New("route processor is invalid, invalid drop action action : " + rp.UnmatchedAction)
		}
		if rp.PrimaryDestinationId == "" {
			return errors.New("route processor is invalid, missing primary destination id")
		}
		if rp.PrimaryDestinationType == "" {
			return errors.New("route processor is invalid, missing primary destination type")
		}
		if rp.ExplicitDropAction == "PUBLISH" && (rp.SecondaryDestinationId == "" || rp.SecondaryDestinationType == "") {
			return errors.New("route processor is invalid, missing secondary destination id or type for publish drop action")
		}
	}

	if rp.IsOverride {
		if rp.OverrideDestinationId == "" {
			return errors.New("route processor is invalid, missing override destination id")
		}
		if rp.OverrideDestinationType == "" {
			return errors.New("route processor is invalid, missing override destination type")
		}
	}
	return nil
}

func validateRule(rule FlagRule) error {
	if rule.Id == "" {
		return errors.New("rule is invalid, missing rule id")
	}
	if rule.PipelineId == "" {
		return errors.New("rule is invalid, missing pipeline id")
	}
	if rule.TenantId == "" {
		return errors.New("rule is invalid, missing tenant id")
	}
	if rule.Name == "" {
		return errors.New("rule is invalid, missing name")
	}
	if rule.Priority <= 0 {
		return errors.New("rule is invalid, wrong priority")
	}
	if rule.ActionType == "" {
		return errors.New("rule is invalid, missing action type")
	}
	if rule.Type == "" {
		return errors.New("rule is invalid, missing type")
	}
	if rule.DestinationId == "" {
		return errors.New("rule is invalid, missing destination id")
	}
	if rule.RuleFilterQuery == "" {
		return errors.New("rule is invalid, missing rule filter query")
	}
	if rule.OldPipelineId == "" {
		return errors.New("rule is invalid, missing old pipeline Id")
	}

	return nil
}

func validateRuleInsight(insight FlagInsightRule) error {
	if insight.Id == "" {
		return errors.New("rule insight is invalid, missing rule id")
	}
	if insight.RuleFilterQuery == "" {
		return errors.New("rule insight is invalid, missing filter query")
	}
	if insight.SourceId == "" {
		return errors.New("rule insight is invalid, missing source")
	}
	if insight.TenantId == "" {
		return errors.New("rule insight is invalid, missing tenant details")
	}
	if len(insight.Attributes) == 0 {
		return errors.New("rule insight is invalid, missing attributes")
	}
	if insight.DefineLookup && insight.LookupId == "" {
		return errors.New("rule insight is invalid, missing lookup id")
	}
	return nil
}

func validateSensitiveData(sensitiveData FlagSensitiveDataConfig) error {
	if sensitiveData.Id == "" {
		return errors.New("sensitive data is invalid, missing sensitive data id")
	}
	if sensitiveData.SourceId == "" {
		return errors.New("sensitive data is invalid, missing source id")
	}
	if sensitiveData.TenantId == "" {
		return errors.New("sensitive data is invalid, missing tenant id")
	}
	if sensitiveData.Function == "" {
		return errors.New("sensitive data is invalid, missing function")
	}
	if len(sensitiveData.Patterns) == 0 {
		return errors.New("sensitive data is invalid, missing patterns")
	}
	return nil
}

func validateSource(source FlagSource) error {
	if source.Id == "" {
		return errors.New("source is invalid, missing source id")
	}
	if source.TenantId == "" {
		return errors.New("source is invalid, missing tenant id")
	}
	if source.Device == "" {
		return errors.New("source is invalid, missing source device")
	}
	if source.Vendor == "" {
		return errors.New("source is invalid, missing source device")
	}

	return nil
}

func validateTransform(transform FlagTransform) error {
	if transform.ID == "" {
		return errors.New("transform is invalid, missing transform id")
	}
	if transform.DestinationId == "" {
		return errors.New("transform is invalid, missing destination id")
	}
	if transform.TenantId == "" {
		return errors.New("transform is invalid, missing tenant id")
	}
	if transform.DestinationType == "" {
		return errors.New("transform is invalid, missing destination type")
	}
	if transform.PipelineId == "" {
		return errors.New("transform is invalid, missing pipeline id")
	}
	if transform.TransformationType == "" {
		return errors.New("transform is invalid, missing transformation type")
	}
	if transform.TransformationOutput == "" {
		return errors.New("transform is invalid, missing transformation output")
	}
	return nil
}

func ParseFlagGlobalDestination(data []byte) (*FlagGlobalDestination, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var globalDestination FlagGlobalDestination
	err = decode(flag.Entity, &globalDestination)
	if err != nil {
		return nil, err
	}
	err = validateGlobalDestination(globalDestination)
	if err != nil {
		return nil, err
	}
	return &globalDestination, nil
}

func validateGlobalDestination(globaldestination FlagGlobalDestination) error {
	if len(globaldestination.Configuration.EventTypeMap) == 0 {
		return errors.New("globalDestination data is invalid, missing config data")
	}
	if globaldestination.DestinationId == "" {
		return errors.New("globalDestination data is invalid, missing destination id")
	}
	if globaldestination.DestinationType == "" {
		return errors.New("globalDestination data is invalid, invalid destination Type")
	}
	if globaldestination.TenantId == "" {
		return errors.New("globalDestination data is invalid, invalid tenantId Type")
	}
	return nil
}

func ParseFlagAlertConfig(data []byte) (*FlagAlertConfig, error) {
	var flag ChangeFlagBody
	err := json.Unmarshal(data, &flag)
	if err != nil {
		return nil, err
	}

	var alertConfig FlagAlertConfig
	err = decode(flag.Entity, &alertConfig)
	if err != nil {
		return nil, err
	}
	err = validateAlertConfig(alertConfig)
	if err != nil {
		return nil, err
	}
	return &alertConfig, nil
}

func validateAlertConfig(alertConfig FlagAlertConfig) error {
	if alertConfig.Id == "" {
		return errors.New("alert config is invalid, missing id")
	}
	if alertConfig.TenantId == "" {
		return errors.New("alert config is invalid, missing tenant id")
	}
	if alertConfig.AlertType == "" {
		return errors.New("alert config is invalid, missing alert type")
	}
	return nil
}
