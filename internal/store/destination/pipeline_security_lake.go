package destination

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DestTypeAWSSecurityLake = "AWS_SECURITY_LAKE"

	pipelineAWSRegion            = "aws_region"
	pipelineSecurityLakeRoleARN  = "aws_security_lake_role_arn"
	pipelineAWSExternalID        = "aws_external_id"
	pipelineCustomSourceLocation = "aws_security_data_lake_custom_source_location"
	pipelineAWSAccountID         = "aws_account_id"
	pipelineOCSFID               = "ocsf_id"
	connectorAuthType            = "auth_type"
	connectorAuthTypeRoleARN     = "role_based"
	connectorRegion              = "region"
	connectorRoleARN             = "role_arn"
	connectorExternalID          = "external_id"
	connectorBucket              = "bucket"
	connectorOutputBucket        = "output_bucket"
	connectorGlueDatabase        = "glue_database"
	connectorSecurityLake        = "security_lake"
)

var pipelineS3URIPrefix = regexp.MustCompile(`(?i)^s3://([^/]+)(?:/(.*))?$`)

// LoadPipelineSecurityLakeS3Config resolves Athena staging credentials for a pipeline
// AWS_SECURITY_LAKE destination (DATABAHN_DESTINATION search stores).
func LoadPipelineSecurityLakeS3Config(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (*S3Config, error) {
	merged, err := LoadMergedConfiguration(ctx, db, destID, tenantID)
	if err != nil {
		return nil, err
	}
	connector, err := pipelineSecurityLakeConnector(merged)
	if err != nil {
		return nil, err
	}
	staging := S3ConfigFromExternalConnector(connector)
	if staging == nil {
		return nil, fmt.Errorf("pipeline Security Lake destination %s produced empty Athena config", destID)
	}
	if staging.Region == "" {
		return nil, fmt.Errorf("pipeline Security Lake destination %s missing region", destID)
	}
	if staging.Bucket == "" {
		return nil, fmt.Errorf("pipeline Security Lake destination %s missing output_bucket", destID)
	}
	if staging.RoleArn == "" {
		return nil, fmt.Errorf("pipeline Security Lake destination %s missing role ARN", destID)
	}
	if staging.AuthType == "" {
		staging.AuthType = connectorAuthTypeRoleARN
	}
	return staging, nil
}

// pipelineSecurityLakeConnector maps pipeline destination keys to the external-store
// connector shape used by S3ConfigFromExternalConnector (mirrors backend-service
// PipelineSecurityLakeDestinationSupport.toAthenaConnectorConfig).
func pipelineSecurityLakeConnector(config map[string]string) (map[string]string, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("Security Lake pipeline destination configuration is empty")
	}
	region, err := requirePipelineConfig(config, pipelineAWSRegion, "aws_region")
	if err != nil {
		return nil, err
	}
	roleARN, err := requirePipelineConfig(config, pipelineSecurityLakeRoleARN, "role ARN")
	if err != nil {
		return nil, err
	}
	externalID, err := requirePipelineConfig(config, pipelineAWSExternalID, "external id")
	if err != nil {
		return nil, err
	}
	customSource, err := requirePipelineConfig(config, pipelineCustomSourceLocation, "custom source location")
	if err != nil {
		return nil, err
	}
	if _, err := requirePipelineConfig(config, pipelineAWSAccountID, "account id"); err != nil {
		return nil, err
	}
	if _, err := requirePipelineConfig(config, pipelineOCSFID, "ocsf_id"); err != nil {
		return nil, err
	}

	connector := map[string]string{
		connectorAuthType:     connectorAuthTypeRoleARN,
		connectorRegion:       strings.TrimSpace(region),
		connectorRoleARN:      strings.TrimSpace(roleARN),
		connectorExternalID:   strings.TrimSpace(externalID),
		connectorSecurityLake: "true",
		connectorBucket:       resolvePipelineSecurityLakeBucket(config, region, customSource),
	}
	if glueDB := strings.TrimSpace(config[connectorGlueDatabase]); glueDB != "" {
		connector[connectorGlueDatabase] = glueDB
	} else {
		connector[connectorGlueDatabase] = defaultPipelineGlueDatabase(region)
	}
	if outputBucket := strings.TrimSpace(config[connectorOutputBucket]); outputBucket != "" {
		connector[connectorOutputBucket] = outputBucket
	}
	return connector, nil
}

func requirePipelineConfig(config map[string]string, key, label string) (string, error) {
	value := strings.TrimSpace(config[key])
	if value == "" {
		return "", fmt.Errorf("%s is required for Security Lake pipeline destination", label)
	}
	return value, nil
}

func resolvePipelineSecurityLakeBucket(config map[string]string, region, customSourceLocation string) string {
	if bucket := strings.TrimSpace(config[connectorBucket]); bucket != "" {
		return bucket
	}
	if matches := pipelineS3URIPrefix.FindStringSubmatch(strings.TrimSpace(customSourceLocation)); len(matches) > 1 {
		return matches[1]
	}
	return "aws-security-data-lake-" + strings.TrimSpace(region)
}

func defaultPipelineGlueDatabase(region string) string {
	return "amazon_security_lake_glue_db_" + strings.ReplaceAll(strings.TrimSpace(region), "-", "_")
}
