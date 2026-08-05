package datastore

import (
	"context"
	"encoding/json"
	"fmt"

	dbaws "github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

type insightsAwsSecret struct {
	AccessKeyID     string `json:"search.access_key_id"`
	SecretAccessKey string `json:"search.secret_access_key"`
	Bucket          string `json:"search.bucket"`
}

// parseInsightsStagingS3Config builds an Athena staging S3Config for DATABAHN_INSIGHTS from the
// platform search secret. Uses the parquet bucket (search.bucket + "-parquet") consistent with
// how databahn-jobs writes Insights parquet data via UploadFileToObjectStore/UploadFileToS3Parquet.
func parseInsightsStagingS3Config(secret insightsAwsSecret, region string) (*destination.S3Config, error) {
	if region == "" {
		return nil, fmt.Errorf("region not configured for DATABAHN_INSIGHTS staging")
	}
	if secret.Bucket == "" {
		return nil, fmt.Errorf("search.bucket not configured in insights AWS secret")
	}
	if secret.AccessKeyID == "" {
		return nil, fmt.Errorf("search.access_key_id not configured in insights AWS secret")
	}
	if secret.SecretAccessKey == "" {
		return nil, fmt.Errorf("search.secret_access_key not configured in insights AWS secret")
	}
	return &destination.S3Config{
		AuthType:        "key_based",
		AccessKeyID:     secret.AccessKeyID,
		SecretAccessKey: secret.SecretAccessKey,
		Region:          region,
		Bucket:          secret.Bucket + "-parquet",
	}, nil
}

// loadInsightsStagingS3Config loads the Athena staging S3 config for DATABAHN_INSIGHTS from the
// platform search secret (search.secret_name app config key + region). Mirrors how
// util.databahns3.go loads the same secret for other platform operations.
func loadInsightsStagingS3Config(ctx context.Context) (*destination.S3Config, error) {
	cfg := appConfig.GetAppConfiguration()
	secretName := cfg.GetString("search.secret_name")
	region := cfg.GetString("region")
	if secretName == "" {
		return nil, fmt.Errorf("search.secret_name not configured")
	}

	data, err := dbaws.ReadSecretByName(secretName, region)
	if err != nil {
		return nil, fmt.Errorf("failed to read insights AWS secret: %w", err)
	}
	if data.SecretString == nil {
		return nil, fmt.Errorf("insights AWS secret is empty")
	}

	var secret insightsAwsSecret
	if err := json.Unmarshal([]byte(*data.SecretString), &secret); err != nil {
		return nil, fmt.Errorf("failed to parse insights AWS secret: %w", err)
	}

	return parseInsightsStagingS3Config(secret, region)
}
