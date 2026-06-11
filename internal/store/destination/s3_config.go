package destination

import (
	"context"
	"encoding/json"
	"fmt"

	dbaws "github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type S3Config struct {
	AuthType        string
	AccessKeyID     string
	SecretAccessKey string
	RoleArn         string
	ExternalID      string
	Region          string
	Bucket          string
}

// AthenaOutputLocation matches backend AthenaService.athenaQueryOutputLocation.
func (c *S3Config) AthenaOutputLocation() string {
	return fmt.Sprintf("s3://%s/.databahn_out", c.Bucket)
}

type s3ConfigRow struct {
	ID            uuid.UUID `gorm:"column:id"`
	TenantID      uuid.UUID `gorm:"column:tenant_id"`
	Configuration string    `gorm:"column:configuration"`
}

type configWrapper struct {
	SecretID      string                 `json:"secretId"`
	Configuration map[string]interface{} `json:"configuration"`
}

func (w *configWrapper) getString(key string) string {
	if v, ok := w.Configuration[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func parseS3ConfigFromWrapper(wrapper configWrapper, secret map[string]string) (*S3Config, error) {
	cfg := &S3Config{
		AuthType:        wrapper.getString("auth_type"),
		AccessKeyID:     wrapper.getString("access_key_id"),
		SecretAccessKey: wrapper.getString("secret_access_key"),
		RoleArn:         wrapper.getString("role_arn"),
		ExternalID:      wrapper.getString("external_id"),
		Region:          wrapper.getString("region"),
		Bucket:          wrapper.getString("bucket"),
	}

	for k, v := range secret {
		switch k {
		case "access_key_id":
			cfg.AccessKeyID = v
		case "secret_access_key":
			cfg.SecretAccessKey = v
		case "role_arn":
			cfg.RoleArn = v
		case "external_id":
			cfg.ExternalID = v
		}
	}

	if cfg.Bucket == "" {
		return nil, fmt.Errorf("destination bucket is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("destination region is required")
	}

	return cfg, nil
}

func LoadS3Config(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (*S3Config, error) {
	var dest s3ConfigRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, tenant_id, configuration FROM destination WHERE id = ? AND tenant_id = ? LIMIT 1",
		destID, tenantID,
	).Scan(&dest).Error
	if err != nil {
		return nil, fmt.Errorf("destination not found: %w", err)
	}

	if dest.Configuration == "" {
		return nil, fmt.Errorf("destination configuration is empty for %s", destID)
	}

	var wrapper configWrapper
	if err := json.Unmarshal([]byte(dest.Configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	secret := make(map[string]string)
	if wrapper.SecretID != "" {
		var backendSecretID string
		err := db.WithContext(ctx).Raw(
			"SELECT backend_secret_id FROM secrets WHERE id = ? LIMIT 1",
			wrapper.SecretID,
		).Scan(&backendSecretID).Error
		if err != nil || backendSecretID == "" {
			return nil, fmt.Errorf("failed to look up backend_secret_id: secret reference not found or inaccessible (destination=%s, tenant=%s)", destID, dest.TenantID)
		}

		secretOutput, err := dbaws.ReadSecretByName(backendSecretID, appConfig.GetAppConfiguration().GetString("region"))
		if err != nil {
			return nil, fmt.Errorf("failed to read secret from AWS Secrets Manager (destination=%s, tenant=%s)", destID, dest.TenantID)
		}

		if secretOutput.SecretString != nil {
			var secretMap map[string]string
			if err := json.Unmarshal([]byte(*secretOutput.SecretString), &secretMap); err != nil {
				return nil, fmt.Errorf("failed to parse secret value: malformed JSON (destination=%s, tenant=%s)", destID, dest.TenantID)
			}
			secret = secretMap
		}
	}

	return parseS3ConfigFromWrapper(wrapper, secret)
}
