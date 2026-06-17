package destination

import (
	"context"
	"encoding/json"
	"fmt"

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

func parseS3ConfigFromWrapper(wrapper ConfigWrapper, credentialOverrides map[string]string) (*S3Config, error) {
	cfg := &S3Config{
		AuthType:        wrapper.getString("auth_type"),
		AccessKeyID:     wrapper.getString("access_key_id"),
		SecretAccessKey: wrapper.getString("secret_access_key"),
		RoleArn:         wrapper.getString("role_arn"),
		ExternalID:      wrapper.getString("external_id"),
		Region:          wrapper.getString("region"),
		Bucket:          wrapper.getString("bucket"),
	}

	for k, v := range credentialOverrides {
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

	var wrapper ConfigWrapper
	if err := json.Unmarshal([]byte(dest.Configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	var credentialOverrides map[string]string
	if wrapper.SecretID != "" {
		var err error
		credentialOverrides, err = ResolveCredentialOverrides(ctx, db, wrapper.SecretID, destID, dest.TenantID)
		if err != nil {
			return nil, err
		}
	}

	return parseS3ConfigFromWrapper(wrapper, credentialOverrides)
}
