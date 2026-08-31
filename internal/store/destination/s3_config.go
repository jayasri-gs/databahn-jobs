package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/store/dataplane"
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
	if c == nil || c.Bucket == "" {
		return ""
	}
	return fmt.Sprintf("s3://%s/.databahn_out", c.Bucket)
}

// S3ConfigFromExternalConnector builds Athena staging credentials from an
// external-store connectorConfig. Prefers output_bucket over bucket for Athena
// results, matching backend ATHENA_OUTPUT_BUCKET.
func S3ConfigFromExternalConnector(connector map[string]string) *S3Config {
	if len(connector) == 0 {
		return nil
	}
	bucket := strings.TrimSpace(connector["output_bucket"])
	if bucket == "" {
		bucket = strings.TrimSpace(connector["bucket"])
	}
	return &S3Config{
		AuthType:        strings.TrimSpace(connector["auth_type"]),
		AccessKeyID:     connector["access_key_id"],
		SecretAccessKey: connector["secret_access_key"],
		RoleArn:         strings.TrimSpace(connector["role_arn"]),
		ExternalID:      strings.TrimSpace(connector["external_id"]),
		Region:          strings.TrimSpace(connector["region"]),
		Bucket:          bucket,
	}
}

// ApplyS3CredentialOverrides overlays AWS Secrets Manager fields onto an S3Config.
//
// Blank values are ignored rather than assigned: a partial or malformed secret that carries
// a recognised key with an empty value would otherwise erase working inline credentials and
// fail the export at authentication. This matches how resolveExternalAthenaS3 merges the
// same four fields.
func ApplyS3CredentialOverrides(cfg *S3Config, credentialOverrides map[string]string) {
	if cfg == nil {
		return
	}
	for k, v := range credentialOverrides {
		if v == "" {
			continue
		}
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

	ApplyS3CredentialOverrides(cfg, credentialOverrides)

	if cfg.Bucket == "" {
		return nil, fmt.Errorf("destination bucket is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("destination region is required")
	}

	return cfg, nil
}

// parseDatabahnStorageStagingConfig builds an S3Config from a DATABAHN_STORAGE destination's
// configuration map. Region is read only from s3Region; bucket from s3BucketName. Platform
// default credentials (IAM role) are used, so no stored credential fields are set.
func parseDatabahnStorageStagingConfig(cfgMap map[string]string) (*S3Config, error) {
	region := strings.TrimSpace(cfgMap["s3Region"])
	bucket := strings.TrimSpace(cfgMap["s3BucketName"])
	if region == "" {
		return nil, fmt.Errorf("s3Region not configured for DATABAHN_STORAGE destination")
	}
	if bucket == "" {
		return nil, fmt.Errorf("s3BucketName not configured for DATABAHN_STORAGE destination")
	}
	return &S3Config{Region: region, Bucket: bucket}, nil
}

func resolveDatabahnStorageRegion(destS3Region, dataplaneRegion string) (string, error) {
	if region := strings.TrimSpace(destS3Region); region != "" {
		return region, nil
	}
	if region := strings.TrimSpace(dataplaneRegion); region != "" {
		return region, nil
	}
	return "", errors.New("Databahn Storage is not enabled for this data plane. Please contact your administrator.")
}

func databahnStorageRegionFromDataPlane(dp *dataplane.DataPlane) (string, error) {
	if dp == nil {
		return "", errors.New("data plane not found")
	}
	cfg, err := dp.ParseBackupConfiguration()
	if err != nil {
		return "", fmt.Errorf("failed to parse data plane backup configuration: %w", err)
	}
	if cfg == nil {
		return "", nil
	}
	return cfg.DatabahnStorageConfiguration.Region, nil
}

func databahnStorageRegionFromJoinRow(dataPlaneID, joinedDataPlaneID *uuid.UUID, backupJSON []byte) (string, error) {
	if dataPlaneID == nil || *dataPlaneID == uuid.Nil {
		return "", errors.New("destination has no data plane")
	}
	if joinedDataPlaneID == nil || *joinedDataPlaneID == uuid.Nil {
		return "", fmt.Errorf("data plane not found with id: %s", dataPlaneID)
	}
	dp := &dataplane.DataPlane{BackupConfiguration: backupJSON}
	return databahnStorageRegionFromDataPlane(dp)
}

func loadDatabahnStorageRegionFromDataPlane(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (string, error) {
	var row struct {
		DataPlaneID         *uuid.UUID `gorm:"column:data_plane_id"`
		JoinedDataPlaneID   *uuid.UUID `gorm:"column:joined_data_plane_id"`
		BackupConfiguration []byte     `gorm:"column:backup_configuration"`
	}
	err := db.WithContext(ctx).Raw(
		`SELECT d.data_plane_id, dp.id AS joined_data_plane_id, dp.backup_configuration
		 FROM destination d
		 LEFT JOIN data_planes dp ON dp.id = d.data_plane_id
		 WHERE d.id = ? AND d.tenant_id = ?
		 LIMIT 1`,
		destID, tenantID,
	).Scan(&row).Error
	if err != nil {
		return "", fmt.Errorf("failed to load destination data plane: %w", err)
	}
	return databahnStorageRegionFromJoinRow(row.DataPlaneID, row.JoinedDataPlaneID, row.BackupConfiguration)
}

// LoadDatabahnStorageStagingConfig loads the Athena staging S3 config from a DATABAHN_STORAGE
// destination. It uses platform default credentials (no stored keys). Region comes from
// destination.configuration.s3Region when set; otherwise from the destination dataplane's
// backup_configuration.databahnStorageConfiguration.region. Bucket comes from s3BucketName.
func LoadDatabahnStorageStagingConfig(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (*S3Config, error) {
	cfgMap, err := LoadMergedConfiguration(ctx, db, destID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to load DATABAHN_STORAGE destination config: %w", err)
	}

	dataplaneRegion := ""
	if strings.TrimSpace(cfgMap["s3Region"]) == "" {
		dataplaneRegion, err = loadDatabahnStorageRegionFromDataPlane(ctx, db, destID, tenantID)
		if err != nil {
			return nil, err
		}
	}

	region, err := resolveDatabahnStorageRegion(cfgMap["s3Region"], dataplaneRegion)
	if err != nil {
		return nil, err
	}
	cfgMap["s3Region"] = region
	return parseDatabahnStorageStagingConfig(cfgMap)
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
