package dataplane

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DataPlane represents a data plane in the database
type DataPlane struct {
	ID                  uuid.UUID       `gorm:"type:uuid;primary_key" json:"id"`
	CreatedAt           time.Time       `gorm:"type:timestamp" json:"created_at"`
	CPGatewayURL        string          `gorm:"type:varchar(255);column:cp_gateway_url" json:"cp_gateway_url"`
	DPGatewayURL        string          `gorm:"type:varchar(255);column:dp_gateway_url" json:"dp_gateway_url"`
	TenantID            *uuid.UUID      `gorm:"type:uuid;column:tenant_id" json:"tenant_id"`
	UpdatedAt           time.Time       `gorm:"type:timestamp" json:"updated_at"`
	DPControllerURL     string          `gorm:"type:varchar(255);column:dp_controller_url" json:"dp_controller_url"`
	HeartbeatAt         *time.Time      `gorm:"type:timestamp with time zone;column:heartbeat_at" json:"heartbeat_at"`
	Owner               string          `gorm:"type:varchar(255)" json:"owner"`
	Platform            string          `gorm:"type:varchar(255)" json:"platform"`
	Status              *bool           `gorm:"column:status" json:"status"`
	CustomerID          *uuid.UUID      `gorm:"type:uuid;column:customer_id" json:"customer_id"`
	Name                string          `gorm:"type:varchar(100)" json:"name"`
	WebhookURL          string          `gorm:"type:text;column:webhook_url" json:"webhook_url"`
	HTTPCollectorURL    string          `gorm:"type:text;column:http_collector_url" json:"http_collector_url"`
	BackupConfiguration json.RawMessage `gorm:"type:json;column:backup_configuration" json:"backup_configuration"`
	OtelHTTPURL         string          `gorm:"type:text;column:otel_http_url" json:"otel_http_url"`
	OtelGRPCURL         string          `gorm:"type:text;column:otel_grpc_url" json:"otel_grpc_url"`
	BaseURL             string          `gorm:"type:text;column:base_url" json:"base_url"`
}

func (*DataPlane) TableName() string {
	return "data_planes"
}

// BackupConfiguration represents the backup_configuration JSON structure
type BackupConfiguration struct {
	UnparsedConfiguration        UnparsedConfig               `json:"unparsedConfiguration"`
	SandboxConfiguration         SandboxConfig                `json:"sandboxConfiguration"`
	DatabahnStorageConfiguration DatabahnStorageConfiguration `json:"databahnStorageConfiguration"`
}

type DatabahnStorageConfiguration struct {
	Region string `json:"region"`
}

// UnparsedConfig represents unparsed data configuration
type UnparsedConfig struct {
	AWSConfiguration                  AWSConfig    `json:"awsConfiguration"`
	CustomUnparsedAthenaConfiguration AthenaConfig `json:"customUnparsedAthenaConfiguration"`
}

// SandboxConfig represents sandbox storage configuration.
// Backend determines the storage type: empty/"athena" for S3, "synapse" for Azure Blob.
type SandboxConfig struct {
	Backend                     string        `json:"backend"`
	AWSConfiguration            AWSConfig     `json:"awsConfiguration"`
	AzureConfiguration          AzureConfig   `json:"azureConfiguration"`
	SandboxAthenaConfiguration  AthenaConfig  `json:"sandboxAthenaConfiguration"`
	SandboxSynapseConfiguration SynapseConfig `json:"sandboxSynapseConfiguration"`
	Enabled                     bool          `json:"enabled"`
}

// AzureConfig represents Azure Blob Storage configuration
type AzureConfig struct {
	Container   string `json:"container"`
	AccountName string `json:"accountName"`
}

// SynapseConfig represents Azure Synapse configuration
type SynapseConfig struct {
	Workspace       string `json:"workspace"`
	SynapseDatabase string `json:"synapseDatabase"`
	SynapseTable    string `json:"synapseTable"`
}

// IsBlobBackend returns true if this sandbox config uses Azure Blob storage.
func (sc *SandboxConfig) IsBlobBackend() bool {
	return sc.Backend == "synapse"
}

// CollectionName returns the storage collection name (S3 bucket or Azure Blob container).
func (sc *SandboxConfig) CollectionName() string {
	if sc.IsBlobBackend() {
		return sc.AzureConfiguration.Container
	}
	return sc.AWSConfiguration.Bucket
}

// AWSConfig represents AWS S3 configuration
type AWSConfig struct {
	Bucket string `json:"bucket"`
	Region string `json:"region"`
}

// AthenaConfig represents Athena configuration
type AthenaConfig struct {
	AthenaTable    string `json:"athenaTable"`
	AthenaDatabase string `json:"athenaDatabase"`
}

// GetAllDataPlanes retrieves all data planes from the database
func GetAllDataPlanes(ctx context.Context, db *gorm.DB) ([]DataPlane, error) {
	var dataPlanes []DataPlane
	err := db.WithContext(ctx).Find(&dataPlanes).Error
	return dataPlanes, err
}

// ParseBackupConfiguration parses the backup_configuration JSON field
func (dp *DataPlane) ParseBackupConfiguration() (*BackupConfiguration, error) {
	if len(dp.BackupConfiguration) == 0 {
		return nil, nil
	}

	var config BackupConfiguration
	err := json.Unmarshal(dp.BackupConfiguration, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
