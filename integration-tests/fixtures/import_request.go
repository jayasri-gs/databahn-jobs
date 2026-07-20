package fixtures

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ImportTypeDeviceTimezoneMapping = "DEVICE_TIMEZONE_MAPPING"
	ImportStatusRequested           = "REQUESTED"
	ImportStatusProcessing          = "PROCESSING"
	ImportFileStorageArtifacts      = "artifacts"
	ImportFileTypeCSV               = "CSV"
)

// ImportRequestFixture holds tenant and actor IDs for import processor tests.
type ImportRequestFixture struct {
	TenantID    uuid.UUID
	ActorID     uuid.UUID
	SourceID    uuid.UUID
	DataPlaneID uuid.UUID
}

// ImportRequestSeed is a persisted import_request row for integration tests.
type ImportRequestSeed struct {
	ID       uuid.UUID
	Name     string
	FilePath string
	FileName string
}

// SeedImportRequestFixture creates an isolated tenant and log source for import tests.
func SeedImportRequestFixture(ctx context.Context, db *gorm.DB) (*ImportRequestFixture, error) {
	tenantID := uuid.New()
	sourceID := uuid.New()
	dataPlaneID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()

	tenant := map[string]any{
		"id":     tenantID,
		"name":   fmt.Sprintf("import-integration-%s", tenantID.String()[:8]),
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert import tenant: %w", err)
	}

	source := map[string]any{
		"id":            sourceID,
		"name":          "import-integration-source",
		"tenant_id":     tenantID,
		"data_plane_id": dataPlaneID,
		"status":        "ACTIVE",
		"created_by":    actorID,
		"updated_by":    actorID,
		"customer_id":   tenantID,
		"created_at":    now,
		"updated_at":    now,
	}
	if err := db.WithContext(ctx).Table("log_source").Create(source).Error; err != nil {
		return nil, fmt.Errorf("insert import log source: %w", err)
	}

	return &ImportRequestFixture{
		TenantID:    tenantID,
		ActorID:     actorID,
		SourceID:    sourceID,
		DataPlaneID: dataPlaneID,
	}, nil
}

// SeedDeviceTimezoneImportRequest inserts an import_request row with the given status.
func SeedDeviceTimezoneImportRequest(
	ctx context.Context,
	db *gorm.DB,
	fixture *ImportRequestFixture,
	id uuid.UUID,
	name string,
	filePath string,
	fileName string,
	hasHeaders bool,
	status string,
) (*ImportRequestSeed, error) {
	now := time.Now().UTC()
	startedAt := (*time.Time)(nil)
	if status == ImportStatusProcessing {
		started := now.Add(-1 * time.Hour)
		startedAt = &started
	}

	row := map[string]any{
		"id":           id,
		"tenant_id":    fixture.TenantID,
		"created_by":   fixture.ActorID,
		"updated_by":   fixture.ActorID,
		"created_at":   now,
		"updated_at":   now,
		"name":         name,
		"import_type":  ImportTypeDeviceTimezoneMapping,
		"status":       status,
		"file_name":    fileName,
		"file_path":    filePath,
		"file_storage": ImportFileStorageArtifacts,
		"file_type":    ImportFileTypeCSV,
		"has_headers":  hasHeaders,
		"retries":      0,
	}
	if startedAt != nil {
		row["started_at"] = *startedAt
	}
	if err := db.WithContext(ctx).Table("import_request").Create(row).Error; err != nil {
		return nil, fmt.Errorf("insert import_request: %w", err)
	}

	return &ImportRequestSeed{
		ID:       id,
		Name:     name,
		FilePath: filePath,
		FileName: fileName,
	}, nil
}

// BuildImportFilePath mirrors backend import artifact key layout.
func BuildImportFilePath(tenantID, importRequestID uuid.UUID, fileName string) string {
	return fmt.Sprintf(
		"TenantID=%s/ImportRequestID=%s/%s/%s",
		tenantID,
		importRequestID,
		uuid.New(),
		fileName,
	)
}
