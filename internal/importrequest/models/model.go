package models

import (
	"encoding/json"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/importrequest/consts"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ImportRequest struct {
	ID           uuid.UUID      `gorm:"column:id"`
	TenantID     uuid.UUID      `gorm:"column:tenant_id"`
	CreatedBy    uuid.UUID      `gorm:"column:created_by"`
	UpdatedBy    uuid.UUID      `gorm:"column:updated_by"`
	Name         string         `gorm:"column:name"`
	Description  string         `gorm:"column:description"`
	ImportType   string         `gorm:"column:import_type"`
	Status       string         `gorm:"column:status"`
	FileName     string         `gorm:"column:file_name"`
	FilePath     string         `gorm:"column:file_path"`
	FileStorage  string         `gorm:"column:file_storage"`
	FileType     string         `gorm:"column:file_type"`
	HasHeaders   *bool          `gorm:"column:has_headers"`
	ImportConfig datatypes.JSON `gorm:"column:import_config"`
	Stats        datatypes.JSON `gorm:"column:stats"`
	ErrorMessage string         `gorm:"column:error_message"`
	Retries      int            `gorm:"column:retries"`
	StartedAt    *time.Time     `gorm:"column:started_at"`
	CompletedAt  *time.Time     `gorm:"column:completed_at"`
	CreatedAt    time.Time      `gorm:"column:created_at"`
	UpdatedAt    time.Time      `gorm:"column:updated_at"`
}

func (ImportRequest) TableName() string {
	return "import_request"
}

func (r ImportRequest) HasHeaderRow() bool {
	return r.HasHeaders != nil && *r.HasHeaders
}

func GetPendingImportRequests(db *gorm.DB) ([]ImportRequest, error) {
	var requests []ImportRequest
	err := db.Table("import_request").
		Where("status IN ?", []string{consts.REQUESTED, consts.PROCESSING}).
		Order("created_at ASC").
		Find(&requests).Error
	return requests, err
}

func ClaimImportRequest(db *gorm.DB, id uuid.UUID) (bool, error) {
	now := time.Now()
	result := db.Table("import_request").
		Where("id = ? AND status IN ?", id, []string{consts.REQUESTED, consts.PROCESSING}).
		Updates(map[string]interface{}{
			"status":     consts.PROCESSING,
			"started_at": now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func UpdateImportRequestComplete(db *gorm.DB, id uuid.UUID, stats ImportStats) error {
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	now := time.Now()
	return db.Table("import_request").
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":        consts.COMPLETED,
			"stats":         datatypes.JSON(statsJSON),
			"error_message": nil,
			"completed_at":  now,
		}).Error
}

func UpdateImportRequestFailed(db *gorm.DB, id uuid.UUID, errorMessage string, stats *ImportStats) error {
	updates := map[string]interface{}{
		"status":        consts.FAILED,
		"error_message": errorMessage,
	}
	if stats != nil {
		statsJSON, err := json.Marshal(stats)
		if err != nil {
			return err
		}
		updates["stats"] = datatypes.JSON(statsJSON)
	}
	return db.Table("import_request").
		Where("id = ?", id).
		Updates(updates).Error
}

// ImportStats mirrors the JSON shape stored in import_request.stats.
type ImportStats struct {
	TotalRows     int `json:"total_rows"`
	ProcessedRows int `json:"processed_rows"`
	SkippedRows   int `json:"skipped_rows"`
	FailedRows    int `json:"failed_rows"`
}
