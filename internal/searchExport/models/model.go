package models

import (
	"encoding/json"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/consts"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type SearchExportReport struct {
	ID                  uuid.UUID      `json:"id" gorm:"column:id"`
	Name                string         `json:"name" gorm:"column:name"`
	TenantID            string         `json:"tenant_id" gorm:"column:tenant_id"`
	ReportType          string         `json:"report_type" gorm:"column:report_type"`
	Status              string         `json:"status" gorm:"column:status"`
	ReportConfiguration datatypes.JSON `json:"report_configuration" gorm:"column:report_configuration"`
	DownloadLink        string         `json:"download_link" gorm:"column:download_link"`
	DownloadLinkExpiry  time.Time      `json:"download_link_expiry" gorm:"column:download_link_expiry"`
	Retries             int            `json:"retries" gorm:"column:retries"`
}

func (SearchExportReport) TableName() string {
	return "audit_report"
}

type ReportConfiguration struct {
	SearchExportConfig *SearchExportConfig `json:"searchExportConfig,omitempty"`
}

type SearchExportConfig struct {
	Query         string `json:"query"`
	Database      string `json:"database"`
	DataStoreID   string `json:"dataStoreId"`
	DataSetID     string `json:"dataSetId"`
	DataStoreType string `json:"dataStoreType"`
	StartTime     int64  `json:"startTime"`
	EndTime       int64  `json:"endTime"`
	Format        string `json:"format"`
	Delimiter     string `json:"delimiter"`
	IncludeHeader bool   `json:"includeHeader"`
	DestinationID string `json:"destinationId"`
}

func (r *SearchExportReport) GetConfig() (*SearchExportConfig, error) {
	var config ReportConfiguration
	if err := json.Unmarshal(r.ReportConfiguration, &config); err != nil {
		return nil, err
	}
	return config.SearchExportConfig, nil
}

func GetSearchExportRequests(db *gorm.DB) ([]SearchExportReport, error) {
	var reports []SearchExportReport
	status := []string{consts.REQUESTED, consts.FAILED}
	err := db.Table("audit_report").
		Where("status IN ? AND retries < ? AND report_type = ?",
			status, consts.MaxRetries, consts.ReportTypeSEARCH_EXPORT).
		Find(&reports).Error
	return reports, err
}

func UpdateRequestStatus(db *gorm.DB, id string, status string) error {
	return db.Table("audit_report").
		Where("id = ?", id).
		Update("status", status).Error
}

func UpdateRequestStatusAndRetries(db *gorm.DB, id string, status string, retries int) error {
	return db.Table("audit_report").
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":  status,
			"retries": retries,
		}).Error
}

func UpdateExportComplete(db *gorm.DB, id string, downloadLink string, expiry time.Time) error {
	return db.Table("audit_report").
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":               consts.COMPLETED,
			"download_link":        downloadLink,
			"download_link_expiry": expiry,
		}).Error
}
