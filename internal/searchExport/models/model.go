package models

import (
	"encoding/json"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/consts"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	QueryEngineAthena   = "ATHENA"
	QueryEngineSynapse  = "SYNAPSE"
	QueryEngineKustoADX = "KUSTO_ADX"
	QueryEngineKustoLAW = "KUSTO_LAW"
	DestTypeS3          = "S3"
	DestTypeS3Parquet   = "S3_PARQUET"
	DestTypeAzureBlob   = "AZURE_BLOB"
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
	Query               string `json:"query"`
	Database            string `json:"database"`
	DataStoreID         string `json:"dataStoreId"`
	DataSetID           string `json:"dataSetId"`
	DataSetName         string `json:"dataSetName"`
	TableName           string `json:"tableName"`
	DataStoreType       string `json:"dataStoreType"`
	StartTime           int64  `json:"startTime"`
	EndTime             int64  `json:"endTime"`
	Format              string `json:"format"`
	Delimiter           string `json:"delimiter"`
	IncludeHeader       bool   `json:"includeHeader"`
	DestinationID       string `json:"destinationId"`
	GlobalDestinationID string `json:"globalDestinationId,omitempty"`

	QueryEngine           string `json:"queryEngine,omitempty"`
	StorageTier           string `json:"storageTier,omitempty"`
	DestinationType       string `json:"destinationType,omitempty"`
	SynapseDataSourceName string `json:"synapseDataSourceName,omitempty"`
	QueryExecutionID      string `json:"queryExecutionId,omitempty"`

	// Sentinel — written by backend-service, unread by the worker today. They are what a
	// future chunked export needs to insert a per-window time filter after the table
	// reference; see query.PlanSentinelQueries.
	KqlTable      string `json:"kqlTable,omitempty"`
	KqlTimeColumn string `json:"kqlTimeColumn,omitempty"`

	// External Athena (Security Lake / external S3) — additive; null on older audit rows.
	ExternalSearchProvider string `json:"externalSearchProvider,omitempty"`
	AthenaOutputLocation   string `json:"athenaOutputLocation,omitempty"`
	Region                 string `json:"region,omitempty"`

	// Runtime fields — written by jobs worker, ignored by backend-service
	AthenaExecutionID  string     `json:"athenaExecutionId,omitempty"`
	ExecutionStartedAt *time.Time `json:"executionStartedAt,omitempty"`
}

func (r *SearchExportReport) GetConfig() (*SearchExportConfig, error) {
	var config ReportConfiguration
	if err := json.Unmarshal(r.ReportConfiguration, &config); err != nil {
		return nil, err
	}
	return config.SearchExportConfig, nil
}

func GetSearchExportRequests(db *gorm.DB, staleCutoff time.Time) ([]SearchExportReport, error) {
	var reports []SearchExportReport
	err := db.Table("audit_report").
		Where(
			"((status IN ? AND retries < ?) OR "+
				"(status = ? AND retries < ? AND "+
				"((report_configuration->'searchExportConfig'->>'executionStartedAt')::timestamptz < ? "+
				"OR (report_configuration->'searchExportConfig'->>'executionStartedAt' IS NULL "+
				"AND updated_at < ?)))) "+
				"AND report_type = ?",
			[]string{consts.REQUESTED, consts.FAILED}, consts.MaxRetries,
			consts.PROCESSING, consts.MaxRetries, staleCutoff, staleCutoff,
			consts.ReportTypeSEARCH_EXPORT,
		).
		Find(&reports).Error
	return reports, err
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

// UpdateAthenaExecutionID writes the Athena query execution ID into report_configuration JSON.
func UpdateAthenaExecutionID(db *gorm.DB, id, executionID string) error {
	return UpdateQueryExecutionID(db, id, executionID)
}

// UpdateQueryExecutionID writes the query execution ID into report_configuration JSON.
func UpdateQueryExecutionID(db *gorm.DB, id, executionID string) error {
	return db.Table("audit_report").
		Where("id = ?", id).
		Update("report_configuration",
			gorm.Expr(
				`jsonb_set(
					jsonb_set(report_configuration::jsonb, '{searchExportConfig,queryExecutionId}', to_jsonb(?::text)),
					'{searchExportConfig,athenaExecutionId}', to_jsonb(?::text)
				)::json`,
				executionID, executionID,
			),
		).Error
}

// UpdateExecutionStartedAt writes the timestamp into report_configuration JSON when processing begins.
func UpdateExecutionStartedAt(db *gorm.DB, id string, t time.Time) error {
	return db.Table("audit_report").
		Where("id = ?", id).
		Update("report_configuration",
			gorm.Expr(
				"jsonb_set(report_configuration::jsonb, '{searchExportConfig,executionStartedAt}', to_jsonb(?::text))::json",
				t.UTC().Format(time.RFC3339),
			),
		).Error
}

// ClaimPendingJob atomically moves a REQUESTED/FAILED report to PROCESSING.
// Returns true only for the caller whose UPDATE actually transitioned the row,
// so two pods polling the same report can never both process it (and never
// abort each other's in-flight multipart uploads via prefix cleanup).
func ClaimPendingJob(db *gorm.DB, id string) (bool, error) {
	result := db.Table("audit_report").
		Where("id = ? AND status IN ?", id, []string{consts.REQUESTED, consts.FAILED}).
		Update("status", consts.PROCESSING)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// ClaimStaleProcessingJob atomically refreshes executionStartedAt for a stale PROCESSING job.
// Returns true if this caller successfully claimed the job (RowsAffected == 1).
// Returns false if another pod already claimed it.
func ClaimStaleProcessingJob(db *gorm.DB, id string, staleCutoff time.Time) (bool, error) {
	result := db.Table("audit_report").
		Where(
			"id = ? AND status = ? AND "+
				"((report_configuration->'searchExportConfig'->>'executionStartedAt')::timestamptz < ? "+
				"OR (report_configuration->'searchExportConfig'->>'executionStartedAt' IS NULL AND updated_at < ?))",
			id, consts.PROCESSING, staleCutoff, staleCutoff,
		).
		Update("report_configuration",
			gorm.Expr(
				"jsonb_set(report_configuration::jsonb, '{searchExportConfig,executionStartedAt}', to_jsonb(?::text))::json",
				time.Now().UTC().Format(time.RFC3339),
			),
		)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}
