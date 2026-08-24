package searchExport

import (
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func exportLogger(report models.SearchExportReport, cfg *models.SearchExportConfig) *zap.Logger {
	fields := []zap.Field{
		zap.String("reportId", report.ID.String()),
		zap.String("tenantId", report.TenantID),
		zap.Int("retries", report.Retries),
	}
	if cfg != nil {
		fields = append(fields,
			zap.String("destinationId", cfg.DestinationID),
			zap.String("dataStoreId", cfg.DataStoreID),
			zap.String("dataSetId", cfg.DataSetID),
			zap.String("dataStoreType", cfg.DataStoreType),
			zap.String("format", cfg.Format),
			zap.String("database", cfg.Database),
			zap.String("query", cfg.Query),
		)
		if cfg.ExternalSearchProvider != "" {
			fields = append(fields, zap.String("externalSearchProvider", cfg.ExternalSearchProvider))
		}
	}
	return logging.GetLogger().With(fields...)
}
