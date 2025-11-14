package util

import (
	"context"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// GetSourcesOnlySendingToSandbox returns a map of source IDs that only send to sandbox destination
// Uses bulk query to check all sources at once for efficiency
func GetSourcesOnlySendingToSandbox(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (map[string]bool, error) {
	sandboxOnlySources := make(map[string]bool)

	// Query to find sources that have exactly 1 active pipeline and that pipeline goes to sandbox destination
	query := `
		SELECT DISTINCT pls.log_source_id
		FROM pipeline_log_sources_mapping pls
		JOIN pipelines p ON pls.pipeline_id = p.id
		JOIN pipeline_destinations_mapping pd ON p.id = pd.pipeline_id
		WHERE p.status = 'ACTIVE'
		  AND p.tenant_id = ?
		GROUP BY pls.log_source_id
		HAVING COUNT(DISTINCT p.id) = 1
		  AND COUNT(DISTINCT pd.destination_id) = 1
		  AND MAX(pd.destination_id::text) = ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, constants.SandboxDestinationID).Rows()
	if err != nil {
		logger.GetLogger().Error("error querying sources only sending to sandbox",
			zap.Error(err),
			zap.String("tenantId", tenantId.String()))
		return sandboxOnlySources, err
	}
	defer rows.Close()

	for rows.Next() {
		var sourceId string
		if err := rows.Scan(&sourceId); err != nil {
			logger.GetLogger().Error("error scanning source ID", zap.Error(err))
			continue
		}
		sandboxOnlySources[sourceId] = true
		logger.GetLogger().Debug("identified source only sending to sandbox",
			zap.String("sourceId", sourceId),
			zap.String("tenantId", tenantId.String()))
	}

	if err := rows.Err(); err != nil {
		logger.GetLogger().Error("error iterating over sandbox-only sources", zap.Error(err))
		return sandboxOnlySources, err
	}

	logger.GetLogger().Info("completed sandbox-only sources check",
		zap.String("tenantId", tenantId.String()),
		zap.Int("sandboxOnlySourceCount", len(sandboxOnlySources)))

	return sandboxOnlySources, nil
}

// ReadSourcesPaginated reads sources in paginated fashion for a given tenant
func ReadSourcesPaginated(db *gorm.DB, tenantId uuid.UUID, page, pageSize int) ([]source.Source, error) {
	var sources []source.Source
	offset := page * pageSize

	result := db.
		Where("tenant_id = ? AND status = 'ACTIVE'", tenantId).
		Limit(pageSize).
		Offset(offset).
		Find(&sources)

	return sources, result.Error
}

// IsSandboxDestination checks if a destination ID matches the sandbox destination
func IsSandboxDestination(destinationId string) bool {
	return destinationId == constants.SandboxDestinationID
}
