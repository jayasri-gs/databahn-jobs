package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/util"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Default values for environment variables
const (
	DefaultDeploymentTimeoutMinutes = 15
	DefaultDataPlaneId              = "dbd00000-0000-0000-0000-000000000000" // Default value for data plane ID if not set
)

func SendAlertForDeploymentDelayAlert(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	// Load configuration from environment variables
	timeoutMinutes := utils.GetEnvInt("DEPLOYMENT_TIMEOUT_MINUTES", DefaultDeploymentTimeoutMinutes)

	logger.GetLoggerWithContext(ctx).Info("Deploying state alert configuration loaded",
		zap.Int("timeout_minutes", timeoutMinutes),
	)

	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting all tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("Error getting all tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	// Calculate the cutoff time (current time - timeout minutes)
	cutoffTime := time.Now().UTC().Add(-time.Duration(timeoutMinutes) * time.Minute)

	for _, t := range tenants {
		tenantId := t.Id.String()
		logger.GetLoggerWithContext(ctx).Info("checking deploying entities for tenant", zap.String("tenant_id", tenantId))

		// Create deploying state checker for this tenant
		checker := NewDeployingStateChecker(ctx, db, alertsManager, t.Id, cutoffTime)

		// Check each entity type for deploying state
		err := checker.CheckAndAlertDeployingEntities()
		if err != nil {
			errorMsg := fmt.Sprintf("error checking deploying entities for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("Error checking deploying entities for tenant",
				zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed deployment delay alert processing")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLoggerWithContext(ctx).Info("deployment delay alert processing completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

// DataPlaneCache caches data plane IDs to avoid repeated queries
type DataPlaneCache struct {
	pipelineToDataPlane map[uuid.UUID]uuid.UUID
}

// NewDataPlaneCache creates a new data plane cache
func NewDataPlaneCache() *DataPlaneCache {
	return &DataPlaneCache{
		pipelineToDataPlane: make(map[uuid.UUID]uuid.UUID),
	}
}

// DeployingStateChecker handles deploying state checks and alerts for a specific tenant
type DeployingStateChecker struct {
	ctx           context.Context
	db            *gorm.DB
	alertsManager *alert.AlertsManager
	tenantId      uuid.UUID
	cutoffTime    time.Time
	cache         *DataPlaneCache
}

// NewDeployingStateChecker creates a new deploying state checker for a tenant
func NewDeployingStateChecker(ctx context.Context, db *gorm.DB, alertsManager *alert.AlertsManager, tenantId uuid.UUID, cutoffTime time.Time) *DeployingStateChecker {
	return &DeployingStateChecker{
		ctx:           ctx,
		db:            db,
		alertsManager: alertsManager,
		tenantId:      tenantId,
		cutoffTime:    cutoffTime,
		cache:         NewDataPlaneCache(),
	}
}

// CheckAndAlertDeployingEntities checks all entity types for deploying state and sends alerts
func (checker *DeployingStateChecker) CheckAndAlertDeployingEntities() error {
	// Process deploying entities and send alerts
	if err := checker.processDeployingEntities(); err != nil {
		return err
	}

	// Collect healthy entities for auto-resolution
	healthyEntities, err := checker.collectHealthyEntities()
	if err != nil {
		return err
	}

	// Auto-resolve alerts for healthy entities
	if err := checker.autoResolveHealthyEntityAlerts(healthyEntities); err != nil {
		return err
	}

	return nil
}

// processDeployingEntities checks all entity types for deploying state and sends alerts
func (checker *DeployingStateChecker) processDeployingEntities() error {
	// Check sources
	if err := checker.processEntityType("sources", func() ([]model.DeployingEntity, error) {
		return getDeployingSources(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	// Check destinations
	if err := checker.processEntityType("destinations", func() ([]model.DeployingEntity, error) {
		return getDeployingDestinations(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	// Check insight rules
	if err := checker.processEntityType("insight rules", func() ([]model.DeployingEntity, error) {
		return getDeployingInsightRules(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	// Check VC rules
	if err := checker.processEntityType("vc rules", func() ([]model.DeployingEntity, error) {
		return getDeployingVCRules(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	// Check enrichments
	if err := checker.processEntityType("enrichments", func() ([]model.DeployingEntity, error) {
		return getDeployingEnrichments(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	// Check route processors
	if err := checker.processEntityType("route processors", func() ([]model.DeployingEntity, error) {
		return getDeployingRouteProcessors(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime, checker.cache)
	}); err != nil {
		return err
	}

	// Check lookups
	if err := checker.processEntityType("lookups", func() ([]model.DeployingEntity, error) {
		return getDeployingLookups(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime, checker.cache)
	}); err != nil {
		return err
	}

	// Check data transformations
	if err := checker.processEntityType("data transformations", func() ([]model.DeployingEntity, error) {
		return getDeployingDataTransformations(checker.ctx, checker.db, checker.tenantId, checker.cutoffTime)
	}); err != nil {
		return err
	}

	return nil
}

// processEntityType processes a single entity type for deploying alerts
func (checker *DeployingStateChecker) processEntityType(entityTypeName string, getEntities func() ([]model.DeployingEntity, error)) error {
	entities, err := getEntities()
	if err != nil {
		return fmt.Errorf("error getting deploying %s: %w", entityTypeName, err)
	}

	for _, entity := range entities {
		if err := sendDeployingAlert(checker.ctx, checker.alertsManager, entity); err != nil {
			logger.GetLoggerWithContext(checker.ctx).Error(fmt.Sprintf("Error sending deploying %s alert", entityTypeName),
				zap.Error(err), zap.String("entity_id", entity.ID.String()))
		}
	}

	return nil
}

// collectHealthyEntities collects all healthy entities for auto-resolution
func (checker *DeployingStateChecker) collectHealthyEntities() ([]model.HealthyEntity, error) {
	var healthyEntities []model.HealthyEntity

	// Collect healthy sources
	if healthySources, err := getHealthySources(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy sources",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthySources {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeSource})
		}
	}

	// Collect healthy destinations
	if healthyDestinations, err := getHealthyDestinations(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy destinations",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyDestinations {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeDestination})
		}
	}

	// Collect healthy insight rules
	if healthyInsightRules, err := getHealthyInsightRules(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy insight rules",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyInsightRules {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeInsightRule})
		}
	}

	// Collect healthy VC rules
	if healthyVCRules, err := getHealthyVCRules(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy vc rules",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyVCRules {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeVCRule})
		}
	}

	// Collect healthy enrichments
	if healthyEnrichments, err := getHealthyEnrichments(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy enrichments",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyEnrichments {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeEnrichment})
		}
	}

	// Collect healthy route processors
	if healthyRouteProcessors, err := getHealthyRouteProcessors(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy route processors",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyRouteProcessors {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeRouteProcessor})
		}
	}

	// Collect healthy lookups
	if healthyLookups, err := getHealthyLookups(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy lookups",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyLookups {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeLookup})
		}
	}

	// Collect healthy data transformations
	if healthyDataTransformations, err := getHealthyDataTransformations(checker.ctx, checker.db, checker.tenantId); err != nil {
		logger.GetLoggerWithContext(checker.ctx).Error("Error getting healthy data transformations",
			zap.Error(err), zap.String("tenant_id", checker.tenantId.String()))
	} else {
		for _, entityId := range healthyDataTransformations {
			healthyEntities = append(healthyEntities, model.HealthyEntity{EntityId: entityId, EntityType: model.EntityTypeDataTransformation})
		}
	}

	return healthyEntities, nil
}

// autoResolveHealthyEntityAlerts auto-resolves alerts for healthy entities
func (checker *DeployingStateChecker) autoResolveHealthyEntityAlerts(healthyEntities []model.HealthyEntity) error {
	if len(healthyEntities) == 0 {
		return nil
	}

	osClient := os.GetClient()

	tenantIdStr := checker.tenantId.String()

	for _, healthy := range healthyEntities {
		functionalityType := alerts_async.DeploymentStatus.String()

		q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionalityType:%s AND functionalityEntityId:%s",
			tenantIdStr, functionalityType, healthy.EntityId)

		// Use pagination to handle potentially large result sets
		var after []any
		pageSize := 100
		sort := []os.Sort{{Field: "updatedAt", Order: "asc"}}
		for {
			var alertsToDismiss []string
			openAlerts, newAfter, err := os.SearchPaginated(checker.ctx, osClient, common.AlertsIndex, q, pageSize, after, sort)
			if err != nil {
				logger.GetLoggerWithContext(checker.ctx).Error("error while searching deploying alerts to auto-resolve",
					zap.Error(err), zap.String("query", q))
				break
			}

			alerts, err := statistics.ParseAlertDocuments(openAlerts)
			if err != nil {
				logger.GetLoggerWithContext(checker.ctx).Error("error while decoding OpenSearch deploying alert response",
					zap.Error(err), zap.String("tenantId", tenantIdStr))
				break
			}

			for _, alrt := range alerts {
				alertsToDismiss = append(alertsToDismiss, alrt.Id)
			}

			if err := checker.alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
				logger.GetLoggerWithContext(checker.ctx).Error("error while auto-resolving deploying alerts",
					zap.Error(err), zap.String("tenant_id", tenantIdStr))
			}

			// Break if we received fewer results than page size (end of data)
			if len(openAlerts) < pageSize {
				break
			}

			after = newAfter
		}
	}

	return nil
}

// getDeployingSources queries log_source table for entities stuck in DEPLOYING state
func getDeployingSources(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var sources []model.DeployingEntity

	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM log_source 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeSource
		sources = append(sources, entity)
	}

	return sources, nil
}

// getDeployingDestinations queries destination table for entities stuck in DEPLOYING state
func getDeployingDestinations(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var destinations []model.DeployingEntity

	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM destination 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeDestination
		destinations = append(destinations, entity)
	}

	return destinations, nil
}

// getDeployingInsightRules queries insights_rule table for entities stuck in DEPLOYING state
func getDeployingInsightRules(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var insightRules []model.DeployingEntity

	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM insights_rule 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeInsightRule
		insightRules = append(insightRules, entity)
	}

	return insightRules, nil
}

// getDeployingVCRules queries vc_rule table for entities stuck in DEPLOYING state
// Excludes VC rules attached to pipelines going to sandbox destination
func getDeployingVCRules(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var vcRules []model.DeployingEntity

	// Join with pipeline and destination to exclude sandbox pipelines
	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM vc_rule 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeVCRule
		vcRules = append(vcRules, entity)
	}

	return vcRules, nil
}

// getHealthySources queries log_source table for entities that are no longer in DEPLOYING state
func getHealthySources(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM log_source 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getHealthyDestinations queries destination table for entities that are no longer in DEPLOYING state
func getHealthyDestinations(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM destination 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getHealthyInsightRules queries insights_rule table for entities that are no longer in DEPLOYING state
func getHealthyInsightRules(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM insights_rule 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getHealthyVCRules queries vc_rule table for entities that are no longer in DEPLOYING state
func getHealthyVCRules(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM vc_rule 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getDeployingEnrichments queries enrichment table for entities stuck in DEPLOYING state
func getDeployingEnrichments(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var enrichments []model.DeployingEntity

	// Join with pipeline and destination to exclude sandbox pipelines
	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM enrichment 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeEnrichment
		enrichments = append(enrichments, entity)
	}

	return enrichments, nil
}

// getHealthyEnrichments queries enrichment table for entities that are no longer in DEPLOYING state
func getHealthyEnrichments(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM enrichment 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getDeployingRouteProcessors queries route_processor table for entities stuck in DEPLOYING state
func getDeployingRouteProcessors(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time, cache *DataPlaneCache) ([]model.DeployingEntity, error) {
	var routeProcessors []model.DeployingEntity

	query := `
		SELECT id, tenant_id, data_plane_id, pipeline_id, updated_at
		FROM route_processor 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		var pipelineId uuid.UUID
		err := rows.Scan(&entity.ID, &entity.TenantID, &entity.DataPlaneID, &pipelineId, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeRouteProcessor
		// Route processor doesn't have a name field, so use ID as name
		entity.Name = entity.ID.String()
		routeProcessors = append(routeProcessors, entity)
	}

	return routeProcessors, nil
}

// getHealthyRouteProcessors queries route_processor table for entities that are no longer in DEPLOYING state
func getHealthyRouteProcessors(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM route_processor 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getDeployingLookups queries lookup table for entities stuck in DEPLOYING state
func getDeployingLookups(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time, cache *DataPlaneCache) ([]model.DeployingEntity, error) {
	var lookups []model.DeployingEntity

	query := `
		SELECT id, name, tenant_id, updated_at
		FROM lookup 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.DataPlaneID = DefaultDataPlaneId
		entity.Type = model.EntityTypeLookup
		lookups = append(lookups, entity)
	}

	return lookups, nil
}

// getHealthyLookups queries lookup table for entities that are no longer in DEPLOYING state
func getHealthyLookups(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM lookup 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// getDeployingDataTransformations queries data_transformation table for entities stuck in DEPLOYING state
func getDeployingDataTransformations(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, cutoffTime time.Time) ([]model.DeployingEntity, error) {
	var dataTransformations []model.DeployingEntity

	// Join with pipeline and destination to exclude sandbox pipelines
	query := `
		SELECT id, name, tenant_id, data_plane_id, updated_at
		FROM data_transformation 
		WHERE tenant_id = ? AND status = 'DEPLOYING' AND updated_at < ?
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId, cutoffTime).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entity model.DeployingEntity
		err := rows.Scan(&entity.ID, &entity.Name, &entity.TenantID, &entity.DataPlaneID, &entity.UpdatedAt)
		if err != nil {
			return nil, err
		}

		entity.Type = model.EntityTypeDataTransformation
		dataTransformations = append(dataTransformations, entity)
	}

	return dataTransformations, nil
}

// getHealthyDataTransformations queries data_transformation table for entities that are no longer in DEPLOYING state
func getHealthyDataTransformations(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) ([]string, error) {
	var entityIds []string

	query := `
		SELECT id
		FROM data_transformation 
		WHERE tenant_id = ? AND status != 'DEPLOYING'
	`

	rows, err := db.WithContext(ctx).Raw(query, tenantId).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityId string
		err := rows.Scan(&entityId)
		if err != nil {
			return nil, err
		}
		entityIds = append(entityIds, entityId)
	}

	return entityIds, nil
}

// sendDeployingAlert sends an alert for an entity stuck in deploying state
func sendDeployingAlert(ctx context.Context, alertsManager *alert.AlertsManager, entity model.DeployingEntity) error {
	deployingAlert, err := buildDeployingAlert(entity)
	if err != nil {
		return fmt.Errorf("error building deploying alert: %w", err)
	}

	err = alertsManager.SendAlerts([]*alerts_async.Alert{deployingAlert})
	if err != nil {
		return fmt.Errorf("error sending deploying alert: %w", err)
	}

	logger.GetLoggerWithContext(ctx).Info("Successfully sent deploying state alert",
		zap.String("entity_type", string(entity.Type)),
		zap.String("entity_id", entity.ID.String()),
		zap.String("entity_name", entity.Name))

	return nil
}

// buildDeployingAlert builds an alert for an entity stuck in deploying state
func buildDeployingAlert(entity model.DeployingEntity) (*alerts_async.Alert, error) {
	var entityTypeName string
	var functionality alerts_async.Functionality

	switch entity.Type {
	case model.EntityTypeSource:
		entityTypeName = "Source"
		functionality = alerts_async.LogSource
	case model.EntityTypeDestination:
		entityTypeName = "Destination"
		functionality = alerts_async.Dispenser
	case model.EntityTypeInsightRule:
		entityTypeName = "Insight Rule"
		functionality = alerts_async.InsightsRule
	case model.EntityTypeVCRule:
		entityTypeName = "Volume Control Rule"
		functionality = alerts_async.VolumeControlRule
	case model.EntityTypeEnrichment:
		entityTypeName = "Enrichment"
		functionality = alerts_async.Enrichment
	case model.EntityTypeRouteProcessor:
		entityTypeName = "Route Processor"
		functionality = alerts_async.RouteProcessor
	case model.EntityTypeLookup:
		entityTypeName = "Lookup"
		functionality = alerts_async.Lookup
	case model.EntityTypeDataTransformation:
		entityTypeName = "Data Transformation"
		functionality = alerts_async.Transformer
	default:
		entityTypeName = "Entity"
		functionality = alerts_async.Unknown
	}

	timeSinceUpdate := time.Since(entity.UpdatedAt)

	title := fmt.Sprintf("Deployment Alert: %s '%s' Stuck in Deploying State", entityTypeName, entity.Name)

	message := fmt.Sprintf(
		"%s '%s' has been stuck in DEPLOYING state for %s. "+
			"The %s was last updated on %s and may require manual intervention to complete the deployment or resolve any blocking issues.",
		entityTypeName,
		entity.Name,
		util.HumanReadableDuration(timeSinceUpdate),
		entityTypeName,
		util.HumanReadableTimeWithZone(entity.UpdatedAt),
	)

	return alerts_async.NewAlert(
		functionality,
		alerts_async.WithEntity(entity),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.DeploymentStatus),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DIOE30001, fmt.Sprintf("%s stuck in deploying state", entityTypeName)),
		alerts_async.WithAction("Please contact Databahn Team for the help."),
	)
}
