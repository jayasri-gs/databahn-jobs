package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pipelineconstants "github.com/databahn-ai/common-utils/constants"

	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	ackdb "github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/store/audit"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	defaultSandboxFirstWarningDays  = 5
	defaultSandboxSecondWarningDays = 6
	defaultSandboxDisableDays       = 7
	statusActive                    = "ACTIVE"
	statusDisabled                  = "DISABLED"
)

type sandboxPipelineEntity struct {
	PipelineID   uuid.UUID
	PipelineName string
	DataPlaneID  uuid.UUID
	TenantID     uuid.UUID
}

func (s sandboxPipelineEntity) GetEntityId() string {
	return s.PipelineID.String()
}

func (s sandboxPipelineEntity) GetEntityName() string {
	return s.PipelineName
}

func (s sandboxPipelineEntity) GetDataPlaneId() string {
	if s.DataPlaneID == uuid.Nil {
		return common.DatabahnDataPlaneId
	}
	return s.DataPlaneID.String()
}

func (s sandboxPipelineEntity) GetTenantId() string {
	return s.TenantID.String()
}

func (s sandboxPipelineEntity) GetSecondaryEntityId() string {
	return ""
}

// CheckSandboxStoragePipelines disables aged sandbox destination pipelines and alerts tenants.
func CheckSandboxStoragePipelines(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	db := config.GetDB()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error while creating alerts manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	defer alertsManager.Close(ctx)

	// Parse sandbox destination ID
	destID, err := uuid.Parse(pipelineconstants.SandboxDestinationId)
	if err != nil {
		errorMsg := fmt.Sprintf("error parsing sandbox destination ID: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error parsing sandbox destination ID", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Get all active tenants
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error getting tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Read warning and disable days from environment variables or use defaults
	firstWarningDays := utils.GetEnvInt("SANDBOX_FIRST_WARNING_DAYS", defaultSandboxFirstWarningDays)
	secondWarningDays := utils.GetEnvInt("SANDBOX_SECOND_WARNING_DAYS", defaultSandboxSecondWarningDays)
	disableDays := utils.GetEnvInt("SANDBOX_DISABLE_DAYS", defaultSandboxDisableDays)

	// Parse release date from environment variable (DD-MM-YYYY format)
	releaseTime, err := parseReleaseDate(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("failed to parse RELEASE_DATE: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		return common.NewJobResultFromErrors(jobErrors)
	}

	logger.GetLoggerWithContext(ctx).Info("checking sandbox destination pipelines",
		zap.Int("tenant_count", len(tenants)),
		zap.Int("first_warning_days", firstWarningDays),
		zap.Int("second_warning_days", secondWarningDays),
		zap.Int("disable_days", disableDays))

	now := time.Now().UTC()
	firstWarnCutoff := now.AddDate(0, 0, -firstWarningDays)
	secondWarnCutoff := now.AddDate(0, 0, -secondWarningDays)
	disableCutoff := now.AddDate(0, 0, -disableDays)

	var alertsToSend []*alerts_async.Alert
	totalPipelinesProcessed := 0

	// Process each tenant separately to avoid loading all pipelines in memory
	for _, t := range tenants {
		tenantId := t.Id.String()

		// Get sandbox destination pipelines for this tenant
		pipelines, err := pipeline.GetPipelinesByDestinationAndStatus(ctx, db, t.Id, destID, statusActive)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting sandbox destination pipelines for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("error getting sandbox destination pipelines for tenant",
				zap.String("tenant_id", tenantId),
				zap.Error(err))
			continue
		}

		if len(pipelines) == 0 {
			logger.GetLoggerWithContext(ctx).Debug("no sandbox destination pipelines found for tenant",
				zap.String("tenant_name", t.Name), zap.String("tenant_id", tenantId))
			continue
		}

		logger.GetLoggerWithContext(ctx).Debug("processing sandbox destination pipelines for tenant",
			zap.String("tenant_id", tenantId),
			zap.Int("pipeline_count", len(pipelines)))

		totalPipelinesProcessed += len(pipelines)

		for _, p := range pipelines {
			updatedAt := p.UpdatedAt.UTC()
			pipelineTenantId := p.TenantID.String()

			// If release date is set and pipeline was updated before release date, use release date
			if releaseTime != nil && updatedAt.Before(*releaseTime) {
				logger.GetLoggerWithContext(ctx).Info("pipeline updated before release date, using release date",
					zap.String("pipeline_id", p.ID.String()),
					zap.Time("original_updated_at", updatedAt),
					zap.Time("release_time", *releaseTime))
				updatedAt = *releaseTime
			}

			entity := sandboxPipelineEntity{
				PipelineID:   p.ID,
				PipelineName: p.Name,
				DataPlaneID:  p.DataPlaneID,
				TenantID:     p.TenantID,
			}

			switch {
			case updatedAt.Before(disableCutoff):
				if err := disableSandboxPipeline(ctx, db, &p); err != nil {
					errorMsg := fmt.Sprintf("error disabling sandbox pipeline %s for tenant %s: %v",
						p.ID.String(), pipelineTenantId, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("error disabling sandbox pipeline",
						zap.Error(err),
						zap.String("pipeline_id", p.ID.String()),
						zap.String("tenant_id", pipelineTenantId))
					continue
				}

				alert, buildErr := buildSandboxPipelineDisabledAlert(entity, updatedAt, now, disableDays)
				if buildErr != nil {
					errorMsg := fmt.Sprintf("error building sandbox pipeline disabled alert for pipeline %s: %v",
						p.ID.String(), buildErr)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("error building sandbox pipeline disabled alert",
						zap.Error(buildErr),
						zap.String("pipeline_id", p.ID.String()),
						zap.String("tenant_id", pipelineTenantId))
					continue
				}
				alertsToSend = append(alertsToSend, alert)

			case updatedAt.Before(secondWarnCutoff):
				alert, buildErr := buildSandboxPipelineWarningAlert(entity, updatedAt, now, disableDays, alerts_async.SandboxExpirationSecondWarning)
				if buildErr != nil {
					errorMsg := fmt.Sprintf("error building sandbox pipeline second warning alert for pipeline %s: %v",
						p.ID.String(), buildErr)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("error building sandbox pipeline second warning alert",
						zap.Error(buildErr),
						zap.String("pipeline_id", p.ID.String()),
						zap.String("tenant_id", pipelineTenantId))
					continue
				}
				alertsToSend = append(alertsToSend, alert)

			case updatedAt.Before(firstWarnCutoff):
				alert, buildErr := buildSandboxPipelineWarningAlert(entity, updatedAt, now, disableDays, alerts_async.SandboxExpirationFirstWarning)
				if buildErr != nil {
					errorMsg := fmt.Sprintf("error building sandbox pipeline first warning alert for pipeline %s: %v",
						p.ID.String(), buildErr)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("error building sandbox pipeline first warning alert",
						zap.Error(buildErr),
						zap.String("pipeline_id", p.ID.String()),
						zap.String("tenant_id", pipelineTenantId))
					continue
				}
				alertsToSend = append(alertsToSend, alert)

			default:
				logger.GetLoggerWithContext(ctx).Debug("sandbox pipeline is within allowed window",
					zap.String("pipeline_id", p.ID.String()),
					zap.String("tenant_id", pipelineTenantId),
					zap.Time("updated_at", updatedAt))
			}
		}
	}

	if len(alertsToSend) > 0 {
		if err := alertsManager.SendAlerts(alertsToSend); err != nil {
			errorMsg := fmt.Sprintf("error sending sandbox pipeline alerts: %v", err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("error sending sandbox pipeline alerts", zap.Error(err))
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed sandbox destination pipeline checker",
			zap.Int("total_pipelines_processed", totalPipelinesProcessed),
			zap.Int("alerts_sent", len(alertsToSend)))
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("sandbox destination pipeline checker completed with errors",
		zap.Int("error_count", len(jobErrors)),
		zap.Int("total_pipelines_processed", totalPipelinesProcessed),
		zap.Int("alerts_sent", len(alertsToSend)))
	return common.NewJobResultFromErrors(jobErrors)
}

func disableSandboxPipeline(ctx context.Context, db *gorm.DB, pl *pipeline.Pipeline) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updateResult := tx.Model(&pipeline.Pipeline{}).
			Where("id = ? AND status = ?", pl.ID, statusActive).
			Updates(map[string]any{
				"status":     statusDisabled,
				"updated_at": time.Now().UTC(),
			})
		if updateResult.Error != nil {
			return updateResult.Error
		}

		if updateResult.RowsAffected == 0 {
			logger.GetLoggerWithContext(ctx).Info("pipeline already disabled or not active, skipping change flag",
				zap.String("pipeline_id", pl.ID.String()))
			return nil
		}

		// Get source_id for the pipeline
		var sourceMapping pipeline.PipelineLogSourceMapping
		err := tx.Where("pipeline_id = ?", pl.ID).First(&sourceMapping).Error
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error getting source mapping for pipeline",
				zap.String("pipeline_id", pl.ID.String()),
				zap.Error(err))
			return err
		}

		// Create the body JSON for change flag
		bodyData := map[string]interface{}{
			"entity": map[string]interface{}{
				"pipeline_id":   pl.ID.String(),
				"tenant_id":     pl.TenantID.String(),
				"source_id":     sourceMapping.LogSourceID.String(),
				"pipeline_next": nil,
			},
		}
		bodyJSON, err := json.Marshal(bodyData)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error marshaling change flag body",
				zap.String("pipeline_id", pl.ID.String()),
				zap.Error(err))
			return err
		}

		req := ackdb.ChangeFlagRequest{
			RequestId:   uuid.New().String(),
			EntityId:    pl.ID.String(),
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
			TenantId:    pl.TenantID.String(),
			EntityType:  utilConst.EntityPipeline,
			Action:      utilConst.ActionDelete,
			IsProcessed: false,
			Body:        string(bodyJSON),
			DataPlaneId: pl.DataPlaneID.String(),
			EntityName:  pl.Name,
		}

		if err := tx.Table(constants.TableChangeFlagRequest).Create(&req).Error; err != nil {
			return err
		}

		logger.GetLoggerWithContext(ctx).Info("pipeline disabled and change flag created",
			zap.String("pipeline_id", pl.ID.String()),
			zap.String("tenant_id", pl.TenantID.String()),
			zap.String("source_id", sourceMapping.LogSourceID.String()),
			zap.String("data_plane_id", pl.DataPlaneID.String()))

		// Create audit entry for pipeline disable action
		if err := createPipelineDisabledAuditEntry(tx, pl); err != nil {
			logger.GetLoggerWithContext(ctx).Error("error creating audit entry for disabled pipeline",
				zap.String("pipeline_id", pl.ID.String()),
				zap.Error(err))
			// Don't fail the transaction for audit entry failure, just log it
		}

		return nil
	})
}

func buildSandboxPipelineWarningAlert(entity sandboxPipelineEntity, lastUpdatedAt time.Time, now time.Time, disableDays int, functionalityType alerts_async.FunctionalityType) (*alerts_async.Alert, error) {
	daysSinceUpdate := int(now.Sub(lastUpdatedAt).Hours() / 24)
	daysUntilDisable := disableDays - daysSinceUpdate

	title := fmt.Sprintf("Sandbox pipeline '%s' unchanged for %d days", entity.PipelineName, daysSinceUpdate)
	message := fmt.Sprintf("Pipeline '%s' targeting Databahn Sandbox Destination has not been modified since %s (%d days ago). "+
		"This pipeline will be automatically disabled in %d day(s) if no changes are made. "+
		"To prevent automatic disabling, update the pipeline configuration or move to a permanent destination before %s.",
		entity.PipelineName,
		util.HumanReadableTimeWithZone(lastUpdatedAt),
		daysSinceUpdate,
		daysUntilDisable,
		util.HumanReadableTimeWithZone(lastUpdatedAt.AddDate(0, 0, disableDays)),
	)
	action := "Update the pipeline configuration or move to a permanent destination to keep it active."

	return alerts_async.NewAlert(
		alerts_async.Pipeline,
		alerts_async.WithEntity(entity),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(functionalityType),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DGRW10001, "Sandbox pipeline guardrail"),
		alerts_async.WithAction(action),
	)
}

func buildSandboxPipelineDisabledAlert(entity sandboxPipelineEntity, lastUpdatedAt time.Time, now time.Time, disableDays int) (*alerts_async.Alert, error) {
	daysSinceUpdate := int(now.Sub(lastUpdatedAt).Hours() / 24)

	title := fmt.Sprintf("Sandbox pipeline '%s' disabled after %d days without changes", entity.PipelineName, daysSinceUpdate)
	message := fmt.Sprintf("Pipeline '%s' targeting Databahn Sandbox Destination was automatically disabled after remaining unchanged for more than %d days. "+
		"Pipeline status has been set to DISABLED. To use this pipeline again, re-enable it and consider moving to a permanent destination.",
		entity.PipelineName,
		disableDays,
	)

	return alerts_async.NewAlert(
		alerts_async.Pipeline,
		alerts_async.WithEntity(entity),
		alerts_async.WithCriticality(alerts_async.Sever),
		alerts_async.WithFunctionalityType(alerts_async.SandboxAutoDisabled),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DGRW10001, "Sandbox pipeline guardrail"),
		alerts_async.WithAction("Re-enable the pipeline if it is still required."),
	)
}

// createPipelineDisabledAuditEntry creates an audit log entry for a disabled sandbox destination pipeline
func createPipelineDisabledAuditEntry(tx *gorm.DB, pl *pipeline.Pipeline) error {
	isSuccess := true
	entry := &audit.AuditEntry{
		TenantUUID: pl.TenantID,
		ObjectID:   pl.ID.String(),
		ObjectType: "Pipeline",
		ObjectName: pl.Name,
		Action:     "Disabled",
		Timestamp:  time.Now(),
		IsSuccess:  &isSuccess,
		Message:    "Sandbox destination pipeline automatically disabled due to inactivity (guardrail policy)",
	}

	return audit.CreateAuditEntry(tx, entry)
}

// parseReleaseDate parses the RELEASE_DATE environment variable and returns the release time.
// Returns nil if RELEASE_DATE is not set or is "0".
// Expected format: DD-MM-YYYY
func parseReleaseDate(ctx context.Context) (*time.Time, error) {
	releaseDateStr := utils.GetEnvOrDefault("RELEASE_DATE", "0")
	if releaseDateStr == "0" {
		return nil, nil
	}

	parsedTime, err := time.Parse("02-01-2006", releaseDateStr)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error parsing RELEASE_DATE",
			zap.String("value", releaseDateStr),
			zap.Error(err))
		return nil, fmt.Errorf("error parsing RELEASE_DATE '%s': %w", releaseDateStr, err)
	}

	releaseTimeUTC := parsedTime.UTC()
	logger.GetLoggerWithContext(ctx).Info("using release date for pipeline comparison",
		zap.String("release_date", releaseDateStr),
		zap.Time("release_time_utc", releaseTimeUTC))

	return &releaseTimeUTC, nil
}
