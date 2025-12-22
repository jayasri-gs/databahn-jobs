package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	ackdb "github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	sandboxStorageDestinationID = "dbd00000-0000-0000-0000-000000000001"
	defaultSandboxWarningDays   = 5
	defaultSandboxDisableDays   = 7
	statusActive                = "ACTIVE"
	statusDisabled              = "DISABLED"
)

// CheckSandboxStoragePipelines disables aged sandbox storage pipelines and alerts tenants.
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

	// Parse sandbox storage destination ID
	destID, err := uuid.Parse(sandboxStorageDestinationID)
	if err != nil {
		errorMsg := fmt.Sprintf("error parsing sandbox storage destination ID: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error parsing sandbox storage destination ID", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Get all active pipelines with sandbox storage destination
	pipelines, err := pipeline.GetPipelinesByDestinationAndStatus(ctx, db, destID, statusActive)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting sandbox storage pipelines: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error getting sandbox storage pipelines", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Read warning and disable days from environment variables or use defaults
	warningDays := utils.GetEnvInt("SANDBOX_WARNING_DAYS", defaultSandboxWarningDays)
	disableDays := utils.GetEnvInt("SANDBOX_DISABLE_DAYS", defaultSandboxDisableDays)

	logger.GetLoggerWithContext(ctx).Info("checking sandbox storage pipelines",
		zap.Int("pipeline_count", len(pipelines)),
		zap.Int("warning_days", warningDays),
		zap.Int("disable_days", disableDays))

	now := time.Now().UTC()
	warnCutoff := now.AddDate(0, 0, -warningDays)
	disableCutoff := now.AddDate(0, 0, -disableDays)

	var alertsToSend []*alerts_async.Alert

	for _, p := range pipelines {
		updatedAt := p.UpdatedAt.UTC()
		tenantId := p.TenantID.String()

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
					p.ID.String(), tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("error disabling sandbox pipeline",
					zap.Error(err),
					zap.String("pipeline_id", p.ID.String()),
					zap.String("tenant_id", tenantId))
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
					zap.String("tenant_id", tenantId))
				continue
			}
			alertsToSend = append(alertsToSend, alert)

		case updatedAt.Before(warnCutoff):
			alert, buildErr := buildSandboxPipelineWarningAlert(entity, updatedAt, now, disableDays)
			if buildErr != nil {
				errorMsg := fmt.Sprintf("error building sandbox pipeline warning alert for pipeline %s: %v",
					p.ID.String(), buildErr)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("error building sandbox pipeline warning alert",
					zap.Error(buildErr),
					zap.String("pipeline_id", p.ID.String()),
					zap.String("tenant_id", tenantId))
				continue
			}
			alertsToSend = append(alertsToSend, alert)
		default:
			logger.GetLoggerWithContext(ctx).Debug("sandbox pipeline is within allowed window",
				zap.String("pipeline_id", p.ID.String()),
				zap.String("tenant_id", tenantId),
				zap.Time("updated_at", updatedAt))
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
		logger.GetLoggerWithContext(ctx).Info("successfully completed sandbox storage pipeline checker")
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("sandbox storage pipeline checker completed with errors", zap.Int("error_count", len(jobErrors)))
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

		return nil
	})
}

func buildSandboxPipelineWarningAlert(entity sandboxPipelineEntity, createdAt time.Time, now time.Time, disableDays int) (*alerts_async.Alert, error) {
	ageDays := int(now.Sub(createdAt).Hours() / 24)

	title := fmt.Sprintf("Sandbox pipeline '%s' active for %d days", entity.PipelineName, ageDays)
	message := fmt.Sprintf("Pipeline '%s' (ID: %s) targeting Databahn Sandbox Storage has been active since %s. "+
		"It will be automatically disabled after %d days of activity. If this pipeline is still needed, please disable it or move it before %s.",
		entity.PipelineName,
		entity.PipelineID.String(),
		util.HumanReadableTimeWithZone(createdAt),
		disableDays,
		util.HumanReadableTimeWithZone(createdAt.AddDate(0, 0, disableDays)),
	)

	return alerts_async.NewAlert(
		alerts_async.Unknown,
		alerts_async.WithEntity(entity),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.DeploymentStatus),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DGRW10001, "Sandbox pipeline guardrail"),
		alerts_async.WithAction("Disable or delete the sandbox storage pipeline if it is no longer needed."),
	)
}

func buildSandboxPipelineDisabledAlert(entity sandboxPipelineEntity, createdAt time.Time, now time.Time, disableDays int) (*alerts_async.Alert, error) {
	ageDays := int(now.Sub(createdAt).Hours() / 24)

	title := fmt.Sprintf("Sandbox pipeline '%s' disabled after %d days", entity.PipelineName, ageDays)
	message := fmt.Sprintf("Pipeline '%s' (ID: %s) targeting Databahn Sandbox Storage was disabled after being active for more than %d days. "+
		"Pipeline status has been set to DISABLED",
		entity.PipelineName,
		entity.PipelineID.String(),
		disableDays,
	)

	return alerts_async.NewAlert(
		alerts_async.Unknown,
		alerts_async.WithEntity(entity),
		alerts_async.WithCriticality(alerts_async.Sever),
		alerts_async.WithFunctionalityType(alerts_async.DeploymentStatus),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DGRW10001, "Sandbox pipeline guardrail"),
		alerts_async.WithAction("Re-enable the pipeline only if it is still required."),
	)
}

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
