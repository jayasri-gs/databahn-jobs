package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func findInactiveAgents(db *gorm.DB, healthCheckTime time.Time, page, pageSize int) ([]*model.UnhealthyAgent, error) {
	var agents []agent.Agent
	offset := page * pageSize
	err := db.Table("agents").
		Where("heartbeat_at < ? AND status != ?", healthCheckTime, healthchecker.StatusCreated).
		Limit(pageSize).
		Offset(offset).
		Find(&agents).Error
	if err != nil {
		return nil, err
	}
	var result []*model.UnhealthyAgent
	for _, a := range agents {
		result = append(result, &model.UnhealthyAgent{
			Agent: &a,
		})
	}
	return result, nil
}

func sendAgentInAppAlerts(inactiveAgents []*model.UnhealthyAgent, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(inactiveAgents))
	for i, ia := range inactiveAgents {
		newAlert, err := buildAgentAlert(*ia)
		if err != nil {
			logger.GetLogger().Error("error while building agent alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	return alertsManager.SendAlerts(alertsToSave)
}

func AlertForUnhealthyAgents(ctx context.Context) error {
	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString("AGENT_HEALTH_CHECK_TIME")))
	page, pageSize := 0, 50
	for {
		inactiveAgents, err := findInactiveAgents(db, healthCheckTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive agents", zap.Error(err))
			return err
		}
		if len(inactiveAgents) == 0 {
			logger.GetLogger().Info("no more inactive agents found", zap.Int("page", page))
		} else {
			err := sendAgentInAppAlerts(inactiveAgents, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending agent in-app alerts", zap.Error(err))
				return err
			}
		}
		page++
	}
	return nil
}

func buildAgentAlert(ia model.UnhealthyAgent) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.AgentHealthCheckerFunctionalityTitle, ia.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.CloudLogSource,
		alerts_async.WithEntity(ia),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}
