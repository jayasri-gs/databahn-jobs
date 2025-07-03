package jobs

import (
	"context"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func CheckAndRestartFHLAgent(ctx context.Context) {
	osClient := os.GetClient()
	db := config.GetDB()

	logging.GetLogger().Info("CheckAndRestartFHLAgent")
	var sourceIds = []string{
		"63d9f8d0-6bf0-4fbd-b0d0-c8dc3e9f11ec",
		"de1344b6-07e5-4f06-b74d-4bb02665bad7",
		"057a52f2-c8e1-4773-9de1-f22a2eac451c",
	}
	var tenantId = "59876413-3295-44a0-9fd6-251aecfda9f5"
	var agentId = "a648dbc1-c493-49bd-ba76-3382803aaddc"
	interval := 20
	allSourcesInactive := false
	// Get last event times for all specified sources
	sourceIdToLastEventTime, err := getSourceIdToLastEventTimeNew(ctx, osClient, tenantId, interval, sourceIds)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error getting sourceIdToLastEventTime", zap.Error(err))
		return
	}

	// Check if all sources haven't sent data for more than 20 minutes
	currentTime := time.Now().UTC()
	thresholdTime := currentTime.Add(-time.Duration(interval) * time.Minute)

	logging.GetLoggerWithContext(ctx).Info("Checking source activity",
		zap.String("agentId", agentId),
		zap.String("tenantId", tenantId),
		zap.Time("thresholdTime", thresholdTime))

	for sourceId, lastEventTime := range sourceIdToLastEventTime {
		logger := logging.GetLoggerWithContext(ctx).With(zap.String("sourceId", sourceId), zap.Time("lastEventTime", lastEventTime))
		logger.Info("Checking last event time for source")

		// If any source has sent data within the last 20 minutes, mark as active
		if !lastEventTime.After(thresholdTime) {
			allSourcesInactive = true
			logger.Info("Source is not active - has sent data within threshold time")
			break
		}
	}

	// If no sources have sent data for more than 20 minutes, update the agent
	if allSourcesInactive {
		logging.GetLoggerWithContext(ctx).Info("All sources inactive for more than 20 minutes, updating agent upgrade availability",
			zap.String("agentId", agentId),
			zap.String("tenantId", tenantId))

		// Update the agent_node table to set is_upgrade_available to true
		agentUUID := utils.UUIDFromStringOrNil(agentId)
		tenantUUID := utils.UUIDFromStringOrNil(tenantId)

		// First, verify the agent exists and belongs to the specified tenant
		var agentRecord agent.Agent
		err = db.Where("id = ? AND tenant_id = ?", agentUUID, tenantUUID).First(&agentRecord).Error
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error finding agent",
				zap.Error(err),
				zap.String("agentId", agentId),
				zap.String("tenantId", tenantId))
			return
		}

		// Update the is_upgrade_available field to true
		// Note: This assumes the field exists in the database. If it doesn't exist in the Go model,
		// we'll need to add it to the Agent struct or use a raw SQL query
		err = db.Model(&agent.Agent{}).
			Where("id = ? AND tenant_id = ?", agentUUID, tenantUUID).
			Update("is_upgrade_available", true).Error

		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating agent is_upgrade_available",
				zap.Error(err),
				zap.String("agentId", agentId),
				zap.String("tenantId", tenantId))
			return
		}

		logging.GetLoggerWithContext(ctx).Info("Successfully updated agent is_upgrade_available to true",
			zap.String("agentId", agentId),
			zap.String("tenantId", tenantId),
			zap.String("agentName", agentRecord.Name))
	} else {
		logging.GetLoggerWithContext(ctx).Info("Some sources are still active, not updating agent upgrade availability",
			zap.String("agentId", agentId),
			zap.String("tenantId", tenantId))
	}
}
