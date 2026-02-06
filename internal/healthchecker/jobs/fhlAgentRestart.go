package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

type AgentData struct {
	SourceIds  []string
	TenantId   string
	AgentId    []string
	TenantName string
}

var agentData = []AgentData{
	{
		SourceIds: []string{
			"63d9f8d0-6bf0-4fbd-b0d0-c8dc3e9f11ec",
			"de1344b6-07e5-4f06-b74d-4bb02665bad7",
			"057a52f2-c8e1-4773-9de1-f22a2eac451c"},
		TenantId:   "59876413-3295-44a0-9fd6-251aecfda9f5",
		AgentId:    []string{"a648dbc1-c493-49bd-ba76-3382803aaddc"},
		TenantName: "FHL",
	},
	{
		SourceIds: []string{
			"e16d0c1f-069c-4e83-94db-0c09499781bb",
			"85f6e763-1ce1-48c8-9e50-4c3635498a66"},
		TenantId: "bd1d99b5-1049-4fab-b5ad-0dac8474d551",
		AgentId: []string{
			"f1467eeb-e101-4df8-95f3-f6b25dcaa7a3",
			"a6221fc7-e540-4379-9c18-c17fb461cb14",
		},
		TenantName: "Element",
	},
	{
		SourceIds: []string{
			"5aa09703-7778-4892-9b63-15ac9a689881",
		},
		TenantId: "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId: []string{"2d9bd8f9-83b0-4b1d-b4ed-38a990f5f5f9",
			"3c0545ad-3d32-43bd-aacd-02590a98c8c1"},
		TenantName: "CSL-mel server",
	},
	{
		SourceIds: []string{
			"e68d191d-3ebc-425e-a151-17bd20da7d0a",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"57e71b3d-ab00-4000-830e-1ad03ff8b784"},
		TenantName: "CSL-mbr server",
	},
	{
		SourceIds: []string{
			"09410d22-9007-437b-bd85-87cbd15be003",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"5016c3fe-ba07-4b38-a1fa-a14328d99a7d"},
		TenantName: "CSL-kan server",
	},
	{
		SourceIds: []string{
			"ac48ddb0-72cb-41d5-a6c9-cd57deb43add",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"03efaf03-7a47-4fa8-97a0-2587c130a149"},
		TenantName: "CSL-pkv server",
	},
	{
		SourceIds: []string{
			"c9b663f6-e117-4e93-a5d0-f586c7060907",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"9099d799-d932-486f-b5d1-4f370b7f8005"},
		TenantName: "CSL-brn server",
	},
	{
		SourceIds: []string{
			"8cc105d6-0dea-4eb9-8257-a9e0dc0104bf",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"a209c23e-33a7-4cf9-927a-9cb84ff10626"},
		TenantName: "CSL-kop server",
	},
	{
		SourceIds: []string{
			"b26920b7-ec2a-48b2-9869-bf3213c8582e",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"31fbc859-a847-4be2-add5-f1d155d7cd8b"},
		TenantName: "CSL-nut server",
	},
	{
		SourceIds: []string{
			"3df30b13-8bf2-4581-8e82-892c582de26c",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"9cce036e-a7da-4552-9029-355f8462d265"},
		TenantName: "CSL-hsp server",
	},
	{
		SourceIds: []string{
			"b29bdee5-6b40-4578-ae2d-1af2c18998dd",
		},
		TenantId:   "15ec2c52-c786-400f-b312-23f7c5d22f09",
		AgentId:    []string{"d2d9e140-065a-45e2-ba6e-68821b263086"},
		TenantName: "CSL-lvp server",
	},
	{
		SourceIds: []string{
			"dccf368f-0e71-445c-afa5-d815eab44da7", // windows-security-events
			"46db6aa9-f21c-48e9-9035-b77b8b7fc2da", // windows-application-events
			"3e306501-a862-4948-82cf-78af879465bf", // windows-system-events
			"12e26c20-728b-46d1-a7ee-80e905dcd5da", // windows-powershell-events
			"ffc48446-bdb3-4366-b26a-560a524d4c62", // windows-sysmon-events
		},
		TenantId: "e3d5fa7e-e232-4be2-895e-9125c5b753ff",
		AgentId: []string{
			"f58cc826-3393-4c6a-9d9f-cf70d9dc7111", // wec1
			"c3ad5714-b748-4f2a-b435-5d35e327c7b3", // wec2
			"4a46ecb8-d5b9-4bd4-a7f5-0869bd278d74", // wec3
		},
		TenantName: "Centene",
	},
}

func CheckAndRestartFHLAgent(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	osClient := os.GetClient()
	db := config.GetDB()

	for _, agentData := range agentData {
		tenantId := agentData.TenantId
		sourceIds := agentData.SourceIds
		agentIds := agentData.AgentId
		tenantName := agentData.TenantName
		logging.GetLogger().Info("Checking for agent", zap.String("tenantName", tenantName), zap.Any("agentIds", agentIds), zap.Any("sourceIds", sourceIds))

		interval := 20
		oneInactiveSource := false
		// Get last event times for all specified sources
		sourceIdToLastEventTime, err := getSourceIdToLastEventTime(ctx, tenantId, osClient)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting sourceIdToLastEventTime for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logging.GetLoggerWithContext(ctx).Error("error getting sourceIdToLastEventTime", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		// Check if any source haven't sent data for more than 20 minutes
		currentTime := time.Now().UTC()
		thresholdTime := currentTime.Add(-time.Duration(interval) * time.Minute)

		logging.GetLoggerWithContext(ctx).Info("Checking source activity",
			zap.Any("agentIds", agentIds),
			zap.String("tenantName", tenantName),
			zap.Time("thresholdTime", thresholdTime))

		for _, sourceId := range sourceIds {
			lastEventTime, ok := sourceIdToLastEventTime[sourceId]
			if !ok {
				logging.GetLoggerWithContext(ctx).Error("sourceId not found in sourceIdToLastEventTime", zap.String("sourceId", sourceId))
				oneInactiveSource = true
				logging.GetLoggerWithContext(ctx).Info("Source is not active - has sent data within threshold time")
				break
			}

			logger := logging.GetLoggerWithContext(ctx).With(zap.String("sourceId", sourceId), zap.Time("lastEventTime", lastEventTime))
			logger.Info("Checking last event time for source")

			// If any source has sent data within the last 20 minutes, mark as active
			if !lastEventTime.After(thresholdTime) {
				oneInactiveSource = true
				logger.Info("Source is not active - has sent data within threshold time")
				break
			}
		}

		// If any sources have not sent data for more than 20 minutes, update all agents at once
		if oneInactiveSource {
			logging.GetLoggerWithContext(ctx).Info("sources inactive for more than 20 minutes, updating agent upgrade availability",
				zap.Any("agentIds", agentIds),
				zap.String("tenantName", tenantName))

			// Convert agentIds to UUIDs for the query
			var agentUUIDs []uuid.UUID
			for _, agentId := range agentIds {
				if agentId != "" { // Skip empty agent IDs
					agentUUID := utils.UUIDFromStringOrNil(agentId)
					agentUUIDs = append(agentUUIDs, agentUUID)
				}
			}

			if len(agentUUIDs) == 0 {
				logging.GetLoggerWithContext(ctx).Warn("No valid agent IDs found to update",
					zap.String("tenantName", tenantName))
				continue
			}

			tenantUUID := utils.UUIDFromStringOrNil(tenantId)

			// Update all agents for this tenant at once using IN clause
			result := db.Model(&agent.Agent{}).
				Where("id IN ? AND tenant_id = ?", agentUUIDs, tenantUUID).
				Update("is_upgrade_available", true)

			if result.Error != nil {
				errorMsg := fmt.Sprintf("error updating agents is_upgrade_available for tenant %s: %v", tenantId, result.Error)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logging.GetLoggerWithContext(ctx).Error("error updating agents is_upgrade_available",
					zap.Error(result.Error),
					zap.Any("agentIds", agentIds),
					zap.String("tenantName", tenantName))
			}

			logging.GetLoggerWithContext(ctx).Info("Successfully updated agents is_upgrade_available to true",
				zap.Any("agentIds", agentIds),
				zap.String("tenantName", tenantName),
				zap.Int64("affectedRows", result.RowsAffected))
		} else {
			logging.GetLoggerWithContext(ctx).Info("Some sources are still active, not updating agent upgrade availability",
				zap.Any("agentIds", agentIds),
				zap.String("tenantName", tenantName))
		}
	}

	if len(jobErrors) == 0 {
		logging.GetLoggerWithContext(ctx).Info("successfully completed FHL agent check and restart")
		return common.NewJobResultSuccess()
	} else {
		logging.GetLoggerWithContext(ctx).Info("FHL agent check and restart completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func getSourceIdToLastEventTime(ctx context.Context, tenantId string, osClient *opensearch.Client) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	sourceIdToLastEventTime := make(map[string]time.Time)
	var after map[string]any = nil
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, statsAlias, q, []string{"tags.db_event_source_id.keyword"}, []os.AggregationFunction{aggFunc}, after)
		if err != nil {
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			if sourceId, ok := response.Key["tags.db_event_source_id.keyword"].(string); ok {
				if lastEventTime, ok := response.Values["last_event_time"].(float64); ok {
					sourceIdToLastEventTime[sourceId] = time.Unix(int64(lastEventTime)/1000, 0).UTC()
				}
			}
		}
		after = newAfter
	}
	return sourceIdToLastEventTime, nil
}
