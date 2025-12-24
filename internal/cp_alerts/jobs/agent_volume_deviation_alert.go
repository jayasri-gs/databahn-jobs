package jobs

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	cpcommon "github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	DefaultAgentBatchPageSize = 100 // DefaultAgentBatchPageSize is the default page size for fetching agents from database
	DefaultWorkerPoolSize     = 10  // DefaultWorkerPoolSize is the default number of workers for processing agents
	DefaultWorkerQueueSize    = 100 // DefaultWorkerQueueSize is the default queue size for worker pool
)

// AgentVolumeDeviationTask represents a task for processing agent volume deviation
type AgentVolumeDeviationTask struct {
	Agent     *agent.Agent
	TenantId  uuid.UUID
	Config    *util.AgentVolumeDeviationConfig
	DateRange *util.VolumeDeviationDateRange
}

// AgentVolumeDeviationJobSetup holds all setup components for the agent volume deviation job
type AgentVolumeDeviationJobSetup struct {
	AlertsManager      *alert.AlertsManager
	TaskChan           chan AgentVolumeDeviationTask
	AlertsChan         chan []*alerts_async.Alert
	TotalAlertsSent    *int64 // Atomic counter for total alerts sent
	WorkerWg           *sync.WaitGroup
	AlertsWg           *sync.WaitGroup
	DB                 *gorm.DB
	DateRange          *util.VolumeDeviationDateRange
	TenantConfigCache  map[uuid.UUID]*entities.EntityAlertsConfig
	WorkerPoolSize     int
	WorkerQueueSize    int
	AgentBatchPageSize int
}

// AgentVolumeStats represents the volume statistics for an agent
type AgentVolumeStats struct {
	AgentId                   uuid.UUID
	TenantId                  uuid.UUID
	CurrentDayVolume          float64 // Last 24 hours (same as current week same day)
	YesterdayVolume           float64 // Previous 24 hours
	PreviousWeekSameDayVolume float64 // Same day from previous week
}

// setupAgentVolumeDeviationJob initializes all components needed for the agent volume deviation job
func setupAgentVolumeDeviationJob(ctx context.Context) (*AgentVolumeDeviationJobSetup, func(), error) {
	// Load configuration from environment variables
	workerPoolSize := utils.GetEnvInt("AGENT_VOLUME_DEVIATION_WORKER_POOL_SIZE", DefaultWorkerPoolSize)
	workerQueueSize := utils.GetEnvInt("AGENT_VOLUME_DEVIATION_WORKER_QUEUE_SIZE", DefaultWorkerQueueSize)
	agentBatchPageSize := utils.GetEnvInt("AGENT_VOLUME_DEVIATION_BATCH_PAGE_SIZE", DefaultAgentBatchPageSize)

	logger.GetLoggerWithContext(ctx).Info("Agent volume deviation alert configuration loaded",
		zap.Int("worker_pool_size", workerPoolSize),
		zap.Int("worker_queue_size", workerQueueSize),
		zap.Int("agent_batch_page_size", agentBatchPageSize),
	)

	db := config.GetDB()

	dateRange := util.CalculateVolumeDeviationDateRange()
	logger.GetLoggerWithContext(ctx).Info("Date ranges calculated for agent volume deviation analysis",
		zap.String("current_day_start", dateRange.CurrentDayStart.Format("2006-01-02 15:04:05")),
		zap.String("current_day_end", dateRange.CurrentDayEnd.Format("2006-01-02 15:04:05")),
		zap.String("yesterday_start", dateRange.YesterdayStart.Format("2006-01-02 15:04:05")),
		zap.String("yesterday_end", dateRange.YesterdayEnd.Format("2006-01-02 15:04:05")),
		zap.String("previous_week_same_day_start", dateRange.PreviousWeekSameDayStart.Format("2006-01-02 15:04:05")),
		zap.String("previous_week_same_day_end", dateRange.PreviousWeekSameDayEnd.Format("2006-01-02 15:04:05")))

	// Cache for tenant-level configs to avoid redundant DB queries across batches
	tenantConfigCache := make(map[uuid.UUID]*entities.EntityAlertsConfig)

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return nil, nil, fmt.Errorf("error getting alerts manager: %w", err)
	}

	// Cleanup function
	cleanup := func() {
		alertsManager.Close(ctx)
	}

	// Channel to collect alerts from workers
	alertsChan := make(chan []*alerts_async.Alert, workerQueueSize)
	alertsWg := &sync.WaitGroup{}
	var totalAlertsSent int64 // Use atomic for thread-safe counting

	// Start alert collector goroutine that publishes alerts to kafka
	alertsWg.Add(1)
	go func() {
		defer alertsWg.Done()
		for alerts := range alertsChan {
			if len(alerts) > 0 {
				if err := alertsManager.SendAlerts(alerts); err != nil {
					logger.GetLoggerWithContext(ctx).Error("Error sending alerts batch", zap.Error(err), zap.Int("batch_size", len(alerts)))
				} else {
					atomic.AddInt64(&totalAlertsSent, int64(len(alerts)))
					logger.GetLoggerWithContext(ctx).Debug("Sent alerts batch", zap.Int("batch_size", len(alerts)))
				}
			}
		}
	}()

	// Initialize worker pool using goroutines and channels
	taskChan := make(chan AgentVolumeDeviationTask, workerQueueSize)
	workerWg := &sync.WaitGroup{}

	// Start workers
	for i := 0; i < workerPoolSize; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			logger.GetLoggerWithContext(ctx).Debug("Worker started", zap.Int("worker_id", workerID))
			for task := range taskChan {
				processAgentVolumeDeviation(ctx, task, alertsChan)
			}
			logger.GetLoggerWithContext(ctx).Debug("Worker stopped", zap.Int("worker_id", workerID))
		}(i)
	}

	return &AgentVolumeDeviationJobSetup{
		AlertsManager:      alertsManager,
		TaskChan:           taskChan,
		AlertsChan:         alertsChan,
		TotalAlertsSent:    &totalAlertsSent,
		WorkerWg:           workerWg,
		AlertsWg:           alertsWg,
		DB:                 db,
		DateRange:          dateRange,
		TenantConfigCache:  tenantConfigCache,
		WorkerPoolSize:     workerPoolSize,
		WorkerQueueSize:    workerQueueSize,
		AgentBatchPageSize: agentBatchPageSize,
	}, cleanup, nil
}

// SendAlertForAgentVolumeDeviation generates alerts when there are significant deviations in agent ingestion volume
func SendAlertForAgentVolumeDeviation(ctx context.Context) cpcommon.JobResult {
	var jobErrors []cpcommon.JobError

	setup, cleanup, err := setupAgentVolumeDeviationJob(ctx)
	if err != nil {
		jobErrors = append(jobErrors, cpcommon.JobError{Message: err.Error()})
		return cpcommon.NewJobResultFromErrors(jobErrors)
	}
	defer cleanup()

	// Fetch agents in batches with pagination
	page := 0
	totalTasksProcessed := 0

agentFetchLoop:
	for {
		agents, err := getAgentsWithRecentHeartbeat(ctx, setup.DB, page, setup.AgentBatchPageSize)
		if err != nil {
			errorMsg := fmt.Sprintf("error fetching agents batch (page %d): %v", page, err)
			jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("Error fetching agents batch", zap.Error(err), zap.Int("page", page))
			break
		}

		if len(agents) == 0 {
			logger.GetLoggerWithContext(ctx).Info("No more agents to process", zap.Int("page", page))
			break
		}

		// Filter out agents that are less than 2 weeks old, we need at least 2 weeks of data for volume deviation analysis
		twoWeeksAgo := time.Now().UTC().Add(-14 * 24 * time.Hour)
		var filteredAgents []*agent.Agent
		var skippedCount int
		for _, a := range agents {
			if a.CreatedAt.After(twoWeeksAgo) {
				logger.GetLoggerWithContext(ctx).Debug("Agent is less than 2 weeks old, skipping volume deviation check",
					zap.String("agent_id", a.ID.String()),
					zap.String("tenant_id", a.TenantId.String()),
					zap.Time("created_at", a.CreatedAt),
					zap.Time("two_weeks_ago", twoWeeksAgo))
				skippedCount++
				continue
			}
			filteredAgents = append(filteredAgents, a)
		}

		if len(filteredAgents) == 0 {
			logger.GetLoggerWithContext(ctx).Info("All agents in batch are less than 2 weeks old, skipping batch",
				zap.Int("page", page),
				zap.Int("skipped_count", skippedCount))
			page++
			continue
		}

		logger.GetLoggerWithContext(ctx).Info("Processing agent batch",
			zap.Int("page", page),
			zap.Int("batch_size", len(agents)),
			zap.Int("filtered_batch_size", len(filteredAgents)),
			zap.Int("skipped_count", skippedCount))

		// Build agent to tenant map for batch config resolution
		agentToTenantMap := make(map[uuid.UUID]uuid.UUID)
		for _, a := range filteredAgents {
			agentToTenantMap[a.ID] = a.TenantId
		}

		// Batch resolve configurations for all agents in this batch
		agentConfigs, err := util.ResolveAgentVolumeDeviationConfigsBatch(setup.DB, agentToTenantMap, setup.TenantConfigCache)
		if err != nil {
			errorMsg := fmt.Sprintf("error resolving agent volume deviation configs for batch (page %d): %v", page, err)
			jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("Error resolving agent volume deviation configs",
				zap.Error(err), zap.Int("page", page))
			page++
			continue
		}

		// Submit agents to worker pool, only agents with alerts enabled are in agentConfigs map (disabled agents are filtered out)
		for _, a := range filteredAgents {
			agentConfig, ok := agentConfigs[a.ID]
			if !ok {
				logger.GetLoggerWithContext(ctx).Debug("Agent volume deviation alert is disabled or no config, skipping",
					zap.String("agent_id", a.ID.String()),
					zap.String("tenant_id", a.TenantId.String()))
				continue
			}

			task := AgentVolumeDeviationTask{
				Agent:     a,
				TenantId:  a.TenantId,
				Config:    agentConfig,
				DateRange: setup.DateRange,
			}
			select {
			case setup.TaskChan <- task:
				totalTasksProcessed++
			case <-ctx.Done():
				errorMsg := fmt.Sprintf("context cancelled while submitting agent task for agent %s", a.ID.String())
				jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("Context cancelled while submitting agent task",
					zap.String("agent_id", a.ID.String()))
				break agentFetchLoop
			}
		}

		page++
	}

	// Close task channel and wait for all workers to complete
	close(setup.TaskChan)
	setup.WorkerWg.Wait()
	close(setup.AlertsChan)
	setup.AlertsWg.Wait()

	logger.GetLoggerWithContext(ctx).Info("Agent volume deviation processing completed",
		zap.Int("total_tasks_processed", totalTasksProcessed),
		zap.Int64("total_alerts_sent", atomic.LoadInt64(setup.TotalAlertsSent)))

	if len(jobErrors) > 0 {
		return cpcommon.NewJobResultFromErrors(jobErrors)
	}

	return cpcommon.NewJobResultSuccess()
}

// getAgentsWithRecentHeartbeat fetches agents with heartbeat in the last 60 minutes
func getAgentsWithRecentHeartbeat(ctx context.Context, db *gorm.DB, page, pageSize int) ([]*agent.Agent, error) {
	var agents []*agent.Agent

	cutoffTime := time.Now().UTC().Add(-60 * time.Minute)

	offset := page * pageSize

	err := db.WithContext(ctx).
		Where("heartbeat_at >= ?", cutoffTime).
		Order("id ASC").
		Limit(pageSize).
		Offset(offset).
		Find(&agents).Error

	if err != nil {
		return nil, fmt.Errorf("error fetching agents: %w", err)
	}

	return agents, nil
}

// processAgentVolumeDeviation checks for volume deviation for a single agent
func processAgentVolumeDeviation(
	ctx context.Context,
	task AgentVolumeDeviationTask,
	alertsChan chan<- []*alerts_async.Alert,
) {
	agentId := task.Agent.ID
	tenantId := task.TenantId

	logger.GetLoggerWithContext(ctx).Debug("Processing agent volume deviation",
		zap.String("agent_id", agentId.String()),
		zap.String("tenant_id", tenantId.String()))

	// Get agent volume stats for all time periods (current day, yesterday, previous week same day)
	stats, err := getAgentVolumeStats(ctx, agentId, tenantId, task.DateRange)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting agent volume stats",
			zap.Error(err),
			zap.String("agent_id", agentId.String()),
			zap.String("tenant_id", tenantId.String()))
		return
	}

	// Check for volume deviation in both conditions:
	// 1. Compare current day (last 24 hours) with yesterday
	// 2. Compare same day from current week with same day from previous week

	// Condition 1: Current day vs Yesterday
	yesterdayIsSpike, yesterdayIsDrop, yesterdayDeviation := util.CheckSpikeOrDrop(
		stats.CurrentDayVolume,
		stats.YesterdayVolume,
		task.Config.PercentageIncreaseThreshold,
		task.Config.PercentageDecreaseThreshold,
	)

	// Condition 2: Same day current week vs previous week
	weekIsSpike, weekIsDrop, weekDeviation := util.CheckSpikeOrDrop(
		stats.CurrentDayVolume,
		stats.PreviousWeekSameDayVolume,
		task.Config.PercentageIncreaseThreshold,
		task.Config.PercentageDecreaseThreshold,
	)

	// Skip alerts for spike from zero (0 to non-zero), this typically indicates recovery:
	if stats.YesterdayVolume == 0 && stats.CurrentDayVolume > 0 {
		logger.GetLoggerWithContext(ctx).Debug("Skipping alert for spike from zero (recovery scenario)",
			zap.String("agent_id", agentId.String()),
			zap.Float64("current_volume", stats.CurrentDayVolume),
			zap.Float64("yesterday_volume", stats.YesterdayVolume),
			zap.Float64("previous_week_volume", stats.PreviousWeekSameDayVolume))
		return
	}

	// Apply minimum volume threshold check:
	// - For spikes: only alert if current volume is above threshold (meaningful spike)
	// - For drops: alert if previous volume was significant, even if current drops below threshold (significant drop)
	if task.Config.MinimumVolumeThreshold > 0 {
		// Check if we have a spike scenario
		if yesterdayIsSpike && weekIsSpike {
			// For spikes, current volume must be above threshold
			if stats.CurrentDayVolume < task.Config.MinimumVolumeThreshold {
				logger.GetLoggerWithContext(ctx).Debug("Agent volume spike below minimum threshold, skipping alert",
					zap.String("agent_id", agentId.String()),
					zap.Float64("current_volume", stats.CurrentDayVolume),
					zap.Float64("minimum_threshold", task.Config.MinimumVolumeThreshold))
				return
			}
		}
		// For drops, check if previous volumes were significant (indicating a meaningful drop)
		if yesterdayIsDrop && weekIsDrop {
			// Check if either previous volume was above threshold (significant drop from meaningful volume)
			previousVolumeSignificant := stats.YesterdayVolume >= task.Config.MinimumVolumeThreshold ||
				stats.PreviousWeekSameDayVolume >= task.Config.MinimumVolumeThreshold
			if !previousVolumeSignificant {
				logger.GetLoggerWithContext(ctx).Debug("Agent volume drop from below minimum threshold, skipping alert",
					zap.String("agent_id", agentId.String()),
					zap.Float64("current_volume", stats.CurrentDayVolume),
					zap.Float64("yesterday_volume", stats.YesterdayVolume),
					zap.Float64("previous_week_volume", stats.PreviousWeekSameDayVolume),
					zap.Float64("minimum_threshold", task.Config.MinimumVolumeThreshold))
				return
			}
		}
	}

	// Generate alert only when both conditions match
	var alert *alerts_async.Alert
	var alertErr error

	if (yesterdayIsSpike && weekIsSpike) || (yesterdayIsDrop && weekIsDrop) {
		alert, alertErr = buildAgentVolumeDeviationAlert(
			ctx,
			task.Agent,
			stats,
			task.DateRange,
			yesterdayIsSpike,
			math.Max(math.Abs(yesterdayDeviation), math.Abs(weekDeviation)),
			alerts_async.Critical,
		)
	}

	if alertErr != nil {
		logger.GetLoggerWithContext(ctx).Error("Error creating agent volume deviation alert",
			zap.Error(alertErr),
			zap.String("agent_id", agentId.String()))
		return
	}

	if alert != nil {
		alertsChan <- []*alerts_async.Alert{alert}
		logger.GetLoggerWithContext(ctx).Info("Generated agent volume deviation alert",
			zap.String("agent_id", agentId.String()),
			zap.String("tenant_id", tenantId.String()),
			zap.String("criticality", fmt.Sprintf("%v", alert.Criticality)))
	}
}

// getAgentVolumeStats queries OpenSearch for agent volume statistics
func getAgentVolumeStats(
	ctx context.Context,
	agentId, tenantId uuid.UUID,
	dateRange *util.VolumeDeviationDateRange,
) (*AgentVolumeStats, error) {
	stats := &AgentVolumeStats{
		AgentId:  agentId,
		TenantId: tenantId,
	}

	// Query current day volume (used for both comparisons)
	currentDayVolume, err := queryAgentVolume(
		ctx,
		agentId,
		tenantId,
		dateRange.CurrentDayStart,
		dateRange.CurrentDayEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying current day volume: %w", err)
	}
	stats.CurrentDayVolume = currentDayVolume

	// Query yesterday volume (for Condition 1: current day vs yesterday)
	yesterdayVolume, err := queryAgentVolume(
		ctx,
		agentId,
		tenantId,
		dateRange.YesterdayStart,
		dateRange.YesterdayEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying yesterday volume: %w", err)
	}
	stats.YesterdayVolume = yesterdayVolume

	// Query previous week same day volume (for Condition 2: current day vs previous week same day)
	previousWeekSameDayVolume, err := queryAgentVolume(
		ctx,
		agentId,
		tenantId,
		dateRange.PreviousWeekSameDayStart,
		dateRange.PreviousWeekSameDayEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying previous week same day volume: %w", err)
	}
	stats.PreviousWeekSameDayVolume = previousWeekSameDayVolume

	return stats, nil
}

// queryAgentVolume queries OpenSearch for agent volume
func queryAgentVolume(
	ctx context.Context,
	agentId, tenantId uuid.UUID,
	startTime, endTime time.Time,
) (float64, error) {
	query := fmt.Sprintf(`tags.component_name:"agent-ingestion" AND name:"total_data_received" AND tags.db_agent_id:%q`, agentId.String())

	startTimeStr := fmt.Sprintf("%d", startTime.UnixMilli())
	endTimeStr := fmt.Sprintf("%d", endTime.UnixMilli())

	// Query db_statistics_agent data stream
	response, err := statistics.GetAgentStatsSum(ctx, query, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error querying OpenSearch for agent volume",
			zap.Error(err),
			zap.String("agent_id", agentId.String()),
			zap.String("query", query))
		return 0, fmt.Errorf("error querying OpenSearch: %w", err)
	}

	return response.Sum, nil
}

// buildAgentVolumeDeviationAlert builds an alert for agent volume deviation
func buildAgentVolumeDeviationAlert(
	ctx context.Context,
	agent *agent.Agent,
	stats *AgentVolumeStats,
	dateRange *util.VolumeDeviationDateRange,
	isSpike bool,
	deviationPercent float64,
	criticality alerts_async.Criticality,
) (*alerts_async.Alert, error) {
	direction := "decrease"
	if isSpike {
		direction = "increase"
	}

	severityText := "Critical"

	title := fmt.Sprintf("Agent Volume %s Alert: %.1f%% %s",
		severityText,
		deviationPercent,
		direction)

	// Format dates in human-readable format (YYYY-MM-DD)
	currentDayDate := dateRange.CurrentDayStart.Format("2006-01-02")
	previousDayDate := dateRange.YesterdayStart.Format("2006-01-02")
	currentWeekSameDayDate := dateRange.CurrentDayStart.Format("2006-01-02")
	previousWeekSameDayDate := dateRange.PreviousWeekSameDayStart.Format("2006-01-02")

	// Build detailed message with date ranges (current day, yesterday, current week same day, previous week same day)
	message := fmt.Sprintf(
		"Agent '%s' (ID: %s) shows a %.1f%% %s in volume. "+
			"Current day (%s): %s, Previous day (%s): %s. "+
			"Current week same day (%s): %s, Previous week same day (%s): %s. "+
			"Alert severity: %s.",
		agent.Name,
		agent.ID.String(),
		deviationPercent,
		direction,
		currentDayDate,
		util.HumanReadableBytes(int64(stats.CurrentDayVolume)),
		previousDayDate,
		util.HumanReadableBytes(int64(stats.YesterdayVolume)),
		currentWeekSameDayDate,
		util.HumanReadableBytes(int64(stats.CurrentDayVolume)),
		previousWeekSameDayDate,
		util.HumanReadableBytes(int64(stats.PreviousWeekSameDayVolume)),
		severityText,
	)

	alert, err := alerts_async.NewAlert(
		alerts_async.Agent,
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithCriticality(criticality),
		alerts_async.WithFunctionalityType(alerts_async.DataRatioAlertChecker),
		alerts_async.WithEntityDetails("agent", agent.ID.String(), agent.DataPlaneId.String(), stats.TenantId.String()),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "Unusual agent log volume deviation detected."),
		alerts_async.WithAction("Please check agent configuration, network connectivity, source data availability, or potential security incidents."),
	)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error creating agent volume deviation alert",
			zap.Error(err),
			zap.String("agent_id", agent.ID.String()))
		return nil, err
	}

	return alert, nil
}
