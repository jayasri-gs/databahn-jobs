package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdos "os"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/db-models/rule"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// VCRuleStatusActive represents active VC rule status (database stores as string)
	VCRuleStatusActive = "ACTIVE"
	// DROP action type for volume control rules
	VCRuleActionTypeDrop = "DROP"
	// Default thresholds (can be overridden by environment variables)
	DefaultDropRuleIncreaseThreshold          = 50.0
	DefaultPipelineDataReductionThreshold     = 50.0
	DefaultUnmatchedNoRouteProcessorThreshold = 20.0
	// Minimum events threshold to avoid noise from low-traffic sources
	MinimumEventsThreshold = 1000
	// Default behavior: false means production mode (n-1 and n-2), true means testing mode (n and n-1)
	DefaultUseCurrentDay = false
)

// VCRuleQueryResult represents a VC rule with status as string (matching database schema)
type VCRuleQueryResult struct {
	rule.Rule
	Status string `gorm:"column:status"` // Override the Status field to be string
}

// getEnvFloat gets a float value from environment variable with a default value
func getEnvFloat(key string, defaultValue float64) float64 {
	envValue := stdos.Getenv(key)
	if envValue == "" {
		return defaultValue
	}

	floatValue, err := strconv.ParseFloat(envValue, 64)
	if err != nil {
		logger.GetLogger().Error("Failed to parse float environment variable, using default",
			zap.String("key", key), zap.String("value", envValue), zap.Float64("default", defaultValue), zap.Error(err))
		return defaultValue
	}

	return floatValue
}

// getEnvBool gets a boolean value from environment variable with a default value
func getEnvBool(key string, defaultValue bool) bool {
	envValue := stdos.Getenv(key)
	if envValue == "" {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(envValue)
	if err != nil {
		logger.GetLogger().Error("Failed to parse boolean environment variable, using default",
			zap.String("key", key), zap.String("value", envValue), zap.Bool("default", defaultValue), zap.Error(err))
		return defaultValue
	}

	return boolValue
}

// getTodayTimeRange returns start and end time for "today" based on useCurrentDay flag
// If useCurrentDay=true (testing): today = n (current day)
// If useCurrentDay=false (production): today = n-1 (yesterday, for 1AM runs analyzing previous day)
func getTodayTimeRange(useCurrentDay bool) (time.Time, time.Time) {
	now := time.Now().UTC()

	var targetDay time.Time
	if useCurrentDay {
		// Testing mode: use current day (n)
		targetDay = now
		logger.GetLogger().Info("Using current day mode for 'today'", zap.String("date", targetDay.Format("2006-01-02")))
	} else {
		// Production mode: use yesterday (n-1) - for 1AM runs analyzing previous day
		targetDay = now.AddDate(0, 0, -1)
		logger.GetLogger().Info("Using production mode for 'today' (previous day)", zap.String("date", targetDay.Format("2006-01-02")))
	}

	todayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	todayEnd := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 23, 59, 59, 999999999, time.UTC)

	return todayStart, todayEnd
}

// getYesterdayTimeRange returns start and end time for "yesterday" based on useCurrentDay flag
// If useCurrentDay=true (testing): yesterday = n-1 (actual yesterday)
// If useCurrentDay=false (production): yesterday = n-2 (day before yesterday)
func getYesterdayTimeRange(useCurrentDay bool) (time.Time, time.Time) {
	now := time.Now().UTC()

	var targetDay time.Time
	if useCurrentDay {
		// Testing mode: use actual yesterday (n-1)
		targetDay = now.AddDate(0, 0, -1)
		logger.GetLogger().Info("Using current day mode for 'yesterday'", zap.String("date", targetDay.Format("2006-01-02")))
	} else {
		// Production mode: use day before yesterday (n-2)
		targetDay = now.AddDate(0, 0, -2)
		logger.GetLogger().Info("Using production mode for 'yesterday' (day before yesterday)", zap.String("date", targetDay.Format("2006-01-02")))
	}

	yesterdayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	yesterdayEnd := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 23, 59, 59, 999999999, time.UTC)

	return yesterdayStart, yesterdayEnd
}

type VCAlertConfig struct {
	DropRuleIncreaseThreshold          float64
	PipelineDataReductionThreshold     float64
	UnmatchedNoRouteProcessorThreshold float64
	MinimumEventsThreshold             int64
	UseCurrentDay                      bool
	TodayStart                         time.Time
	TodayEnd                           time.Time
	YesterdayStart                     time.Time
	YesterdayEnd                       time.Time
}

// VCStatisticsService handles all statistics-related operations
type VCStatisticsService struct {
	ctx      context.Context
	tenantId uuid.UUID
	config   *VCAlertConfig
}

// RuleStats represents statistics for a rule
type RuleStats struct {
	Evaluated int64
	Matched   int64
}

// PipelineStats represents statistics for a pipeline
type PipelineStats struct {
	Ingested  int64
	Delivered int64
}

// AlertProcessor interface for different alert types
type AlertProcessor interface {
	ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error)
	GetAlertType() string
}

// ProcessingData contains all data needed for alert processing
type ProcessingData struct {
	Tenant       *tenant.Tenant
	Pipeline     *pipeline.Pipeline
	VCRules      []rule.Rule
	LogSources   []pipeline.PipelineLogSourceMapping
	StatsService *VCStatisticsService
	Config       *VCAlertConfig
}

type VCAlertOrchestrator struct {
	ctx           context.Context
	db            *gorm.DB
	osClient      *opensearch.Client
	alertsManager *alert.AlertsManager
	config        *VCAlertConfig
	processors    []AlertProcessor
}

// DropRuleIncreaseProcessor handles DROP rule increase alerts
type DropRuleIncreaseProcessor struct{}

// PipelineDataReductionProcessor handles pipeline data reduction alerts
type PipelineDataReductionProcessor struct{}

// UnmatchedNoRouteProcessor handles unmatched no route processor alerts
type UnmatchedNoRouteProcessor struct {
	db *gorm.DB
}

// ================================
// Configuration Management
// ================================

func NewVCAlertConfig() *VCAlertConfig {
	dropRuleIncreaseThreshold := getEnvFloat("VC_DROP_RULE_INCREASE_THRESHOLD", DefaultDropRuleIncreaseThreshold)
	pipelineDataReductionThreshold := getEnvFloat("VC_PIPELINE_DATA_REDUCTION_THRESHOLD", DefaultPipelineDataReductionThreshold)
	unmatchedNoRouteProcessorThreshold := getEnvFloat("VC_UNMATCHED_NO_ROUTE_PROCESSOR_THRESHOLD", DefaultUnmatchedNoRouteProcessorThreshold)
	minimumEventsThreshold := utils.GetEnvInt("VC_MINIMUM_EVENTS_THRESHOLD", MinimumEventsThreshold)
	useCurrentDay := getEnvBool("VC_USE_CURRENT_DAY", DefaultUseCurrentDay)

	todayStart, todayEnd := getTodayTimeRange(useCurrentDay)
	yesterdayStart, yesterdayEnd := getYesterdayTimeRange(useCurrentDay)

	config := &VCAlertConfig{
		DropRuleIncreaseThreshold:          dropRuleIncreaseThreshold,
		PipelineDataReductionThreshold:     pipelineDataReductionThreshold,
		UnmatchedNoRouteProcessorThreshold: unmatchedNoRouteProcessorThreshold,
		MinimumEventsThreshold:             int64(minimumEventsThreshold),
		UseCurrentDay:                      useCurrentDay,
		TodayStart:                         todayStart,
		TodayEnd:                           todayEnd,
		YesterdayStart:                     yesterdayStart,
		YesterdayEnd:                       yesterdayEnd,
	}

	logger.GetLogger().Info("Volume control alert configuration loaded",
		zap.Float64("drop_rule_increase_threshold", config.DropRuleIncreaseThreshold),
		zap.Float64("pipeline_data_reduction_threshold", config.PipelineDataReductionThreshold),
		zap.Float64("unmatched_no_route_processor_threshold", config.UnmatchedNoRouteProcessorThreshold),
		zap.Int64("minimum_events_threshold", config.MinimumEventsThreshold),
		zap.Bool("use_current_day", config.UseCurrentDay))

	return config
}

// ================================
// Statistics Service
// ================================

// NewVCStatisticsService creates a new statistics service
func NewVCStatisticsService(ctx context.Context, tenantId uuid.UUID, config *VCAlertConfig) *VCStatisticsService {
	return &VCStatisticsService{
		ctx:      ctx,
		tenantId: tenantId,
		config:   config,
	}
}

// GetRuleStats gets rule evaluation and match statistics from OpenSearch
func (s *VCStatisticsService) GetRuleStats(sourceId, ruleId string, startTime, endTime time.Time) (*RuleStats, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Get evaluated events
	evaluatedQuery := fmt.Sprintf(`name:"rule_events_evaluated" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)
	evaluatedResponse, err := statistics.GetStatsSum(s.ctx, evaluatedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return nil, fmt.Errorf("error getting evaluated events for rule %s: %w", ruleId, err)
	}

	// Get matched events
	matchedQuery := fmt.Sprintf(`name:"rule_events_matched" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)
	matchedResponse, err := statistics.GetStatsSum(s.ctx, matchedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return nil, fmt.Errorf("error getting matched events for rule %s: %w", ruleId, err)
	}

	return &RuleStats{
		Evaluated: int64(evaluatedResponse.Sum),
		Matched:   int64(matchedResponse.Sum),
	}, nil
}

// GetPipelineStats gets pipeline-level ingestion and delivery statistics from OpenSearch
func (s *VCStatisticsService) GetPipelineStats(pipelineId, logSourceId string, startTime, endTime time.Time) (*PipelineStats, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Get ingested events
	ingestedQuery := fmt.Sprintf(`name:"total_events_delivered" AND tags.component_name:"ingestion" AND tags.db_event_source_id:"%s"`, logSourceId)
	ingestedResponse, err := statistics.GetStatsSum(s.ctx, ingestedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return nil, fmt.Errorf("error getting ingested events for pipeline %s: %w", pipelineId, err)
	}

	// Get delivered events
	deliveredQuery := fmt.Sprintf(`name:"total_events_delivered" AND tags.component_name:"dispenser" AND tags.db_pipeline_id:"%s"`, pipelineId)
	deliveredResponse, err := statistics.GetStatsSum(s.ctx, deliveredQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return nil, fmt.Errorf("error getting delivered events for pipeline %s: %w", pipelineId, err)
	}

	return &PipelineStats{
		Ingested:  int64(ingestedResponse.Sum),
		Delivered: int64(deliveredResponse.Sum),
	}, nil
}

// ================================
// DROP Rule Increase Processor
// ================================

func (p *DropRuleIncreaseProcessor) GetAlertType() string {
	return "DropRuleIncrease"
}

func (p *DropRuleIncreaseProcessor) ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	logger.GetLogger().Debug("Processing DROP rule increase alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.Pipeline.ID.String()),
		zap.Int("ruleCount", len(data.VCRules)))

	for _, vcRule := range data.VCRules {
		for _, logSource := range data.LogSources {
			sourceId := logSource.LogSourceID

			// Get today's and yesterday's rule statistics
			todayStats, err := data.StatsService.GetRuleStats(sourceId.String(), vcRule.ID.String(), data.Config.TodayStart, data.Config.TodayEnd)
			if err != nil {
				logger.GetLogger().Error("error getting today's rule stats", zap.Error(err),
					zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
				continue
			}

			yesterdayStats, err := data.StatsService.GetRuleStats(sourceId.String(), vcRule.ID.String(), data.Config.YesterdayStart, data.Config.YesterdayEnd)
			if err != nil {
				logger.GetLogger().Error("error getting yesterday's rule stats", zap.Error(err),
					zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
				continue
			}

			logger.GetLogger().Debug("rule statistics",
				zap.String("ruleId", vcRule.ID.String()),
				zap.String("ruleName", vcRule.Name),
				zap.String("actionType", vcRule.ActionType),
				zap.Int64("todayEvaluated", todayStats.Evaluated),
				zap.Int64("todayMatched", todayStats.Matched),
				zap.Int64("yesterdayEvaluated", yesterdayStats.Evaluated),
				zap.Int64("yesterdayMatched", yesterdayStats.Matched))

			// Check alert conditions
			shouldAlert := p.checkDropRuleIncreaseCondition(vcRule, todayStats, yesterdayStats, data.Config)

			if shouldAlert {
				// Calculate percentages
				var matchedPercent, unmatchedPercent float64
				if todayStats.Evaluated > 0 {
					matchedPercent = (float64(todayStats.Matched) / float64(todayStats.Evaluated)) * 100
					unmatchedPercent = 100 - matchedPercent
				}

				vcAlert := model.NewVCAlert(data.Tenant, data.Pipeline, vcRule.ID, vcRule.Name, sourceId,
					model.VCAlertTypeDropRuleIncrease,
					todayStats.Matched, yesterdayStats.Matched, todayStats.Evaluated, yesterdayStats.Evaluated,
					matchedPercent, unmatchedPercent)

				alert, err := buildVCAlert(*vcAlert, data.Config)
				if err != nil {
					logger.GetLogger().Error("error building DROP rule increase alert", zap.Error(err),
						zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
					continue
				}

				alerts = append(alerts, alert)

				logger.GetLogger().Info("DROP rule increase alert created",
					zap.String("tenantId", data.Tenant.Id.String()),
					zap.String("ruleId", vcRule.ID.String()))
			} else {
				// Only auto-resolve if rule has sufficient traffic
				if todayStats.Evaluated >= data.Config.MinimumEventsThreshold {
					healthyRuleIds = append(healthyRuleIds, vcRule.ID.String())
					logger.GetLogger().Info("Rule is healthy with sufficient traffic, adding to auto-resolution list",
						zap.String("tenantId", data.Tenant.Id.String()),
						zap.String("ruleId", vcRule.ID.String()),
						zap.String("ruleName", vcRule.Name),
						zap.Int64("todayEvaluated", todayStats.Evaluated),
						zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
				}
			}
		}
	}

	return alerts, healthyRuleIds, nil
}

// checkDropRuleIncreaseCondition checks if DROP rule increase condition is met
func (p *DropRuleIncreaseProcessor) checkDropRuleIncreaseCondition(vcRule rule.Rule, todayStats, yesterdayStats *RuleStats, config *VCAlertConfig) bool {
	// Only alert if there's significant traffic to avoid noise
	if todayStats.Evaluated < config.MinimumEventsThreshold {
		logger.GetLogger().Debug("skipping alert due to low traffic",
			zap.String("ruleId", vcRule.ID.String()),
			zap.Int64("todayEvaluated", todayStats.Evaluated),
			zap.Int64("minimumThreshold", config.MinimumEventsThreshold))
		return false
	}

	// Only for DROP rules
	if strings.ToUpper(vcRule.ActionType) != VCRuleActionTypeDrop {
		return false
	}

	// Need valid data for both days
	if yesterdayStats.Matched <= 0 || todayStats.Matched <= 0 || yesterdayStats.Evaluated <= 0 || todayStats.Evaluated <= 0 {
		return false
	}

	// Calculate percentages
	yesterdayMatchPercent := float64(yesterdayStats.Matched) / float64(yesterdayStats.Evaluated) * 100
	todayMatchPercent := float64(todayStats.Matched) / float64(todayStats.Evaluated) * 100

	// Check if today's match percentage is more than configured threshold higher than yesterday's
	increasePercent := ((todayMatchPercent - yesterdayMatchPercent) / yesterdayMatchPercent) * 100

	if increasePercent > config.DropRuleIncreaseThreshold {
		logger.GetLogger().Info("DROP rule match increase detected",
			zap.String("ruleId", vcRule.ID.String()),
			zap.String("ruleName", vcRule.Name),
			zap.Float64("yesterdayMatchPercent", yesterdayMatchPercent),
			zap.Float64("todayMatchPercent", todayMatchPercent),
			zap.Float64("increasePercent", increasePercent),
			zap.Float64("threshold", config.DropRuleIncreaseThreshold))
		return true
	}

	return false
}

// ================================
// Pipeline Data Reduction Processor
// ================================

func (p *PipelineDataReductionProcessor) GetAlertType() string {
	return "PipelineDataReduction"
}

func (p *PipelineDataReductionProcessor) ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	// Only process if pipeline has rules and log sources
	if len(data.VCRules) == 0 || len(data.LogSources) == 0 {
		logger.GetLogger().Debug("skipping pipeline data reduction check - no rules or log sources",
			zap.String("pipelineId", data.Pipeline.ID.String()),
			zap.Int("rules", len(data.VCRules)),
			zap.Int("logSources", len(data.LogSources)))
		return alerts, healthyRuleIds, nil
	}

	logger.GetLogger().Debug("Processing pipeline data reduction alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.Pipeline.ID.String()))

	shouldAlert, todayIngested, todayDelivered, yesterdayIngested, yesterdayDelivered, err := p.checkPipelineDataReductionCondition(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking pipeline data reduction: %w", err)
	}

	if shouldAlert {
		vcAlert := model.NewPipelineVCAlert(data.Tenant, data.Pipeline, model.VCAlertTypePipelineDataReduction,
			todayIngested, yesterdayIngested, todayDelivered, yesterdayDelivered)

		alert, err := buildVCAlert(*vcAlert, data.Config)
		if err != nil {
			return nil, nil, fmt.Errorf("error building pipeline data reduction alert: %w", err)
		}

		alerts = append(alerts, alert)

		logger.GetLogger().Info("Pipeline data reduction alert created",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("pipelineId", data.Pipeline.ID.String()))
	} else {
		// Only auto-resolve if pipeline has sufficient traffic to make a reliable assessment
		if todayIngested >= data.Config.MinimumEventsThreshold {
			// For pipeline-level alerts, the "rule ID" is actually the pipeline ID
			healthyRuleIds = append(healthyRuleIds, data.Pipeline.ID.String())
			logger.GetLogger().Info("Pipeline is healthy with sufficient traffic, adding to auto-resolution list",
				zap.String("tenantId", data.Tenant.Id.String()),
				zap.String("pipelineId", data.Pipeline.ID.String()),
				zap.String("pipelineName", data.Pipeline.Name),
				zap.Int64("todayIngested", todayIngested),
				zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
		}
	}

	return alerts, healthyRuleIds, nil
}

// checkPipelineDataReductionCondition checks if pipeline has excessive data reduction
func (p *PipelineDataReductionProcessor) checkPipelineDataReductionCondition(data *ProcessingData) (bool, int64, int64, int64, int64, error) {
	pipelineId := data.Pipeline.ID.String()
	logSourceId := data.LogSources[0].LogSourceID.String() // Use first log source

	// Get today's pipeline statistics
	todayStats, err := data.StatsService.GetPipelineStats(pipelineId, logSourceId, data.Config.TodayStart, data.Config.TodayEnd)
	if err != nil {
		return false, 0, 0, 0, 0, err
	}

	// Get yesterday's pipeline statistics
	yesterdayStats, err := data.StatsService.GetPipelineStats(pipelineId, logSourceId, data.Config.YesterdayStart, data.Config.YesterdayEnd)
	if err != nil {
		return false, 0, 0, 0, 0, err
	}

	// Only alert if there's sufficient traffic
	if todayStats.Ingested < data.Config.MinimumEventsThreshold {
		logger.GetLogger().Debug("skipping pipeline data reduction alert due to low traffic",
			zap.String("pipelineId", pipelineId),
			zap.Int64("todayIngested", todayStats.Ingested),
			zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
		return false, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
	}

	// Calculate reduction percentages
	var todayReductionPercent, yesterdayReductionPercent float64
	if todayStats.Ingested > 0 {
		todayReductionPercent = ((float64(todayStats.Ingested) - float64(todayStats.Delivered)) / float64(todayStats.Ingested)) * 100
	}
	if yesterdayStats.Ingested > 0 {
		yesterdayReductionPercent = ((float64(yesterdayStats.Ingested) - float64(yesterdayStats.Delivered)) / float64(yesterdayStats.Ingested)) * 100
	}

	// Check if today's reduction is significantly higher than yesterday's
	if yesterdayReductionPercent > 0 {
		reductionIncrease := ((todayReductionPercent - yesterdayReductionPercent) / yesterdayReductionPercent) * 100

		if reductionIncrease > data.Config.PipelineDataReductionThreshold {
			logger.GetLogger().Info("Pipeline data reduction increase detected",
				zap.String("pipelineId", pipelineId),
				zap.Float64("todayReductionPercent", todayReductionPercent),
				zap.Float64("yesterdayReductionPercent", yesterdayReductionPercent),
				zap.Float64("reductionIncrease", reductionIncrease),
				zap.Float64("threshold", data.Config.PipelineDataReductionThreshold))
			return true, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
		}
	}

	return false, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
}

// ================================
// Unmatched No Route Processor
// ================================

func (p *UnmatchedNoRouteProcessor) GetAlertType() string {
	return "UnmatchedNoRouteProcessor"
}

func (p *UnmatchedNoRouteProcessor) ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	// Only process if pipeline has rules
	if len(data.VCRules) == 0 {
		logger.GetLogger().Debug("skipping unmatched no route processor check - no rules",
			zap.String("pipelineId", data.Pipeline.ID.String()))
		return alerts, healthyRuleIds, nil
	}

	logger.GetLogger().Debug("Processing unmatched no route processor alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.Pipeline.ID.String()))

	unmatchedAlerts, healthyRules, err := p.checkUnmatchedNoRouteProcessorConditions(ctx, data)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking unmatched no route processor alerts: %w", err)
	}

	for _, vcAlert := range unmatchedAlerts {
		alert, err := buildVCAlert(*vcAlert, data.Config)
		if err != nil {
			logger.GetLogger().Error("error building unmatched no route processor alert", zap.Error(err),
				zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcAlert.RuleID.String()))
			continue
		}
		alerts = append(alerts, alert)

		logger.GetLogger().Info("Unmatched no route processor alert created",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("ruleId", vcAlert.RuleID.String()))
	}

	// Add healthy rules to auto-resolution list
	for _, ruleId := range healthyRules {
		healthyRuleIds = append(healthyRuleIds, ruleId.String())
		logger.GetLogger().Info("Rule is healthy, adding to auto-resolution list",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("ruleId", ruleId.String()))
	}

	return alerts, healthyRuleIds, nil
}

// checkUnmatchedNoRouteProcessorConditions implements the complex Alert 3 logic
func (p *UnmatchedNoRouteProcessor) checkUnmatchedNoRouteProcessorConditions(ctx context.Context, data *ProcessingData) ([]*model.VCAlert, []uuid.UUID, error) {
	var vcAlerts []*model.VCAlert
	var healthyRules []uuid.UUID

	// Track rules we've checked to avoid duplicates in healthy list
	checkedRules := make(map[uuid.UUID]bool)

	for _, logSource := range data.LogSources {
		sourceId := logSource.LogSourceID

		// Check 1: sendUnmatchedEvents to primary destination
		sendUnmatchedToPrimary, err := p.getSourceConfiguration(ctx, sourceId)
		if err != nil {
			logger.GetLogger().Error("error getting source configuration",
				zap.Error(err), zap.String("sourceId", sourceId.String()))
			continue
		}

		// Check 2: Route processor configuration
		hasValidRouteProcessor, err := p.checkRouteProcessorConfiguration(ctx, data.Pipeline.ID, sourceId)
		if err != nil {
			logger.GetLogger().Error("error checking route processor configuration",
				zap.Error(err), zap.String("pipelineId", data.Pipeline.ID.String()), zap.String("sourceId", sourceId.String()))
			continue
		}

		// Check 3: Get rule with lowest priority and check drop percentage
		lowestPriorityRule, err := p.getRuleWithLowestPriority(ctx, data.Pipeline.ID, data.Tenant.Id)
		if err != nil {
			logger.GetLogger().Error("error getting lowest priority rule",
				zap.Error(err), zap.String("pipelineId", data.Pipeline.ID.String()))
			continue
		}

		if lowestPriorityRule == nil {
			logger.GetLogger().Debug("no rules found for pipeline",
				zap.String("pipelineId", data.Pipeline.ID.String()))
			continue
		}

		// If sendUnmatchedEvents to primary destination is true, rule is healthy
		if sendUnmatchedToPrimary {
			if !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to sendUnmatchedEvents enabled",
					zap.String("sourceId", sourceId.String()),
					zap.String("ruleId", lowestPriorityRule.ID.String()))
			}
			continue
		}

		// If pipeline has valid route processor configuration, rule is healthy
		if hasValidRouteProcessor {
			if !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to valid route processor",
					zap.String("pipelineId", data.Pipeline.ID.String()),
					zap.String("sourceId", sourceId.String()),
					zap.String("ruleId", lowestPriorityRule.ID.String()))
			}
			continue
		}

		// Check if the rule's drop percentage exceeds threshold
		exceedsThreshold, todayEvaluated, todayMatched, err := p.checkRuleDropPercentage(lowestPriorityRule, sourceId, data)
		if err != nil {
			logger.GetLogger().Error("error checking rule drop percentage",
				zap.Error(err), zap.String("ruleId", lowestPriorityRule.ID.String()))
			continue
		}

		if exceedsThreshold {
			// Create alert for this combination
			var matchedPercent, unmatchedPercent float64
			if todayEvaluated > 0 {
				matchedPercent = (float64(todayMatched) / float64(todayEvaluated)) * 100
				unmatchedPercent = 100 - matchedPercent
			}

			vcAlert := model.NewVCAlert(data.Tenant, data.Pipeline, lowestPriorityRule.ID, lowestPriorityRule.Name, sourceId,
				model.VCAlertTypeUnmatchedNoRouteProcessor,
				todayMatched, 0, todayEvaluated, 0, // No yesterday data needed for this alert
				matchedPercent, unmatchedPercent)

			vcAlerts = append(vcAlerts, vcAlert)

			logger.GetLogger().Info("Created unmatched no route processor alert",
				zap.String("pipelineId", data.Pipeline.ID.String()),
				zap.String("sourceId", sourceId.String()),
				zap.String("ruleId", lowestPriorityRule.ID.String()),
				zap.Float64("unmatchedPercent", unmatchedPercent))
		} else {
			// Rule doesn't exceed threshold and has sufficient traffic, it's healthy
			if todayEvaluated >= data.Config.MinimumEventsThreshold && !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to low drop percentage with sufficient traffic",
					zap.String("ruleId", lowestPriorityRule.ID.String()),
					zap.Int64("todayEvaluated", todayEvaluated),
					zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
			}
		}
	}

	return vcAlerts, healthyRules, nil
}

// Helper methods for UnmatchedNoRouteProcessor
func (p *UnmatchedNoRouteProcessor) getSourceConfiguration(ctx context.Context, sourceId uuid.UUID) (sendUnmatchedToPrimary bool, err error) {
	// Define a struct to hold only the advanced_configuration field
	var source struct {
		AdvancedConfiguration json.RawMessage `gorm:"column:advanced_configuration"`
	}

	err = p.db.WithContext(ctx).Table("log_source").
		Select("advanced_configuration").
		Where("id = ?", sourceId).
		Take(&source).Error

	if err != nil {
		return false, fmt.Errorf("error getting source advanced_configuration: %w", err)
	}

	// Parse the advanced_configuration JSON to check sendUnmatchedEventToPrimaryDestination
	if len(source.AdvancedConfiguration) > 0 {
		var advancedConfig map[string]interface{}
		err = json.Unmarshal(source.AdvancedConfiguration, &advancedConfig)
		if err != nil {
			return false, fmt.Errorf("error parsing source advanced_configuration: %w", err)
		}

		// Check for sendUnmatchedEventToPrimaryDestination
		if sendUnmatched, exists := advancedConfig["sendUnmatchedEventToPrimaryDestination"]; exists {
			if sendUnmatchedBool, ok := sendUnmatched.(bool); ok {
				return sendUnmatchedBool, nil
			}
		}
	}

	// Default to false if not specified
	return false, nil
}

func (p *UnmatchedNoRouteProcessor) checkRouteProcessorConfiguration(ctx context.Context, pipelineId, sourceId uuid.UUID) (hasValidRouteProcessor bool, err error) {
	var routeProcessor struct {
		UnmatchedAction        string `json:"unmatched_action"`
		ExplicitDropAction     string `json:"explicit_drop_action"`
		SecondaryDestinationId string `json:"secondary_destination_id"`
	}

	// Query route processor from change flag
	err = p.db.WithContext(ctx).Table("route_processor").
		Where("pipeline_id = ? AND has_vc_route_processor = true",
			pipelineId.String()).
		First(&routeProcessor).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("error checking route processor: %w", err)
	}

	if routeProcessor.UnmatchedAction == "PUBLISH" || routeProcessor.ExplicitDropAction == "PUBLISH" {
		return true, nil
	}

	return false, nil
}

func (p *UnmatchedNoRouteProcessor) getRuleWithLowestPriority(ctx context.Context, pipelineId, tenantId uuid.UUID) (*rule.Rule, error) {
	var vcRuleResult VCRuleQueryResult
	err := p.db.WithContext(ctx).Table("vc_rule").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
					pipelineId.String(), tenantId.String(), VCRuleStatusActive).
		Order("priority DESC"). // Higher priority number = lower priority
		First(&vcRuleResult).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // No rules found
		}
		return nil, fmt.Errorf("error getting rule with lowest priority: %w", err)
	}

	return &vcRuleResult.Rule, nil
}

func (p *UnmatchedNoRouteProcessor) checkRuleDropPercentage(vcRule *rule.Rule, sourceId uuid.UUID, data *ProcessingData) (bool, int64, int64, error) {
	if vcRule == nil {
		return false, 0, 0, nil
	}

	// Get rule statistics for today
	todayStats, err := data.StatsService.GetRuleStats(sourceId.String(), vcRule.ID.String(), data.Config.TodayStart, data.Config.TodayEnd)
	if err != nil {
		return false, 0, 0, fmt.Errorf("error getting rule stats: %w", err)
	}

	// Only check if there's sufficient traffic
	if todayStats.Evaluated < data.Config.MinimumEventsThreshold {
		return false, todayStats.Evaluated, todayStats.Matched, nil
	}

	// Calculate drop percentage (unmatched percentage)
	var dropPercent float64
	if todayStats.Evaluated > 0 {
		todayUnmatched := todayStats.Evaluated - todayStats.Matched
		dropPercent = (float64(todayUnmatched) / float64(todayStats.Evaluated)) * 100
	}

	// Check if drop percentage exceeds threshold
	if dropPercent > data.Config.UnmatchedNoRouteProcessorThreshold {
		logger.GetLogger().Info("Rule drop percentage exceeds threshold",
			zap.String("ruleId", vcRule.ID.String()),
			zap.String("ruleName", vcRule.Name),
			zap.Float64("dropPercent", dropPercent),
			zap.Float64("threshold", data.Config.UnmatchedNoRouteProcessorThreshold),
			zap.Int64("todayEvaluated", todayStats.Evaluated),
			zap.Int64("todayMatched", todayStats.Matched))
		return true, todayStats.Evaluated, todayStats.Matched, nil
	}

	return false, todayStats.Evaluated, todayStats.Matched, nil
}

// ================================
// Main Orchestrator
// ================================

func NewVCAlertOrchestrator(ctx context.Context) (*VCAlertOrchestrator, error) {
	vcConfig := NewVCAlertConfig()
	db := config.GetDB()
	osClient := os.GetClient()

	// Create alerts manager
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return nil, err
	}

	// Initialize processors
	processors := []AlertProcessor{
		&DropRuleIncreaseProcessor{},
		&PipelineDataReductionProcessor{},
		&UnmatchedNoRouteProcessor{db: db},
	}

	return &VCAlertOrchestrator{
		ctx:           ctx,
		db:            db,
		osClient:      osClient,
		alertsManager: alertsManager,
		config:        vcConfig,
		processors:    processors,
	}, nil
}

// Close closes the alerts manager
func (o *VCAlertOrchestrator) Close() {
	o.alertsManager.Close(o.ctx)
}

// ProcessAlerts processes VC alerts for all tenants
func (o *VCAlertOrchestrator) ProcessAlerts() error {
	// Get all tenants
	tenants, err := tenant.GetTenants(o.ctx, o.db)
	if err != nil {
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return err
	}

	mode := "production (n-1, n-2)"
	if o.config.UseCurrentDay {
		mode = "testing (n, n-1)"
	}

	logger.GetLogger().Info("processing VC rule alerts",
		zap.String("mode", mode),
		zap.Bool("useCurrentDay", o.config.UseCurrentDay),
		zap.String("todayRange", fmt.Sprintf("%s to %s",
			o.config.TodayStart.Format("2006-01-02 15:04:05"),
			o.config.TodayEnd.Format("2006-01-02 15:04:05"))),
		zap.String("yesterdayRange", fmt.Sprintf("%s to %s",
			o.config.YesterdayStart.Format("2006-01-02 15:04:05"),
			o.config.YesterdayEnd.Format("2006-01-02 15:04:05"))))

	for _, t := range tenants {
		tenantId := t.Id.String()
		logger.GetLogger().Info("processing tenant for VC alerts", zap.String("tenantId", tenantId))

		err := o.processTenantAlerts(&t)
		if err != nil {
			logger.GetLogger().Error("error processing VC alerts for tenant", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}
	}

	return nil
}

// processTenantAlerts processes alerts for a single tenant
func (o *VCAlertOrchestrator) processTenantAlerts(t *tenant.Tenant) error {
	tenantId := t.Id

	// Get all active pipelines for the tenant
	pipelines, err := pipeline.GetActivePipelines(o.ctx, o.db, tenantId)
	if err != nil {
		logger.GetLogger().Error("error getting active pipelines", zap.Error(err), zap.String("tenantId", tenantId.String()))
		return err
	}

	logger.GetLogger().Info("found active pipelines", zap.Int("count", len(pipelines)), zap.String("tenantId", tenantId.String()))

	for _, pipelineInfo := range pipelines {
		pipelineId := pipelineInfo.ID

		err = o.processPipelineAlerts(t, &pipelineInfo)
		if err != nil {
			logger.GetLogger().Error("error processing pipeline alerts", zap.Error(err),
				zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
			continue
		}
	}

	return nil
}

// processPipelineAlerts processes alerts for a single pipeline
func (o *VCAlertOrchestrator) processPipelineAlerts(t *tenant.Tenant, pipelineInfo *pipeline.Pipeline) error {
	pipelineId := pipelineInfo.ID
	tenantId := t.Id

	// Get active VC rules for this pipeline
	vcRules, err := o.getActiveVCRules(pipelineId, tenantId)
	if err != nil {
		logger.GetLogger().Error("error getting active VC rules", zap.Error(err),
			zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
		return err
	}

	// Get log sources for this pipeline
	logSources, err := pipeline.GetPipelineLogSources(o.ctx, o.db, pipelineId)
	if err != nil {
		logger.GetLogger().Error("error getting pipeline log sources", zap.Error(err),
			zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
		return err
	}

	// Create statistics service
	statsService := NewVCStatisticsService(o.ctx, tenantId, o.config)

	// Prepare processing data
	processingData := &ProcessingData{
		Tenant:       t,
		Pipeline:     pipelineInfo,
		VCRules:      vcRules,
		LogSources:   logSources,
		StatsService: statsService,
		Config:       o.config,
	}

	// Process all alert types
	var allAlerts []*alerts_async.Alert
	var allHealthyRuleIds []string

	for _, processor := range o.processors {
		alerts, healthyRuleIds, err := processor.ProcessAlerts(o.ctx, processingData)
		if err != nil {
			logger.GetLogger().Error("error processing alerts", zap.Error(err),
				zap.String("processorType", processor.GetAlertType()),
				zap.String("tenantId", tenantId.String()),
				zap.String("pipelineId", pipelineId.String()))
			continue
		}

		allAlerts = append(allAlerts, alerts...)
		allHealthyRuleIds = append(allHealthyRuleIds, healthyRuleIds...)
	}

	// Send alerts
	if len(allAlerts) > 0 {
		err = o.alertsManager.SendAlerts(allAlerts)
		if err != nil {
			logger.GetLogger().Error("error sending VC alerts", zap.Error(err), zap.String("tenantId", tenantId.String()))
			return err
		}
		logger.GetLogger().Info("sent VC alerts", zap.Int("count", len(allAlerts)), zap.String("tenantId", tenantId.String()))
	}

	// Auto-resolve any open VC alerts for healthy rules
	if len(allHealthyRuleIds) > 0 {
		err = o.autoResolveHealthyRules(tenantId.String(), allHealthyRuleIds)
		if err != nil {
			logger.GetLogger().Error("error auto-resolving VC alerts", zap.Error(err), zap.String("tenantId", tenantId.String()))
		}
	}

	return nil
}

// getActiveVCRules gets all active volume control rules for a pipeline
func (o *VCAlertOrchestrator) getActiveVCRules(pipelineId, tenantId uuid.UUID) ([]rule.Rule, error) {
	var vcRuleResults []VCRuleQueryResult
	err := o.db.WithContext(o.ctx).Table("vc_rule").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineId.String(), tenantId.String(), VCRuleStatusActive).
		Find(&vcRuleResults).Error

	if err != nil {
		return nil, err
	}

	// Convert to []rule.Rule
	var vcRules []rule.Rule
	for _, result := range vcRuleResults {
		vcRules = append(vcRules, result.Rule)
	}

	return vcRules, nil
}

// autoResolveHealthyRules auto-resolves existing VC alerts for healthy rules
func (o *VCAlertOrchestrator) autoResolveHealthyRules(tenantId string, healthyRuleIds []string) error {
	var alertsToDismiss []string
	for _, ruleId := range healthyRuleIds {
		q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionality:%s AND functionalityType:%s AND secondaryEntityId:%s",
			tenantId, alerts_async.VolumeControlRule.String(), alerts_async.VolumeDeviationChecker.String(), ruleId)
		openAlerts, _, err := os.Search(o.ctx, o.osClient, common.AlertsIndex, q)
		if err != nil {
			logger.GetLogger().Error("error while searching VC alerts to auto-resolve", zap.Error(err), zap.String("query", q))
			continue
		}
		var alerts []statistics.AlertDocument
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
		if err != nil {
			logger.GetLogger().Error("error while creating decoder for VC alerts", zap.Error(err))
			continue
		}
		if err := decoder.Decode(openAlerts); err != nil {
			logger.GetLogger().Error("error while decoding OpenSearch VC alert response", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}
		for _, alrt := range alerts {
			alertsToDismiss = append(alertsToDismiss, alrt.Id)
		}
	}
	if len(alertsToDismiss) > 0 {
		if err := o.alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
			logger.GetLogger().Error("error while auto-resolving VC alerts", zap.Error(err), zap.String("tenantId", tenantId))
			return err
		} else {
			logger.GetLogger().Info("auto-resolved VC alerts for healthy rules", zap.String("tenantId", tenantId), zap.Strings("alertIds", alertsToDismiss))
		}
	}
	return nil
}

// ================================
// Alert Building
// ================================

func buildVCAlert(vcAlert model.VCAlert, config *VCAlertConfig) (*alerts_async.Alert, error) {
	var title, message string

	switch vcAlert.AlertType {
	case model.VCAlertTypeDropRuleIncrease:
		title = fmt.Sprintf("DROP rule '%s' match rate increased significantly", vcAlert.RuleName)
		message = fmt.Sprintf("DROP rule '%s' in pipeline '%s' has increased its match rate by more than %.1f%% compared to yesterday. "+
			"Today: %s matched out of %s evaluated (%.2f%%), Yesterday: %s matched out of %s evaluated. "+
			"This indicates the rule is dropping more events than expected.",
			vcAlert.RuleName, vcAlert.Pipeline.Name, config.DropRuleIncreaseThreshold,
			util.HumanReadableNumber(vcAlert.TodayMatched), util.HumanReadableNumber(vcAlert.TodayEvaluated), vcAlert.MatchedPercent,
			util.HumanReadableNumber(vcAlert.YesterdayMatched), util.HumanReadableNumber(vcAlert.YesterdayEvaluated))

	case model.VCAlertTypePipelineDataReduction:
		title = fmt.Sprintf("Pipeline '%s' has excessive data reduction", vcAlert.Pipeline.Name)
		reductionPercent := ((float64(vcAlert.TodayEvaluated) - float64(vcAlert.TodayMatched)) / float64(vcAlert.TodayEvaluated)) * 100
		message = fmt.Sprintf("Pipeline '%s' is reducing data by %.2f%% today (%s delivered out of %s ingested), "+
			"which is more than %.1f%% higher than yesterday (%s delivered out of %s ingested). "+
			"This indicates volume control rules are dropping significantly more events than normal.",
			vcAlert.Pipeline.Name, reductionPercent, util.HumanReadableNumber(vcAlert.TodayMatched), util.HumanReadableNumber(vcAlert.TodayEvaluated),
			config.PipelineDataReductionThreshold, util.HumanReadableNumber(vcAlert.YesterdayMatched), util.HumanReadableNumber(vcAlert.YesterdayEvaluated))

	case model.VCAlertTypeUnmatchedNoRouteProcessor:
		title = fmt.Sprintf("Rule '%s' has high unmatched events with no route processor", vcAlert.RuleName)
		message = fmt.Sprintf("Rule '%s' in pipeline '%s' has %.2f%% unmatched events (%s unmatched out of %s evaluated), "+
			"exceeding the %.1f%% threshold. The source is not configured to send unmatched events to primary destination "+
			"and the pipeline lacks proper route processor configuration for handling unmatched events. "+
			"These events may be lost or not processed properly.",
			vcAlert.RuleName, vcAlert.Pipeline.Name, vcAlert.UnmatchedPercent,
			util.HumanReadableNumber(vcAlert.TodayEvaluated-vcAlert.TodayMatched), util.HumanReadableNumber(vcAlert.TodayEvaluated), config.UnmatchedNoRouteProcessorThreshold)

	default:
		return nil, fmt.Errorf("unknown VC alert type: %s", vcAlert.AlertType)
	}

	return alerts_async.NewAlert(
		alerts_async.VolumeControlRule,
		alerts_async.WithEntity(vcAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VolumeDeviationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Volume control rule condition detected"),
	)
}

// ================================
// Entry Point
// ================================

func SendAlertsForVCRules(ctx context.Context) error {
	orchestrator, err := NewVCAlertOrchestrator(ctx)
	if err != nil {
		return err
	}
	defer orchestrator.Close()

	return orchestrator.ProcessAlerts()
}
