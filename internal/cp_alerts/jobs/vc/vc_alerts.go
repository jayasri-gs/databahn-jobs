package vc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/store/vc_rule"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// DefaultDropRuleIncreaseThreshold Default thresholds (can be overridden by environment variables)
	DefaultDropRuleIncreaseThreshold          = 50.0
	DefaultPipelineDataReductionThreshold     = 50.0
	DefaultUnmatchedNoRouteProcessorThreshold = 20.0
	// MinimumEventsThreshold Minimum events threshold to avoid noise from low-traffic sources
	MinimumEventsThreshold = 1000
	// DefaultUseCurrentDay Default behavior: false means production mode (n-1 and n-2), true means testing mode (n and n-1)
	DefaultUseCurrentDay = false
)

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
// Each processor is responsible for processing, building, and auto-resolving its own alerts for better decoupling
type AlertProcessor interface {
	ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error)
	AutoResolveAlerts(ctx context.Context, data *ProcessingData, healthyRuleIds []string) error
	GetAlertType() string
}

// ProcessingData contains all data needed for alert processing and auto-resolution
type ProcessingData struct {
	Tenant          *tenant.Tenant
	PipelineMapping *pipeline.PipelineWithMappings
	VCRules         []vc_rule.VCRule
	CachedSources   map[uuid.UUID]source.Source
	StatsService    *VCStatisticsService
	Config          *VCAlertConfig
	// Dependencies for auto-resolution
	OSClient      *opensearch.Client
	AlertsManager *alert.AlertsManager
}

type VCAlertOrchestrator struct {
	ctx           context.Context
	db            *gorm.DB
	osClient      *opensearch.Client
	alertsManager *alert.AlertsManager
	config        *VCAlertConfig
	processors    []AlertProcessor
}

// ================================
// Configuration Management
// ================================

func NewVCAlertConfig() *VCAlertConfig {
	dropRuleIncreaseThreshold := utils.GetEnvFloat("VC_DROP_RULE_INCREASE_THRESHOLD", DefaultDropRuleIncreaseThreshold)
	pipelineDataReductionThreshold := utils.GetEnvFloat("VC_PIPELINE_DATA_REDUCTION_THRESHOLD", DefaultPipelineDataReductionThreshold)
	unmatchedNoRouteProcessorThreshold := utils.GetEnvFloat("VC_UNMATCHED_NO_ROUTE_PROCESSOR_THRESHOLD", DefaultUnmatchedNoRouteProcessorThreshold)
	minimumEventsThreshold := utils.GetEnvInt("VC_MINIMUM_EVENTS_THRESHOLD", MinimumEventsThreshold)
	useCurrentDay := utils.GetEnvBool("VC_USE_CURRENT_DAY", DefaultUseCurrentDay)

	todayStart, todayEnd := getTodayTimeRange(useCurrentDay)
	yesterdayStart, yesterdayEnd := getYesterdayTimeRange(useCurrentDay)

	cfg := &VCAlertConfig{
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
		zap.Float64("drop_rule_increase_threshold", cfg.DropRuleIncreaseThreshold),
		zap.Float64("pipeline_data_reduction_threshold", cfg.PipelineDataReductionThreshold),
		zap.Float64("unmatched_no_route_processor_threshold", cfg.UnmatchedNoRouteProcessorThreshold),
		zap.Int64("minimum_events_threshold", cfg.MinimumEventsThreshold),
		zap.Bool("use_current_day", cfg.UseCurrentDay))

	return cfg
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
	wg := sync.WaitGroup{}
	wg.Add(2)
	var evaluatedCount, matchedCount int64
	var evaluatedErr, matchedErr error

	go func() {
		defer wg.Done()
		evaluatedCount, evaluatedErr = s.GetRuleEvaluatedStats(sourceId, ruleId, startTime, endTime)
	}()

	go func() {
		defer wg.Done()
		matchedCount, matchedErr = s.GetRuleMatchedStats(sourceId, ruleId, startTime, endTime)
	}()

	wg.Wait()
	if evaluatedErr != nil {
		return nil, evaluatedErr
	}

	if matchedErr != nil {
		return nil, matchedErr
	}

	return &RuleStats{
		Evaluated: evaluatedCount,
		Matched:   matchedCount,
	}, nil
}

// GetRuleEvaluatedStats gets only the evaluated stats for a rule from OpenSearch
func (s *VCStatisticsService) GetRuleEvaluatedStats(sourceId, ruleId string, startTime, endTime time.Time) (int64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	evaluatedQuery := fmt.Sprintf(`name:"rule_events_evaluated" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)
	evaluatedResponse, err := statistics.GetStatsSum(s.ctx, evaluatedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, fmt.Errorf("error getting evaluated events for rule %s: %w", ruleId, err)
	}
	return int64(evaluatedResponse.Sum), nil
}

func (s *VCStatisticsService) GetRuleMatchedStats(sourceId, ruleId string, startTime, endTime time.Time) (int64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	matchedQuery := fmt.Sprintf(`name:"rule_events_matched" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)
	matchedResponse, err := statistics.GetStatsSum(s.ctx, matchedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, fmt.Errorf("error getting matched events for rule %s: %w", ruleId, err)
	}
	return int64(matchedResponse.Sum), nil
}

// GetPipelineStats gets pipeline-level ingestion and delivery statistics from OpenSearch
func (s *VCStatisticsService) GetPipelineStats(pipelineId, logSourceId string, startTime, endTime time.Time) (*PipelineStats, error) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	var ingestedCount int64
	var deliveredCount int64
	var ingestedErr error
	var deliveredErr error

	go func() {
		defer wg.Done()
		ingestedCount, ingestedErr = s.GetPipelineIngestionStats(pipelineId, logSourceId, startTime, endTime)
	}()

	go func() {
		defer wg.Done()
		deliveredCount, deliveredErr = s.GetPipelineDeliveryStats(pipelineId, startTime, endTime)
	}()

	wg.Wait()

	if ingestedErr != nil {
		return nil, ingestedErr
	}

	if deliveredErr != nil {
		return nil, deliveredErr
	}

	return &PipelineStats{
		Ingested:  ingestedCount,
		Delivered: deliveredCount,
	}, nil
}

func (s *VCStatisticsService) GetPipelineIngestionStats(pipelineId, logSourceId string, startTime, endTime time.Time) (int64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)
	ingestedQuery := fmt.Sprintf(`name:"total_events_delivered" AND tags.component_name:"ingestion" AND tags.db_event_source_id:"%s"`, logSourceId)
	ingestedResponse, err := statistics.GetStatsSum(s.ctx, ingestedQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, fmt.Errorf("error getting ingested events for pipeline %s: %w", pipelineId, err)
	}

	return int64(ingestedResponse.Sum), nil
}

func (s *VCStatisticsService) GetPipelineDeliveryStats(pipelineId string, startTime, endTime time.Time) (int64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)
	deliveredQuery := fmt.Sprintf(`name:"total_events_delivered" AND tags.component_name:"dispenser" AND tags.db_pipeline_id:"%s"`, pipelineId)
	deliveredResponse, err := statistics.GetStatsSum(s.ctx, deliveredQuery, s.tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, fmt.Errorf("error getting delivered events for pipeline %s: %w", pipelineId, err)
	}

	return int64(deliveredResponse.Sum), nil
}

// ================================
// Shared Auto-Resolution Helper
// ================================

// autoResolveAlertsHelper is a shared helper function for auto-resolving alerts
func autoResolveAlertsHelper(ctx context.Context, data *ProcessingData, healthyRuleIds []string, functionalityType alerts_async.FunctionalityType, processorName string) error {
	if len(healthyRuleIds) == 0 {
		return nil
	}

	tenantId := data.Tenant.Id.String()

	// Build rule ID conditions
	ruleIdConditions := make([]string, len(healthyRuleIds))
	for i, ruleId := range healthyRuleIds {
		ruleIdConditions[i] = fmt.Sprintf("secondaryEntityId:%s", ruleId)
	}

	// Build query to find alerts for this specific functionality type and healthy rule IDs
	q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionality:%s AND functionalityType:%s AND (%s)",
		tenantId,
		alerts_async.VolumeControlRule.String(),
		functionalityType.String(),
		strings.Join(ruleIdConditions, " OR "))

	logger.GetLogger().Debug("searching for alerts to auto-resolve",
		zap.String("processor", processorName),
		zap.String("tenantId", tenantId),
		zap.String("query", q))

	openAlerts, _, err := os.Search(ctx, data.OSClient, common.AlertsIndex, q)
	if err != nil {
		logger.GetLogger().Error("error while searching alerts to auto-resolve", zap.Error(err),
			zap.String("processor", processorName), zap.String("query", q))
		return err
	}

	var alerts []statistics.AlertDocument
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	if err != nil {
		logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err),
			zap.String("processor", processorName))
		return err
	}

	if err := decoder.Decode(openAlerts); err != nil {
		logger.GetLogger().Error("error while decoding OpenSearch alert response", zap.Error(err),
			zap.String("processor", processorName), zap.String("tenantId", tenantId))
		return err
	}

	// Collect alert IDs to dismiss
	alertsToDismiss := make([]string, 0, len(alerts))
	for _, alrt := range alerts {
		alertsToDismiss = append(alertsToDismiss, alrt.Id)
	}

	if len(alertsToDismiss) > 0 {
		if err := data.AlertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
			logger.GetLogger().Error("error while auto-resolving alerts", zap.Error(err),
				zap.String("processor", processorName), zap.String("tenantId", tenantId))
			return err
		} else {
			logger.GetLogger().Info("auto-resolved alerts for healthy rules",
				zap.String("processor", processorName),
				zap.String("tenantId", tenantId),
				zap.Strings("alertIds", alertsToDismiss),
				zap.Int("healthyRules", len(healthyRuleIds)),
				zap.Int("alertsResolved", len(alertsToDismiss)))
		}
	} else {
		logger.GetLogger().Debug("no alerts found to auto-resolve",
			zap.String("processor", processorName),
			zap.String("tenantId", tenantId),
			zap.Int("healthyRules", len(healthyRuleIds)))
	}

	return nil
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

// cacheSourcesForTenant caches all active sources for a tenant
func (o *VCAlertOrchestrator) cacheSourcesForTenant(tenantId uuid.UUID) (map[uuid.UUID]source.Source, error) {
	sources, err := source.GetSourcesByTenantAndStatus(o.ctx, o.db, tenantId, "ACTIVE")
	if err != nil {
		return nil, fmt.Errorf("error getting active sources for tenant %s: %w", tenantId.String(), err)
	}

	// Create a map for quick lookups
	cachedSources := make(map[uuid.UUID]source.Source)
	for _, src := range sources {
		cachedSources[src.ID] = src
	}

	return cachedSources, nil
}

// processTenantAlerts processes alerts for a single tenant
func (o *VCAlertOrchestrator) processTenantAlerts(t *tenant.Tenant) error {
	tenantId := t.Id

	// Cache all active sources for this tenant before processing pipelines
	cachedSources, err := o.cacheSourcesForTenant(tenantId)
	if err != nil {
		logger.GetLogger().Error("error caching sources for tenant", zap.Error(err), zap.String("tenantId", tenantId.String()))
		return err
	}

	logger.GetLogger().Info("cached sources for tenant", zap.Int("sourceCount", len(cachedSources)), zap.String("tenantId", tenantId.String()))

	// Get all active pipelines for the tenant
	pipelines, err := pipeline.GetActivePipelinesWithMappings(o.ctx, o.db, tenantId)
	if err != nil {
		logger.GetLogger().Error("error getting active pipelines", zap.Error(err), zap.String("tenantId", tenantId.String()))
		return err
	}

	logger.GetLogger().Info("found active pipelines", zap.Int("count", len(pipelines)), zap.String("tenantId", tenantId.String()))

	for _, pipelineInfo := range pipelines {
		pipelineId := pipelineInfo.Pipeline.ID

		err = o.processPipelineAlerts(t, &pipelineInfo, cachedSources)
		if err != nil {
			logger.GetLogger().Error("error processing pipeline alerts", zap.Error(err),
				zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
			continue
		}
	}

	return nil
}

// processPipelineAlerts processes alerts for a single pipeline
func (o *VCAlertOrchestrator) processPipelineAlerts(t *tenant.Tenant, pipelineMapping *pipeline.PipelineWithMappings, cachedSources map[uuid.UUID]source.Source) error {
	pipelineId := pipelineMapping.Pipeline.ID
	tenantId := t.Id

	// Get active VC rules for this pipeline
	vcRules, err := vc_rule.GetActiveVCRulesByPipelineAndTenant(pipelineId, tenantId, o.db)
	if err != nil {
		logger.GetLogger().Error("error getting active VC rules", zap.Error(err),
			zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
		return err
	}

	// Create statistics service
	statsService := NewVCStatisticsService(o.ctx, tenantId, o.config)

	// Prepare processing data
	processingData := &ProcessingData{
		Tenant:          t,
		PipelineMapping: pipelineMapping,
		VCRules:         vcRules,
		CachedSources:   cachedSources,
		StatsService:    statsService,
		Config:          o.config,
		// Dependencies for auto-resolution
		OSClient:      o.osClient,
		AlertsManager: o.alertsManager,
	}

	// Process all alert types
	var allAlerts []*alerts_async.Alert

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

		// Auto-resolve alerts for healthy rules in this processor
		if len(healthyRuleIds) > 0 {
			err = processor.AutoResolveAlerts(o.ctx, processingData, healthyRuleIds)
			if err != nil {
				logger.GetLogger().Error("error auto-resolving alerts", zap.Error(err),
					zap.String("processorType", processor.GetAlertType()),
					zap.String("tenantId", tenantId.String()),
					zap.String("pipelineId", pipelineId.String()))
				// Continue processing other processors even if auto-resolve fails
			}
		}
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

	return nil
}

// ================================
// Entry Point
// ================================

func SendAlertsForVCRules(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	orchestrator, err := NewVCAlertOrchestrator(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error creating VC alert orchestrator: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error creating VC alert orchestrator", zap.Error(err))
		return common.NewJobResult(jobErrors, false)
	}
	defer orchestrator.Close()

	err = orchestrator.ProcessAlerts()
	if err != nil {
		errorMsg := fmt.Sprintf("error processing VC alerts: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error processing VC alerts", zap.Error(err))
		return common.NewJobResult(jobErrors, false)
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed VC alerts processing")
		return common.NewJobResult([]common.JobError{}, true)
	} else {
		logger.GetLoggerWithContext(ctx).Info("VC alerts processing completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResult(jobErrors, false)
	}
}
