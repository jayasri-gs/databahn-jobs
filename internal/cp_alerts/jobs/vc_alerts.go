package jobs

import (
	"context"
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
	DefaultHighUnmatchedThreshold    = 20.0
	DefaultDropRuleIncreaseThreshold = 50.0
	// Minimum events threshold to avoid noise from low-traffic sources
	MinimumEventsThreshold = 1000
)

// VCAlertProcessor handles volume control alert processing with configuration and dependencies
type VCAlertProcessor struct {
	ctx                       context.Context
	db                        *gorm.DB
	osClient                  *opensearch.Client
	alertsManager             *alert.AlertsManager
	highUnmatchedThreshold    float64
	dropRuleIncreaseThreshold float64
	minimumEventsThreshold    int64
	todayStart                time.Time
	todayEnd                  time.Time
	yesterdayStart            time.Time
	yesterdayEnd              time.Time
}

// NewVCAlertProcessor creates a new VCAlertProcessor with loaded configuration
func NewVCAlertProcessor(ctx context.Context) (*VCAlertProcessor, error) {
	// Load configuration from environment variables
	highUnmatchedThreshold := getEnvFloat("VC_HIGH_UNMATCHED_THRESHOLD", DefaultHighUnmatchedThreshold)
	dropRuleIncreaseThreshold := getEnvFloat("VC_DROP_RULE_INCREASE_THRESHOLD", DefaultDropRuleIncreaseThreshold)
	minimumEventsThreshold := utils.GetEnvInt("VC_MINIMUM_EVENTS_THRESHOLD", MinimumEventsThreshold)

	logger.GetLogger().Info("Volume control alert configuration loaded",
		zap.Float64("high_unmatched_threshold", highUnmatchedThreshold),
		zap.Float64("drop_rule_increase_threshold", dropRuleIncreaseThreshold),
		zap.Int("minimum_events_threshold", minimumEventsThreshold))

	db := config.GetDB()
	osClient := os.GetClient()

	// Create alerts manager
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return nil, err
	}

	// Calculate time ranges for today and yesterday
	todayStart, todayEnd := getTodayTimeRange()
	yesterdayStart, yesterdayEnd := getYesterdayTimeRange()

	return &VCAlertProcessor{
		ctx:                       ctx,
		db:                        db,
		osClient:                  osClient,
		alertsManager:             alertsManager,
		highUnmatchedThreshold:    highUnmatchedThreshold,
		dropRuleIncreaseThreshold: dropRuleIncreaseThreshold,
		minimumEventsThreshold:    int64(minimumEventsThreshold),
		todayStart:                todayStart,
		todayEnd:                  todayEnd,
		yesterdayStart:            yesterdayStart,
		yesterdayEnd:              yesterdayEnd,
	}, nil
}

// Close closes the alerts manager
func (p *VCAlertProcessor) Close() {
	p.alertsManager.Close(p.ctx)
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

// SendAlertsForVCRules is the main function that processes VC rule alerts
func SendAlertsForVCRules(ctx context.Context) error {
	processor, err := NewVCAlertProcessor(ctx)
	if err != nil {
		return err
	}
	defer processor.Close()

	return processor.ProcessAlerts()
}

// ProcessAlerts processes VC alerts for all tenants
func (p *VCAlertProcessor) ProcessAlerts() error {
	// Get all tenants
	tenants, err := tenant.GetTenants(p.ctx, p.db)
	if err != nil {
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return err
	}

	logger.GetLogger().Info("processing VC rule alerts",
		zap.Time("todayStart", p.todayStart),
		zap.Time("todayEnd", p.todayEnd),
		zap.Time("yesterdayStart", p.yesterdayStart),
		zap.Time("yesterdayEnd", p.yesterdayEnd))

	for _, t := range tenants {
		tenantId := t.Id.String()
		logger.GetLogger().Info("processing tenant for VC alerts", zap.String("tenantId", tenantId))

		// Track healthy rule IDs for auto-resolution
		var healthyRuleIds []string
		var alertsToSend []*alerts_async.Alert

		err := p.processVCAlertsForTenant(&t, &alertsToSend, &healthyRuleIds)
		if err != nil {
			logger.GetLogger().Error("error processing VC alerts for tenant", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		// Send alerts
		if len(alertsToSend) > 0 {
			err = p.alertsManager.SendAlerts(alertsToSend)
			if err != nil {
				logger.GetLogger().Error("error sending VC alerts", zap.Error(err), zap.String("tenantId", tenantId))
				continue
			}
			logger.GetLogger().Info("sent VC alerts", zap.Int("count", len(alertsToSend)), zap.String("tenantId", tenantId))
		}

		// Auto-resolve any open VC alerts for healthy rules
		if len(healthyRuleIds) > 0 {
			err = p.autoResolveHealthyRules(tenantId, healthyRuleIds)
			if err != nil {
				logger.GetLogger().Error("error auto-resolving VC alerts", zap.Error(err), zap.String("tenantId", tenantId))
			}
		}
	}

	return nil
}

// processVCAlertsForTenant processes VC alerts for a single tenant
func (p *VCAlertProcessor) processVCAlertsForTenant(t *tenant.Tenant, alertsToSend *[]*alerts_async.Alert, healthyRuleIds *[]string) error {
	tenantId := t.Id

	// Get all active pipelines for the tenant
	pipelines, err := pipeline.GetActivePipelines(p.ctx, p.db, tenantId)
	if err != nil {
		logger.GetLogger().Error("error getting active pipelines", zap.Error(err), zap.String("tenantId", tenantId.String()))
		return err
	}

	logger.GetLogger().Info("found active pipelines", zap.Int("count", len(pipelines)), zap.String("tenantId", tenantId.String()))

	for _, pipelineInfo := range pipelines {
		pipelineId := pipelineInfo.ID

		// Get active VC rules for this pipeline
		vcRules, err := p.getActiveVCRules(pipelineId, tenantId)
		if err != nil {
			logger.GetLogger().Error("error getting active VC rules", zap.Error(err),
				zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
			continue
		}

		if len(vcRules) == 0 {
			logger.GetLogger().Debug("no active VC rules for pipeline",
				zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
			continue
		}

		logger.GetLogger().Info("found active VC rules for pipeline", zap.Int("count", len(vcRules)),
			zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))

		// Get log sources for this pipeline
		logSources, err := pipeline.GetPipelineLogSources(p.ctx, p.db, pipelineId)
		if err != nil {
			logger.GetLogger().Error("error getting pipeline log sources", zap.Error(err),
				zap.String("tenantId", tenantId.String()), zap.String("pipelineId", pipelineId.String()))
			continue
		}

		for _, vcRule := range vcRules {
			for _, logSource := range logSources {
				sourceId := logSource.LogSourceID

				// Get rule statistics from OpenSearch using the simplified approach
				todayEvaluated, todayMatched, err := p.getRuleStats(tenantId, sourceId.String(), vcRule.ID.String(), p.todayStart, p.todayEnd)
				if err != nil {
					logger.GetLogger().Error("error getting today's rule stats", zap.Error(err),
						zap.String("tenantId", tenantId.String()), zap.String("ruleId", vcRule.ID.String()))
					continue
				}

				yesterdayEvaluated, yesterdayMatched, err := p.getRuleStats(tenantId, sourceId.String(), vcRule.ID.String(), p.yesterdayStart, p.yesterdayEnd)
				if err != nil {
					logger.GetLogger().Error("error getting yesterday's rule stats", zap.Error(err),
						zap.String("tenantId", tenantId.String()), zap.String("ruleId", vcRule.ID.String()))
					continue
				}

				logger.GetLogger().Debug("rule statistics",
					zap.String("ruleId", vcRule.ID.String()),
					zap.String("ruleName", vcRule.Name),
					zap.String("actionType", vcRule.ActionType),
					zap.Int64("todayEvaluated", todayEvaluated),
					zap.Int64("todayMatched", todayMatched),
					zap.Int64("yesterdayEvaluated", yesterdayEvaluated),
					zap.Int64("yesterdayMatched", yesterdayMatched))

				// Check alert conditions
				alertTypes := p.checkVCAlertConditions(vcRule, todayEvaluated, todayMatched, yesterdayEvaluated, yesterdayMatched)

				if len(alertTypes) > 0 {
					// Calculate percentages
					var matchedPercent, unmatchedPercent float64
					if todayEvaluated > 0 {
						matchedPercent = (float64(todayMatched) / float64(todayEvaluated)) * 100
						unmatchedPercent = 100 - matchedPercent
					}

					// Create separate alerts for each alert type
					for _, alertType := range alertTypes {
						vcAlert := model.NewVCAlert(t, &pipelineInfo, vcRule.ID, vcRule.Name, sourceId, alertType,
							todayMatched, yesterdayMatched, todayEvaluated, yesterdayEvaluated,
							matchedPercent, unmatchedPercent)

						alert, err := buildVCAlert(*vcAlert, p.highUnmatchedThreshold, p.dropRuleIncreaseThreshold)
						if err != nil {
							logger.GetLogger().Error("error building VC alert", zap.Error(err),
								zap.String("tenantId", tenantId.String()), zap.String("ruleId", vcRule.ID.String()),
								zap.String("alertType", string(alertType)))
							continue
						}

						*alertsToSend = append(*alertsToSend, alert)

						logger.GetLogger().Info("VC alert created",
							zap.String("tenantId", tenantId.String()),
							zap.String("ruleId", vcRule.ID.String()),
							zap.String("alertType", string(alertType)))
					}
				} else {
					// Only auto-resolve if rule has sufficient traffic to confidently determine it's healthy
					if todayEvaluated >= p.minimumEventsThreshold {
						// Rule is healthy with sufficient traffic, add to list for auto-resolution
						logger.GetLogger().Info("Rule is healthy with sufficient traffic, adding to auto-resolution list",
							zap.String("tenantId", tenantId.String()),
							zap.String("ruleId", vcRule.ID.String()),
							zap.String("ruleName", vcRule.Name),
							zap.Int64("todayEvaluated", todayEvaluated),
							zap.Int64("minimumThreshold", p.minimumEventsThreshold))
						*healthyRuleIds = append(*healthyRuleIds, vcRule.ID.String())
					} else {
						// Rule has low traffic, skip auto-resolution to avoid false positives
						logger.GetLogger().Debug("Rule has low traffic, skipping auto-resolution",
							zap.String("tenantId", tenantId.String()),
							zap.String("ruleId", vcRule.ID.String()),
							zap.String("ruleName", vcRule.Name),
							zap.Int64("todayEvaluated", todayEvaluated),
							zap.Int64("minimumThreshold", p.minimumEventsThreshold))
					}
				}
			}
		}
	}

	return nil
}

// VCRuleQueryResult represents a VC rule with status as string (matching database schema)
type VCRuleQueryResult struct {
	rule.Rule
	Status string `gorm:"column:status"` // Override the Status field to be string
}

// getActiveVCRules gets all active volume control rules for a pipeline
func (p *VCAlertProcessor) getActiveVCRules(pipelineId, tenantId uuid.UUID) ([]rule.Rule, error) {
	var vcRuleResults []VCRuleQueryResult
	err := p.db.WithContext(p.ctx).Table("vc_rule").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineId.String(), tenantId.String(), VCRuleStatusActive).
		Find(&vcRuleResults).Error

	if err != nil {
		return nil, err
	}

	// Convert to []rule.Rule by copying fields (Status will be 0 since it's int, but that's ok for our use case)
	var vcRules []rule.Rule
	for _, result := range vcRuleResults {
		vcRules = append(vcRules, result.Rule)
	}

	return vcRules, nil
}

// getRuleStats gets rule evaluation and match statistics from OpenSearch using the simplified pattern
func (p *VCAlertProcessor) getRuleStats(tenantId uuid.UUID, sourceId, ruleId string, startTime, endTime time.Time) (evaluated, matched int64, err error) {
	// Convert time to string format for statistics API
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Get evaluated events using the simplified statistics API
	evaluatedQuery := fmt.Sprintf(`name:"rule_events_evaluated" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)

	evaluatedResponse, err := statistics.GetStatsSum(p.ctx, evaluatedQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, fmt.Errorf("error getting evaluated events for rule %s: %w", ruleId, err)
	}

	// Get matched events using the simplified statistics API
	matchedQuery := fmt.Sprintf(`name:"rule_events_matched" AND tags.db_event_source_id:"%s" AND tags.rule_id:"%s"`,
		sourceId, ruleId)

	matchedResponse, err := statistics.GetStatsSum(p.ctx, matchedQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, fmt.Errorf("error getting matched events for rule %s: %w", ruleId, err)
	}

	evaluated = int64(evaluatedResponse.Sum)
	matched = int64(matchedResponse.Sum)

	return evaluated, matched, nil
}

// checkVCAlertConditions checks if VC rule conditions are met for alerting with configurable thresholds
// Returns a slice of alert types for all conditions that are met
func (p *VCAlertProcessor) checkVCAlertConditions(vcRule rule.Rule, todayEvaluated, todayMatched, yesterdayEvaluated, yesterdayMatched int64) []model.VCAlertType {

	var alertTypes []model.VCAlertType

	// Only alert if there's significant traffic to avoid noise from low-traffic sources
	if todayEvaluated < p.minimumEventsThreshold {
		logger.GetLogger().Debug("skipping alert due to low traffic",
			zap.String("ruleId", vcRule.ID.String()),
			zap.Int64("todayEvaluated", todayEvaluated),
			zap.Int64("minimumThreshold", p.minimumEventsThreshold))
		return alertTypes
	}

	// Condition 1: For DROP rule, if match % count for today is more than configured threshold than yesterday
	if strings.ToUpper(vcRule.ActionType) == VCRuleActionTypeDrop {
		if yesterdayMatched > 0 && todayMatched > 0 && yesterdayEvaluated > 0 && todayEvaluated > 0 {
			// Calculate yesterday's match percentage
			yesterdayMatchPercent := float64(yesterdayMatched) / float64(yesterdayEvaluated) * 100
			// Calculate today's match percentage
			todayMatchPercent := float64(todayMatched) / float64(todayEvaluated) * 100

			// Check if today's match percentage is more than configured threshold higher than yesterday's
			increasePercent := ((todayMatchPercent - yesterdayMatchPercent) / yesterdayMatchPercent) * 100

			if increasePercent > p.dropRuleIncreaseThreshold {
				logger.GetLogger().Info("DROP rule match increase detected",
					zap.String("ruleId", vcRule.ID.String()),
					zap.String("ruleName", vcRule.Name),
					zap.Float64("yesterdayMatchPercent", yesterdayMatchPercent),
					zap.Float64("todayMatchPercent", todayMatchPercent),
					zap.Float64("increasePercent", increasePercent),
					zap.Float64("threshold", p.dropRuleIncreaseThreshold))
				alertTypes = append(alertTypes, model.VCAlertTypeDropRuleIncrease)
			}
		}
	}

	// Condition 2: Rules have unmatched counts more than configured threshold of incoming data
	if todayEvaluated > 0 {
		todayUnmatched := todayEvaluated - todayMatched
		unmatchedPercent := (float64(todayUnmatched) / float64(todayEvaluated)) * 100

		if unmatchedPercent > p.highUnmatchedThreshold {
			logger.GetLogger().Info("high unmatched events detected",
				zap.String("ruleId", vcRule.ID.String()),
				zap.String("ruleName", vcRule.Name),
				zap.Int64("todayEvaluated", todayEvaluated),
				zap.Int64("todayUnmatched", todayUnmatched),
				zap.Float64("unmatchedPercent", unmatchedPercent),
				zap.Float64("threshold", p.highUnmatchedThreshold))
			alertTypes = append(alertTypes, model.VCAlertTypeHighUnmatched)
		}
	}

	if len(alertTypes) == 0 {
		logger.GetLogger().Debug("no alert conditions met",
			zap.String("ruleId", vcRule.ID.String()),
			zap.String("ruleName", vcRule.Name),
			zap.Int64("todayEvaluated", todayEvaluated),
			zap.Int64("todayMatched", todayMatched))
	}

	return alertTypes
}

// autoResolveHealthyRules auto-resolves existing VC alerts for healthy rules
func (p *VCAlertProcessor) autoResolveHealthyRules(tenantId string, healthyRuleIds []string) error {
	var alertsToDismiss []string
	for _, ruleId := range healthyRuleIds {
		q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionality:%s AND functionalityType:%s AND secondaryEntityId:%s",
			tenantId, alerts_async.VolumeControlRule.String(), alerts_async.VolumeDeviationChecker.String(), ruleId)
		openAlerts, _, err := os.Search(p.ctx, p.osClient, common.AlertsIndex, q)
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
		if err := p.alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
			logger.GetLogger().Error("error while auto-resolving VC alerts", zap.Error(err), zap.String("tenantId", tenantId))
			return err
		} else {
			logger.GetLogger().Info("auto-resolved VC alerts for healthy rules", zap.String("tenantId", tenantId), zap.Strings("alertIds", alertsToDismiss))
		}
	}
	return nil
}

// buildVCAlert builds an alert from VC alert model
func buildVCAlert(vcAlert model.VCAlert, highUnmatchedThreshold, dropRuleIncreaseThreshold float64) (*alerts_async.Alert, error) {
	var title, message string

	switch vcAlert.AlertType {
	case model.VCAlertTypeDropRuleIncrease:
		title = fmt.Sprintf("DROP rule '%s' match rate increased significantly", vcAlert.RuleName)
		message = fmt.Sprintf("DROP rule '%s' in pipeline '%s' has increased its match rate by more than %.1f%% compared to yesterday. "+
			"Today: %d matched out of %d evaluated (%.2f%%), Yesterday: %d matched out of %d evaluated. "+
			"This indicates the rule is dropping more events than expected.",
			vcAlert.RuleName, vcAlert.Pipeline.Name, dropRuleIncreaseThreshold, vcAlert.TodayMatched, vcAlert.TodayEvaluated, vcAlert.MatchedPercent,
			vcAlert.YesterdayMatched, vcAlert.YesterdayEvaluated)

	case model.VCAlertTypeHighUnmatched:
		title = fmt.Sprintf("Rule '%s' has high unmatched event rate", vcAlert.RuleName)
		message = fmt.Sprintf("Rule '%s' in pipeline '%s' has %.2f%% unmatched events today (%d unmatched out of %d evaluated). "+
			"This exceeds the %.1f%% threshold and indicates events are not being processed by any destination through route processor.",
			vcAlert.RuleName, vcAlert.Pipeline.Name, vcAlert.UnmatchedPercent,
			vcAlert.TodayEvaluated-vcAlert.TodayMatched, vcAlert.TodayEvaluated, highUnmatchedThreshold)

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

// getTodayTimeRange returns start and end time for today (00:00 to current time)
func getTodayTimeRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return todayStart, now
}

// getYesterdayTimeRange returns start and end time for yesterday (00:00 to 23:59)
func getYesterdayTimeRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	return yesterdayStart, yesterdayEnd
}
