package jobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type DeviceClass struct {
	Hostname   string `json:"hostname"`
	MinTime    int64  `json:"min_time"`
	MaxTime    int64  `json:"max_time"`
	SourceID   string `json:"source_id"`
	TenantId   string `json:"tenant_id"`
	Reputation string `json:"reputation"`
	TenantName string `json:"tenant_name"`
	SourceName string `json:"source_name"`
}

var deviceFieldMap = map[string]string{
	"hostname":   "Hostname",
	"sourceid":   "SourceID",
	"tenantid":   "TenantId",
	"reputation": "Reputation",
	"sourcename": "SourceName",
}

type RuleResult struct {
	IsMatch bool
}

type StringHelpers struct{}

func (h *StringHelpers) Contains(s1, s2 string) bool {
	return strings.Contains(strings.ToLower(s1), strings.ToLower(s2))
}

func (h *StringHelpers) StartsWith(s1, s2 string) bool {
	return strings.HasPrefix(strings.ToLower(s1), strings.ToLower(s2))
}

func (h *StringHelpers) EndsWith(s1, s2 string) bool {
	return strings.HasSuffix(strings.ToLower(s1), strings.ToLower(s2))
}

type VcRuleFilter struct {
	Field      string         `json:"field"`
	Value      string         `json:"value"`
	Operator   string         `json:"operator"`
	Rules      []VcRuleFilter `json:"rules"`
	Combinator string         `json:"combinator"`
}

func SendAlertForDeviceLevelAlert(ctx context.Context) error {
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		return err
	}

	alertConfig, _, tenantIdToSourceMap, err := getLogSourceIdsFromEntityAlertConfig(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching entity alert config", zap.Error(err))
		return err
	}

	// Create alerts manager
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)

	for _, t := range tenants {
		if t.Id.String() != "1be4494f-0251-4bf1-ad18-e09adc141aea" {
			continue
		}
		tenantId := t.Id.String()
		logging.GetLoggerWithContext(ctx).Info("Processing tenant", zap.String("tenantId", tenantId))

		if len(alertConfig) == 0 {
			logging.GetLoggerWithContext(ctx).Info("no entity alert config found for tenant", zap.String("tenantId", tenantId))
			continue
		}

		alertConfigs, err := entities.ReadSourceEntityConfigs(db, "LOG_SOURCE_DEVICE_INVENTORY", t.Id, tenantIdToSourceMap[tenantId])
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while reading log source entity configs", zap.Error(err))
			return err
		}
		alertConfigsBySourceId := make(map[uuid.UUID]entities.EntityAlertsConfig)
		for _, conf := range alertConfigs {
			alertConfigsBySourceId[conf.EntityID] = conf
		}

		devices, err := fetchDevices(ctx, tenantId, t.Name, tenantIdToSourceMap[tenantId])
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching devices", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		if len(devices) == 0 {
			logging.GetLoggerWithContext(ctx).Info("no devices found for tenant", zap.String("tenantId", tenantId))
			continue
		}
		sourceIdToDevice := make(map[string][]model.DeviceClass)
		for _, device := range devices {
			sourceIdToDevice[device.SourceID] = append(sourceIdToDevice[device.SourceID], device)
		}
		filteredDevices, err := filterDevicesWithConfig(devices, alertConfigsBySourceId)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while filtering devices", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		if len(filteredDevices) == 0 {
			logging.GetLoggerWithContext(ctx).Info("no devices match alert criteria", zap.String("tenantId", tenantId))
			continue
		}
		var alertsToSend []*model.DeviceInventoryAlerts
		for _, alrt := range filteredDevices {
			alertsToSend = append(alertsToSend, &model.DeviceInventoryAlerts{
				DeviceClass: &alrt,
			})
		}

		// Send alerts
		if len(filteredDevices) > 0 {
			err = sendInAppAlertsForDeviceInventory(alertsToSend, alertsManager)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while sending alerts", zap.Error(err))
				continue
			}
		}
	}

	return nil
}

func filterDevicesWithConfig(devices []model.DeviceClass, alertConfigsBySourceId map[uuid.UUID]entities.EntityAlertsConfig) ([]model.DeviceClass, error) {
	var filteredDevices []model.DeviceClass

	for _, device := range devices {
		// Convert device.SourceID string to UUID to match with alertConfigsBySourceId
		sourceID, err := uuid.Parse(device.SourceID)
		if err != nil {
			logging.GetLogger().Error("error parsing device source ID", zap.Error(err), zap.String("sourceID", device.SourceID))
			continue
		}

		// Get the alert configuration for this specific source
		alrtconfig, exists := alertConfigsBySourceId[sourceID]
		if !exists {
			logging.GetLogger().Info("no alert configuration found for source", zap.String("sourceID", device.SourceID))
			continue
		}

		if alrtconfig.Config == nil || !alrtconfig.Config.Enabled {
			logging.GetLogger().Info("alert configuration is nil or disabled for source", zap.String("sourceID", device.SourceID))
			continue
		}

		// Check if device matches the alert criteria for this specific source
		matches, err := matchesAlertCriteria(device, alrtconfig.Config.LogSourceDeviceInventoryAlertConfig)
		if err != nil {
			logging.GetLogger().Error("error checking alert criteria", zap.Error(err), zap.String("hostname", device.Hostname))
			continue
		}
		if matches {
			filteredDevices = append(filteredDevices, device)
		}
	}

	return filteredDevices, nil
}

func matchesAlertCriteria(device model.DeviceClass, config *entities.LogSourceDeviceInventoryAlertConfig) (bool, error) {
	logging.GetLogger().Info("Checking alert criteria for device",
		zap.String("hostname", device.Hostname),
		zap.String("reputation", device.Reputation),
		zap.String("tenantId", device.TenantId))

	// Check reputation
	if len(config.ReputationsToAlert) > 0 {
		reputationMatch := false
		for _, rep := range config.ReputationsToAlert {
			reputationLowerCase := strings.ToLower(string(rep))
			if reputationLowerCase == device.Reputation {
				reputationMatch = true
				break
			}
		}
		if !reputationMatch {
			logging.GetLogger().Info("Device reputation does not match",
				zap.String("hostname", device.Hostname),
				zap.String("deviceReputation", device.Reputation),
				zap.Any("allowedReputations", config.ReputationsToAlert))
			return false, nil
		}
		logging.GetLogger().Info("Device reputation matches",
			zap.String("hostname", device.Hostname),
			zap.String("reputation", device.Reputation))
	}

	// Check rule filters
	if config.VcRuleFilters != nil {
		logging.GetLogger().Info("Processing rule filters",
			zap.String("hostname", device.Hostname),
			zap.Int("ruleCount", len(config.VcRuleFilters.Rules)),
			zap.String("combinator", config.VcRuleFilters.Combinator))

		ruleMatches, err := evaluateDeviceAgainstFiltersWithGrule(device, config.VcRuleFilters)
		if err != nil {
			return false, fmt.Errorf("error evaluating grule rules: %w", err)
		}
		if !ruleMatches {
			logging.GetLogger().Info("Device does not match rule filters",
				zap.String("hostname", device.Hostname))
			return false, nil
		}
		logging.GetLogger().Info("Device matches rule filters",
			zap.String("hostname", device.Hostname))
	} else {
		logging.GetLogger().Info("No rule filters configured, skipping rule evaluation",
			zap.String("hostname", device.Hostname))
	}

	logging.GetLogger().Info("Device matches all alert criteria",
		zap.String("hostname", device.Hostname))
	return true, nil
}

func evaluateDeviceAgainstFiltersWithGrule(device model.DeviceClass, filters *entities.VcRuleFilter) (bool, error) {
	if filters == nil || len(filters.Rules) == 0 {
		return true, nil
	}

	return evaluateFilterRecursive(device, filters)
}

// Simple native Go rule evaluator - much faster than Grule
func evaluateFilterRecursive(device model.DeviceClass, filter *entities.VcRuleFilter) (bool, error) {
	// Handle rule groups (nested rules with combinators)
	if len(filter.Rules) > 0 {
		return evaluateRuleGroup(device, filter)
	}

	// Handle single rule
	return evaluateSingleRule(device, filter)
}

func evaluateRuleGroup(device model.DeviceClass, filter *entities.VcRuleFilter) (bool, error) {
	if len(filter.Rules) == 0 {
		return true, nil
	}

	// Evaluate all rules in the group
	results := make([]bool, len(filter.Rules))
	for i, rule := range filter.Rules {
		result, err := evaluateFilterRecursive(device, &rule)
		if err != nil {
			return false, err
		}
		results[i] = result
	}

	// Apply combinator (AND by default, OR if specified)
	if strings.ToUpper(filter.Combinator) == "OR" {
		// OR logic - return true if any rule matches
		for _, result := range results {
			if result {
				return true, nil
			}
		}
		return false, nil
	} else {
		// AND logic - return true only if all rules match
		for _, result := range results {
			if !result {
				return false, nil
			}
		}
		return true, nil
	}
}

func evaluateSingleRule(device model.DeviceClass, filter *entities.VcRuleFilter) (bool, error) {
	// Get the field value from the device
	fieldValue, err := getDeviceFieldValue(device, filter.Field)
	if err != nil {
		return false, err
	}

	// Apply the operator
	return applyOperator(fieldValue, filter.Operator, filter.Value)
}

func getDeviceFieldValue(device model.DeviceClass, fieldName string) (string, error) {
	normalizedField := strings.ToLower(strings.ReplaceAll(fieldName, " ", ""))

	switch normalizedField {
	case "hostname":
		return device.Hostname, nil
	case "sourceid":
		return device.SourceID, nil
	case "tenantid":
		return device.TenantId, nil
	case "reputation":
		return device.Reputation, nil
	case "sourcename":
		return device.SourceName, nil
	default:
		return "", fmt.Errorf("unsupported field: %s", fieldName)
	}
}

func applyOperator(fieldValue, operator, expectedValue string) (bool, error) {
	switch operator {
	case "==", "=":
		return fieldValue == expectedValue, nil
	case "!=":
		return fieldValue != expectedValue, nil
	case "contains":
		return strings.Contains(strings.ToLower(fieldValue), strings.ToLower(expectedValue)), nil
	case "doesNotContain":
		return !strings.Contains(strings.ToLower(fieldValue), strings.ToLower(expectedValue)), nil
	case "startsWith":
		return strings.HasPrefix(strings.ToLower(fieldValue), strings.ToLower(expectedValue)), nil
	case "endsWith":
		return strings.HasSuffix(strings.ToLower(fieldValue), strings.ToLower(expectedValue)), nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

func getDevices(ctx context.Context, client *opensearch.Client, index string, query string, pageSize int, searchAfter []any, tenantName string) ([]model.DeviceClass, []any, error) {

	logging.GetLogger().Info("query", zap.String("query", query))
	logging.GetLogger().Info("pageSize", zap.Int("pageSize", pageSize))
	logging.GetLogger().Info("searchAfter", zap.Any("searchAfter", searchAfter))
	logging.GetLogger().Info("index", zap.String("index", index))

	var silentDevices []model.DeviceClass
	res, newSearchAfter, err := os.SearchPaginated(ctx, client, index, query, pageSize, searchAfter, []os.Sort{{Field: "max_time", Order: "desc"}})
	if err != nil {
		return nil, nil, err
	}

	if len(res) == 0 {
		return nil, nil, nil
	}

	var deviceInventoryList []statistics.DeviceInventoryDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &deviceInventoryList})
	err = decoder.Decode(res)
	if err != nil {
		return nil, nil, err
	}

	for _, device := range deviceInventoryList {
		silentDevices = append(silentDevices, model.DeviceClass{
			Hostname:   device.Hostname,
			MinTime:    device.MinTime,
			MaxTime:    device.MaxTime,
			SourceID:   device.SourceId,
			TenantId:   device.TenantId,
			Reputation: device.Reputation,
			TenantName: tenantName,
		})
	}
	logging.GetLoggerWithContext(ctx).Info("silent devices fetched", zap.Int("count", len(silentDevices)))

	return silentDevices, newSearchAfter, nil
}

func fetchDevices(ctx context.Context, tenantId string, tenantName string, sources []uuid.UUID) ([]model.DeviceClass, error) {

	// Build the query using getQueryFromFilters
	query, err := getQuery(sources, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while building query", zap.Error(err))
		return nil, err
	}

	logging.GetLoggerWithContext(ctx).Info("query built", zap.String("query", query))

	var searchAfter []any
	pageSize := 100
	index := "db_insights_sights_sourcehostname_" + tenantId

	var allSilentDevices []model.DeviceClass
	for {
		silentDevices, newSearchAfter, err := getDevices(ctx, os.GetClient(), index, query, pageSize, searchAfter, tenantName)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", index))
			return nil, err
		}

		allSilentDevices = append(allSilentDevices, silentDevices...)

		if len(silentDevices) == 0 || newSearchAfter == nil {
			break
		}
		searchAfter = newSearchAfter
	}

	logging.GetLoggerWithContext(ctx).Info("silent devices fetched", zap.Any("silentDevices", allSilentDevices))

	return allSilentDevices, nil
}

func getQuery(sources []uuid.UUID, tenantId string) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		// Convert UUIDs to strings for the query
		sourceStrings := make([]string, len(sources))
		for i, source := range sources {
			sourceStrings[i] = source.String()
		}
		q += ` AND source_id: ` + "(" + strings.Join(sourceStrings, " OR ") + ")"
	}

	// Set endTime to 4 hours before the current time (previous day)
	endTime := time.Now().Add(-4 * time.Hour).Format(time.RFC3339)

	logging.GetLogger().Info("endTime", zap.String("endTime", endTime))

	// Parse endTime and add it to the query
	t, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return "", fmt.Errorf("error parsing endTime: %v", err)
	}
	endTimeEpoch := t.UnixMilli()

	logging.GetLogger().Info("endTimeEpoch", zap.Int64("endTimeEpoch", endTimeEpoch))

	q += ` AND max_time:<` + strconv.FormatInt(endTimeEpoch, 10)

	return q, nil
}

func getLogSourceIdsFromEntityAlertConfig(ctx context.Context, db *gorm.DB) ([]entities.EntityAlertsConfig, map[string][]entities.EntityAlertsConfig, map[string][]uuid.UUID, error) {
	var entityAlertConfig []entities.EntityAlertsConfig

	tenantIdToConfig := make(map[string][]entities.EntityAlertsConfig)
	tenantIdToSource := make(map[string][]uuid.UUID)

	err := db.Where("alert_type = ? ", "LOG_SOURCE_DEVICE_INVENTORY").Find(&entityAlertConfig).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching entity alert config", zap.Error(err))
		return nil, nil, nil, err
	}
	if len(entityAlertConfig) == 0 {
		return nil, nil, nil, nil
	}
	for _, alertConfig := range entityAlertConfig {
		tenantIdToConfig[alertConfig.TenantID.String()] = append(tenantIdToConfig[alertConfig.TenantID.String()], alertConfig)
	}
	for _, alertConfig := range entityAlertConfig {
		tenantIdToSource[alertConfig.TenantID.String()] = append(tenantIdToSource[alertConfig.TenantID.String()], alertConfig.EntityID)
	}
	return entityAlertConfig, tenantIdToConfig, tenantIdToSource, nil

}

func sendInAppAlertsForDeviceInventory(devicesToAlert []*model.DeviceInventoryAlerts, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(devicesToAlert))
	for i, dvcAlert := range devicesToAlert {
		newAlert, err := buildDeviceAlert(*dvcAlert)
		if err != nil {
			logging.GetLogger().Error("error while building alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	err := alertsManager.SendAlerts(alertsToSave)
	if err != nil {
		logging.GetLogger().Error("error while sending inactive source alert", zap.Error(err))
		return err
	}
	return nil
}

func buildDeviceAlert(dia model.DeviceInventoryAlerts) (*alerts_async.Alert, error) {
	details := fmt.Sprintf("Device Inventory Alert - Device %s has reputation %s", dia.GetEntityName(), dia.DeviceClass.Reputation)
	functionality := alerts_async.LogSource

	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(dia),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.SilentDeviceChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10003, ""),
	)
}
