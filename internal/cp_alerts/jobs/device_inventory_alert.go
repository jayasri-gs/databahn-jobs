package jobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	logging "github.com/databahn-ai/go-logging/logger"
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

// Extended VcRuleFilter structure to include field, value, and operator
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
		tenantId := t.Id.String()
		if t.Id.String() != "f5e31bb8-af80-40d8-a0e4-16f12187e4e4" {
			continue
		}
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

		// Filter devices based on alert configuration
		filteredDevices, err := filterDevicesWithConfig(devices, alertConfig)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while filtering devices", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		if len(filteredDevices) == 0 {
			logging.GetLoggerWithContext(ctx).Info("no devices match alert criteria", zap.String("tenantId", tenantId))
			continue
		}

		// Create alerts for filtered devices
		alertsToSend := make([]*alerts_async.Alert, len(filteredDevices))
		for i, device := range filteredDevices {
			alert, err := buildDeviceAlert(device, t)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while building alert", zap.Error(err), zap.String("deviceId", device.Hostname))
				continue
			}
			alertsToSend[i] = alert
		}

		// Send alerts
		if len(alertsToSend) > 0 {
			err = alertsManager.SendAlerts(alertsToSend)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while sending alerts", zap.Error(err))
				continue
			}
		}
	}

	return nil
}

func filterDevicesWithConfig(devices []DeviceClass, alertConfig []entities.EntityAlertsConfig) ([]DeviceClass, error) {
	var filteredDevices []DeviceClass

	for _, device := range devices {
		for _, config := range alertConfig {
			if config.Config == nil || !config.Config.Enabled {
				continue
			}

			deviceInventoryConfig := config.Config.LogSourceDeviceInventoryAlertConfig
			fmt.Println(deviceInventoryConfig)
			if deviceInventoryConfig == nil || !deviceInventoryConfig.Enabled {
				continue
			}

			// Check if device matches the alert criteria
			if matchesAlertCriteria(device, deviceInventoryConfig) {
				filteredDevices = append(filteredDevices, device)
				break
			}
		}
	}

	return filteredDevices, nil
}

func matchesAlertCriteria(device DeviceClass, config *entities.LogSourceDeviceInventoryAlertConfig) bool {
	logging.GetLogger().Info("Checking alert criteria for device",
		zap.String("hostname", device.Hostname),
		zap.String("reputation", device.Reputation),
		zap.String("tenantId", device.TenantId))

	// Check reputation
	if len(config.ReputationsToAlert) > 0 {
		reputationMatch := false
		for _, rep := range config.ReputationsToAlert {
			if string(rep) == device.Reputation {
				reputationMatch = true
				break
			}
		}
		if !reputationMatch {
			logging.GetLogger().Info("Device reputation does not match",
				zap.String("hostname", device.Hostname),
				zap.String("deviceReputation", device.Reputation),
				zap.Any("allowedReputations", config.ReputationsToAlert))
			return false
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

		ruleFilter := convertVcRuleFilter(config.VcRuleFilters)
		if !evaluateRuleFilter(device, ruleFilter) {
			logging.GetLogger().Info("Device does not match rule filters",
				zap.String("hostname", device.Hostname))
			return false
		}
		logging.GetLogger().Info("Device matches rule filters",
			zap.String("hostname", device.Hostname))
	} else {
		logging.GetLogger().Info("No rule filters configured, skipping rule evaluation",
			zap.String("hostname", device.Hostname))
	}

	// Check include/exclude
	if config.IncludeExclude == entities.Exclude {
		logging.GetLogger().Info("Device excluded by include/exclude setting",
			zap.String("hostname", device.Hostname),
			zap.String("includeExclude", string(config.IncludeExclude)))
		return false
	}

	logging.GetLogger().Info("Device matches all alert criteria",
		zap.String("hostname", device.Hostname))
	return true
}

func convertVcRuleFilter(entitiesFilter *entities.VcRuleFilter) *VcRuleFilter {
	if entitiesFilter == nil {
		return nil
	}

	result := &VcRuleFilter{
		Combinator: entitiesFilter.Combinator,
	}

	logging.GetLogger().Info("Converting rule filter",
		zap.Int("ruleCount", len(entitiesFilter.Rules)),
		zap.String("combinator", entitiesFilter.Combinator))

	// Convert nested rules
	for i, rule := range entitiesFilter.Rules {
		convertedRule := convertVcRuleFilter(&rule)
		if convertedRule != nil {
			result.Rules = append(result.Rules, *convertedRule)
			logging.GetLogger().Info("Converted rule",
				zap.Int("ruleIndex", i),
				zap.String("field", convertedRule.Field),
				zap.String("operator", convertedRule.Operator),
				zap.String("value", convertedRule.Value))
		}
	}

	return result
}

func evaluateRuleFilter(device DeviceClass, rule *VcRuleFilter) bool {
	if rule == nil {
		logging.GetLogger().Info("No rule filter to evaluate")
		return true
	}

	// If this is a leaf node (has field, value, operator)
	if rule.Field != "" && rule.Value != "" && rule.Operator != "" {
		result := evaluateCondition(device, rule.Field, rule.Operator, rule.Value)
		logging.GetLogger().Info("Evaluated leaf rule",
			zap.String("hostname", device.Hostname),
			zap.String("field", rule.Field),
			zap.String("operator", rule.Operator),
			zap.String("value", rule.Value),
			zap.Bool("result", result))
		return result
	}

	// If this is a composite node (has rules)
	if len(rule.Rules) > 0 {
		combinator := rule.Combinator
		if combinator == "" {
			combinator = "AND"
		}

		logging.GetLogger().Info("Evaluating composite rule",
			zap.String("hostname", device.Hostname),
			zap.String("combinator", combinator),
			zap.Int("ruleCount", len(rule.Rules)))

		if combinator == "AND" {
			for i, subRule := range rule.Rules {
				if !evaluateRuleFilter(device, &subRule) {
					logging.GetLogger().Info("AND rule failed",
						zap.String("hostname", device.Hostname),
						zap.Int("ruleIndex", i))
					return false
				}
			}
			logging.GetLogger().Info("All AND rules passed",
				zap.String("hostname", device.Hostname))
			return true
		} else if combinator == "OR" {
			for i, subRule := range rule.Rules {
				if evaluateRuleFilter(device, &subRule) {
					logging.GetLogger().Info("OR rule passed",
						zap.String("hostname", device.Hostname),
						zap.Int("ruleIndex", i))
					return true
				}
			}
			logging.GetLogger().Info("No OR rules passed",
				zap.String("hostname", device.Hostname))
			return false
		}
	} else {
		logging.GetLogger().Info("Empty rules array, returning true",
			zap.String("hostname", device.Hostname))
	}

	return true
}

func evaluateCondition(device DeviceClass, field, operator, value string) bool {
	var deviceValue string

	// Get the device field value
	switch field {
	case "Hostname":
		deviceValue = device.Hostname
	case "SourceID":
		deviceValue = device.SourceID
	case "TenantId":
		deviceValue = device.TenantId
	case "Reputation":
		deviceValue = device.Reputation
	case "SourceName":
		deviceValue = device.SourceName
	default:
		return false
	}

	// Evaluate the condition based on operator
	switch operator {
	case "==", "=":
		return deviceValue == value
	case "!=":
		return deviceValue != value
	case "contains":
		return strings.Contains(strings.ToLower(deviceValue), strings.ToLower(value))
	case "startsWith":
		return strings.HasPrefix(strings.ToLower(deviceValue), strings.ToLower(value))
	case "endsWith":
		return strings.HasSuffix(strings.ToLower(deviceValue), strings.ToLower(value))
	default:
		return false
	}
}

func buildDeviceAlert(device DeviceClass, t tenant.Tenant) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Device Inventory Alert - %s - %s", t.Name, time.Now().Format(time.DateOnly))
	message := fmt.Sprintf("Device %s has reputation %s", device.Hostname, device.Reputation)

	return alerts_async.NewAlert(alerts_async.LogSource,
		alerts_async.WithEntityDetails(
			device.SourceID,
			device.Hostname,
			"", // dataPlaneId
			device.TenantId,
		),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.SilentDeviceChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10003, ""),
	)
}

func getDevices(ctx context.Context, client *opensearch.Client, index string, query string, pageSize int, searchAfter []any, tenantName string) ([]DeviceClass, []any, error) {

	logging.GetLogger().Info("query", zap.String("query", query))
	logging.GetLogger().Info("pageSize", zap.Int("pageSize", pageSize))
	logging.GetLogger().Info("searchAfter", zap.Any("searchAfter", searchAfter))
	logging.GetLogger().Info("index", zap.String("index", index))

	var silentDevices []DeviceClass
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
		silentDevices = append(silentDevices, DeviceClass{
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

func fetchDevices(ctx context.Context, tenantId string, tenantName string, sources []uuid.UUID) ([]DeviceClass, error) {

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

	var allSilentDevices []DeviceClass
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
