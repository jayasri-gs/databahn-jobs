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
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
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
	Hostname    string `json:"hostname"`
	MinTime     int64  `json:"min_time"`
	MaxTime     int64  `json:"max_time"`
	SourceID    string `json:"source_id"`
	TenantId    string `json:"tenant_id"`
	Reputation  string `json:"reputation"`
	TenantName  string `json:"tenant_name"`
	SourceName  string `json:"source_name"`
	DataPlaneId string `json:"data_plane_id"`
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

		// Process each source separately with its own configuration
		var consolidatedAlerts []*model.SourceDeviceInventoryAlert
		for _, entityAlertConfig := range alertConfigs {
			if entityAlertConfig.Config == nil || !entityAlertConfig.Config.Enabled {
				logging.GetLoggerWithContext(ctx).Info("alert configuration is nil or disabled for source",
					zap.String("sourceID", entityAlertConfig.EntityID.String()))
				continue
			}

			// Fetch devices for this specific source with its configuration
			sourceSlice := []uuid.UUID{entityAlertConfig.EntityID}
			devices, totalDevices, err := fetchDevicesWithConfig(ctx, tenantId, t.Name, sourceSlice, entityAlertConfig.Config.LogSourceDeviceInventoryAlertConfig)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while fetching devices with config",
					zap.Error(err), zap.String("tenantId", tenantId), zap.String("sourceID", entityAlertConfig.EntityID.String()))
				continue
			}

			if len(devices) == 0 {
				logging.GetLoggerWithContext(ctx).Info("no devices found for source",
					zap.String("tenantId", tenantId), zap.String("sourceID", entityAlertConfig.EntityID.String()))
				continue
			}

			logging.GetLoggerWithContext(ctx).Info("devices found for alerting",
				zap.String("tenantId", tenantId),
				zap.String("sourceID", entityAlertConfig.EntityID.String()),
				zap.Int("count", len(devices)))

			// Create consolidated alert for this source
			if len(devices) > 0 {
				// Get source name and data plane ID from the first device
				sourceName := devices[0].SourceName
				dataPlaneId := devices[0].DataPlaneId

				var deviceSubset []model.DeviceClass
				if len(devices) > 5 {
					// If more than 5 devices, take only the first 5 for the alert
					deviceSubset = devices[:5]
				} else {
					// Otherwise, take all devices
					deviceSubset = devices
				}

				consolidatedAlert := model.NewSourceDeviceInventoryAlert(
					entityAlertConfig.EntityID.String(),
					sourceName,
					tenantId,
					t.Name,
					dataPlaneId,
					deviceSubset,
				)
				// Set total count to show all devices that matched
				consolidatedAlert.TotalCount = totalDevices
				consolidatedAlert.RemainingCount = totalDevices - len(devices)
				consolidatedAlerts = append(consolidatedAlerts, consolidatedAlert)
			}
		}

		// Send consolidated alerts
		if len(consolidatedAlerts) > 0 {
			err = sendInAppAlertsForDeviceInventoryConsolidated(consolidatedAlerts, alertsManager)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while sending consolidated alerts", zap.Error(err))
				continue
			}
		}
	}

	return nil
}

func getDevices(ctx context.Context, client *opensearch.Client, index string, query string, tenantName string) ([]model.DeviceClass, int, error) {

	logging.GetLogger().Info("query", zap.String("query", query))
	logging.GetLogger().Info("index", zap.String("index", index))

	var silentDevices []model.DeviceClass
	res, totalHits, err := os.Search(ctx, client, index, query)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", index))
		return nil, 0, err
	}

	if len(res) == 0 {
		return nil, 0, nil
	}

	var deviceInventoryList []statistics.DeviceInventoryDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &deviceInventoryList})
	err = decoder.Decode(res)
	if err != nil {
		return nil, 0, err
	}

	for _, device := range deviceInventoryList {
		silentDevices = append(silentDevices, model.DeviceClass{
			Hostname:    device.Hostname,
			MinTime:     device.MinTime,
			MaxTime:     device.MaxTime,
			SourceID:    device.SourceId,
			TenantId:    device.TenantId,
			Reputation:  device.Reputation,
			TenantName:  tenantName,
			DataPlaneId: "",
		})
	}
	logging.GetLoggerWithContext(ctx).Info("silent devices fetched", zap.Int("count", len(silentDevices)))
	return silentDevices, totalHits, nil
}

// New function to fetch devices with specific alert config
func fetchDevicesWithConfig(ctx context.Context, tenantId string, tenantName string, sources []uuid.UUID, config *entities.LogSourceDeviceInventoryAlertConfig) ([]model.DeviceClass, int, error) {
	// Build the query using the new function with filters
	query, err := getQueryWithFilters(sources, tenantId, config)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while building query", zap.Error(err))
		return nil, 0, err
	}

	logging.GetLoggerWithContext(ctx).Info("query built with filters", zap.String("query", query))

	index := "db_insights_sights_sourcehostname_" + tenantId

	devices, totalHostnames, err := getDevices(ctx, os.GetClient(), index, query, tenantName)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", index))
		return nil, 0, err
	}

	// Fetch source names and populate them in devices
	err = populateSourceNames(ctx, devices, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while populating source names", zap.Error(err))
		return nil, 0, err
	}

	logging.GetLoggerWithContext(ctx).Info("devices fetched with config", zap.Int("count", len(devices)))
	return devices, totalHostnames, nil
}

// New function to get query with vcRuleFilters
func getQueryWithFilters(sources []uuid.UUID, tenantId string, config *entities.LogSourceDeviceInventoryAlertConfig) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		// Convert UUIDs to strings for the query
		sourceStrings := make([]string, len(sources))
		for i, source := range sources {
			sourceStrings[i] = source.String()
		}
		q += ` AND source_id: ` + "(" + strings.Join(sourceStrings, " OR ") + ")"
	}

	// Set endTime to 4 hours before the current time
	endTime := time.Now().Add(-4 * time.Hour).Format(time.RFC3339)
	t, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return "", fmt.Errorf("error parsing endTime: %v", err)
	}
	endTimeEpoch := t.UnixMilli()
	q += ` AND max_time:<` + strconv.FormatInt(endTimeEpoch, 10)

	// Only add filters if config is provided
	if config != nil {
		// Add reputation filter if specified
		if len(config.ReputationsToAlert) > 0 {
			reputations := make([]string, len(config.ReputationsToAlert))
			for i, rep := range config.ReputationsToAlert {
				reputations[i] = strings.ToLower(string(rep))
			}
			q += ` AND reputation: (` + strings.Join(reputations, " OR ") + `)`
		}

		// Add vcRuleFilters as OpenSearch query
		if config.VcRuleFilters != nil {
			ruleQuery, err := convertVcRuleFiltersToOpenSearchQuery(config.VcRuleFilters)
			if err != nil {
				return "", fmt.Errorf("error converting vcRuleFilters to query: %w", err)
			}

			if ruleQuery != "" {
				// Handle include/exclude logic
				if config.IncludeExclude == "EXCLUDE" {
					// If EXCLUDE, we want devices that DON'T match the rule
					q += ` AND NOT (` + ruleQuery + `)`
					logging.GetLogger().Info("Applied EXCLUDE logic - devices NOT matching the rule will be included", zap.String("ruleQuery", ruleQuery))
				} else {
					// If INCLUDE (or default), we want devices that DO match the rule
					q += ` AND (` + ruleQuery + `)`
					logging.GetLogger().Info("Applied INCLUDE logic - devices matching the rule will be included", zap.String("ruleQuery", ruleQuery))
				}
			}
		}
	}

	logging.GetLogger().Info("Generated OpenSearch query", zap.String("query", q), zap.String("tenantId", tenantId))
	return q, nil
}

// Convert vcRuleFilters to OpenSearch query syntax
func convertVcRuleFiltersToOpenSearchQuery(filter *entities.VcRuleFilter) (string, error) {
	if filter == nil {
		return "", nil
	}

	// Handle rule groups (nested rules with combinators)
	if len(filter.Rules) > 0 {
		return convertRuleGroupToQuery(filter)
	}

	// Handle single rule
	return convertSingleRuleToQuery(filter)
}

func convertRuleGroupToQuery(filter *entities.VcRuleFilter) (string, error) {
	if len(filter.Rules) == 0 {
		return "", nil
	}

	var ruleQueries []string
	for _, rule := range filter.Rules {
		ruleQuery, err := convertVcRuleFiltersToOpenSearchQuery(&rule)
		if err != nil {
			return "", err
		}
		if ruleQuery != "" {
			ruleQueries = append(ruleQueries, ruleQuery)
		}
	}

	if len(ruleQueries) == 0 {
		return "", nil
	}

	if len(ruleQueries) == 1 {
		return ruleQueries[0], nil
	}

	// Apply combinator (AND by default, OR if specified)
	combinator := " AND "
	if strings.ToUpper(filter.Combinator) == "OR" {
		combinator = " OR "
	}

	return "(" + strings.Join(ruleQueries, combinator) + ")", nil
}

func convertSingleRuleToQuery(filter *entities.VcRuleFilter) (string, error) {
	fieldName := getOpenSearchFieldName(filter.Field)
	if fieldName == "" {
		return "", fmt.Errorf("unsupported field: %s", filter.Field)
	}

	// Add quotes around value if fieldName is key1 (hostname field)
	value := filter.Value
	if fieldName == "key1" {
		value = fmt.Sprintf(`"%s"`, filter.Value)
	}

	switch filter.Operator {
	case "contains":
		// Use wildcard query for contains
		return fmt.Sprintf(`%s: *%s*`, fieldName, value), nil
	case "doesNotContain":
		// Use NOT with wildcard for does not contain
		return fmt.Sprintf(`NOT %s: *%s*`, fieldName, value), nil
	default:
		return "", fmt.Errorf("unsupported operator: %s", filter.Operator)
	}
}

func getOpenSearchFieldName(fieldName string) string {
	normalizedField := strings.ToLower(strings.ReplaceAll(fieldName, " ", ""))

	switch normalizedField {
	case "hostname":
		return "key1"
	case "sourceid":
		return "source_id"
	case "tenantid":
		return "tenant_id"
	case "reputation":
		return "reputation"
	case "sourcename":
		return "source_name"
	default:
		return ""
	}
}

func populateSourceNames(ctx context.Context, devices []model.DeviceClass, tenantId string) error {
	db := config.GetDB()

	// Get all log sources for the tenant
	logSources, err := helper.GetAllLogSourcesByTenantId(ctx, db, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching log sources", zap.Error(err))
		return err
	}

	// Create maps of source ID to source name and data plane ID
	sourceIdToName := make(map[string]string)
	sourceIdToDataPlaneId := make(map[string]string)
	for _, source := range logSources {
		sourceIdToName[source.ID.String()] = source.Name
		sourceIdToDataPlaneId[source.ID.String()] = source.DataPlaneID.String()
	}

	// Populate source names and data plane IDs in devices
	for i := range devices {
		if sourceName, exists := sourceIdToName[devices[i].SourceID]; exists {
			devices[i].SourceName = sourceName
		} else {
			// If source name not found, use source ID as fallback
			devices[i].SourceName = devices[i].SourceID
		}

		if dataPlaneId, exists := sourceIdToDataPlaneId[devices[i].SourceID]; exists {
			devices[i].DataPlaneId = dataPlaneId
		} else {
			// If data plane ID not found, use empty string as fallback
			devices[i].DataPlaneId = ""
		}
	}

	return nil
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

func sendInAppAlertsForDeviceInventoryConsolidated(consolidatedAlerts []*model.SourceDeviceInventoryAlert, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(consolidatedAlerts))
	for i, consolidatedAlert := range consolidatedAlerts {
		newAlert, err := buildConsolidatedDeviceAlert(*consolidatedAlert)
		if err != nil {
			logging.GetLogger().Error("error while building consolidated alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	err := alertsManager.SendAlerts(alertsToSave)
	if err != nil {
		logging.GetLogger().Error("error while sending consolidated device inventory alert", zap.Error(err))
		return err
	}
	return nil
}

func buildConsolidatedDeviceAlert(sdia model.SourceDeviceInventoryAlert) (*alerts_async.Alert, error) {
	// Create a detailed message with source info and device details
	var deviceDetails strings.Builder
	deviceDetails.WriteString(fmt.Sprintf("Source: %s\n", sdia.SourceName))
	deviceDetails.WriteString(fmt.Sprintf("Total devices matching alert criteria: %d\n\n", sdia.TotalCount))

	deviceDetails.WriteString("Sample devices (showing up to 5):\n")
	for i, device := range sdia.TopDevices {
		deviceDetails.WriteString(fmt.Sprintf("%d. %s (Reputation: %s)\n", i+1, device.Hostname, device.Reputation))
	}

	if sdia.RemainingCount > 0 {
		deviceDetails.WriteString(fmt.Sprintf("\n... and %d more devices matching the criteria", sdia.RemainingCount))
	}

	title := fmt.Sprintf("Device Inventory Alert - Source %s has %d devices matching alert criteria", sdia.SourceName, sdia.TotalCount)
	message := deviceDetails.String()

	functionality := alerts_async.LogSource

	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(sdia),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.DeviceReputationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10003, ""),
	)
}
