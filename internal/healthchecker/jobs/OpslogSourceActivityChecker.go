package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	awsemail "github.com/databahn-ai/databahn-jobs/internal/healthchecker/aws"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var predefinedIntervals = []time.Duration{
	2 * time.Minute, //INTERNAL DEV ONLY
	30 * time.Minute,
	60 * time.Minute,
	180 * time.Minute,
	360 * time.Minute,
	720 * time.Minute,
	1440 * time.Minute,
}

func GetStatsByInterval(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_event_source_id.keyword"

	conf := os.GetConf()

	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return statistics.AggregateResponse{}, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	return aggObj, err
}

func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
func CheckEntityStats(ctx context.Context) error {
	endTime := time.Now()
	db := config.GetDB()
	configMap, err := helper.CreateEntityAlertsConfigMapByType(db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error creating EntityAlertsConfig map", zap.Error(err))
		return err
	}

	tenantMap, err := createTenantMap(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error while fetching TenantMap", zap.Error(err))
		return err
	}
	intervalCountMap := populateIntervalCountMap(configMap)
	filteredIntervals := filterIntervals(intervalCountMap)

	for _, interval := range filteredIntervals {
		err = processInterval(ctx, endTime, interval, configMap, tenantMap)
		if err != nil {
			return err
		}
	}

	return nil
}

func createTenantMap(ctx context.Context, db *gorm.DB) (map[string]string, error) {
	tenants, err := helper.GetAllTenants(ctx, db)
	if err != nil {
		return nil, err
	}

	tenantMap := make(map[string]string)
	for _, tenant := range tenants {
		tenantMap[tenant.ID.String()] = tenant.Name
	}
	return tenantMap, nil
}

func populateIntervalCountMap(configMap map[string]helper.EntityAlertsConfig) map[int]int {
	intervalCountMap := make(map[int]int)
	for _, conf := range configMap {
		intervalCountMap[conf.Interval]++
	}
	return intervalCountMap
}

func filterIntervals(intervalCountMap map[int]int) []time.Duration {
	var filteredIntervals []time.Duration
	for _, interval := range predefinedIntervals {
		if intervalCountMap[int(interval.Minutes())] > 0 {
			filteredIntervals = append(filteredIntervals, interval)
		} else {
			logging.GetLogger().Info("No entities found for interval", zap.Reflect("interval", interval.Minutes()))
		}
	}
	return filteredIntervals
}

func processInterval(ctx context.Context, endTime time.Time, interval time.Duration, configMap map[string]helper.EntityAlertsConfig, tenantMap map[string]string) error {

	startTime := endTime.Add(-interval)
	aggObj, err := GetStatsByInterval(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error getting stats by interval", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("got response from statistics store", zap.Reflect("interval", interval.Minutes()), zap.Reflect("response", aggObj))

	entityIdsToAlert := compareResults(configMap, tenantMap, aggObj, interval, startTime)
	if len(entityIdsToAlert) > 0 {
		err = sendAlertsForInactivity(ctx, entityIdsToAlert)
		if err != nil {
			return err
		}
	}

	return nil
}
func compareResults(configMap map[string]helper.EntityAlertsConfig, tenantMap map[string]string, aggObj statistics.AggregateResponse, interval time.Duration, startTime time.Time) []helper.EntityAlertsConfig {
	var logsourceIdsStatsReceived []string
	for key, value := range aggObj.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLogger().Error("error while getting value of stats for source", zap.String("type", reflect.TypeOf(value).String()))
			continue
		}
		if valueInt > 0 {
			_, err := uuid.Parse(key)
			if err != nil {
				continue
			}
			logsourceIdsStatsReceived = append(logsourceIdsStatsReceived, key)
		}
	}

	var entityIdsToAlert []helper.EntityAlertsConfig
	for _, config := range configMap {
		sourceId := config.EntityID.String()
		if config.Interval == int(interval.Minutes()) {
			isPresent := Contains(logsourceIdsStatsReceived, sourceId)
			isTimeAfter := startTime.After(config.LastCheckedTime.Add(interval))
			formattedMessage := fmt.Sprintf("Entity status isPresent: %t, isTimeAfter: %t", isPresent, isTimeAfter)
			logging.GetLogger().Info(formattedMessage)
			if !isPresent && isTimeAfter && !config.Disabled {
				config.TenantName = tenantMap[config.TenantID.String()]
				entityIdsToAlert = append(entityIdsToAlert, config)
			}
		}
	}

	return entityIdsToAlert
}

func sendAlertsForInactivity(ctx context.Context, entityIdsToAlert []helper.EntityAlertsConfig) error {

	for _, configObject := range entityIdsToAlert {

		var emailTo []string
		emailTo = append(emailTo, config.GetAppConfiguration().GetString(awsemail.OPSGini))
		configObject.Summary = fmt.Sprintf("No data received for %.2f hr or %v minutes ", float64(configObject.Interval)/60, configObject.Interval)
		formattedConfigObject, err := json.MarshalIndent(configObject, "", "  ")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error marshaling configObject to JSON", zap.Error(err))
			continue
		}

		title := fmt.Sprintf("Ingestion:%s:%s", configObject.TenantName, configObject.EntityName)
		var email = awsemail.EmailNotification{
			Recipients: &awsemail.Recipient{
				To: emailTo,
			},
			Body:    aws.String("<pre>" + string(formattedConfigObject) + "</pre>"),
			Subject: aws.String(title),
		}

		err = awsemail.SendEmail(ctx, email)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Info("Error sending notification")
			return err
		}

		logging.GetLoggerWithContext(ctx).Info("sent notification")

		//Update in DB
		logging.GetLoggerWithContext(ctx).Info("Updating LastCheckedTime", zap.String("entityId", configObject.EntityID.String()))
		err = config.GetDB().Model(&helper.EntityAlertsConfig{}).Where("entity_id = ?", configObject.EntityID).Update("last_checked_time", time.Now().UTC()).Error
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating LastCheckedTime", zap.Error(err), zap.String("entityId", configObject.EntityID.String()))
			return err
		}
		logging.GetLoggerWithContext(ctx).Info("Successfully updated LastCheckedTime", zap.String("entityId", configObject.EntityID.String()))
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}
