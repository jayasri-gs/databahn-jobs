package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	awsemail "github.com/databahn-ai/databahn-jobs/internal/healthchecker/aws"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

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

	alias := conf.StatsIndex + "_alias_f5e31bb8-af80-40d8-a0e4-16f12187e4e4"
	searchResponse, err := os.MakeSearchCall(ctx, alias, &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	//	logging.GetLoggerWithContext(ctx).Info("got response from statistics store", zap.Reflect("response", aggObj))
	return aggObj, err
}

func checkInactivityAlertExistsForGivenLogSourcesV2(ctx context.Context, logsources []string) ([]statistics.AlertDocument, error) {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}
	q := `dismissed:false AND functionalityEntityId:` + "(" + strings.Join(logsources, " OR ") + ")" + ` AND functionalityType:` + alerts_common.LogSourceStatsNotReceived

	res, err := os.Search(ctx, client, common.AlertsIndex, q)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", common.AlertsIndex))
		return nil, err
	}

	var alerts []statistics.AlertDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	err = decoder.Decode(res)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
		return alerts, err
	}
	return alerts, nil
}

var predefinedIntervals = []time.Duration{
	2 * time.Minute,
	30 * time.Minute,
	60 * time.Minute,
	180 * time.Minute,
	360 * time.Minute,
	720 * time.Minute,
	1440 * time.Minute,
}

func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func CheckEntityStatsV2(ctx context.Context) error {
	// Create the map of EntityAlertsConfig
	db := config.GetDB()
	configMap, err := helper.CreateEntityAlertsConfigMapByType(db)
	tenants, err := helper.GetAllTenants(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error while fetching TenantMap", zap.Error(err))
	}

	tenantMap := make(map[string]string)
	for _, tenant := range tenants {
		tenantMap[tenant.ID.String()] = tenant.Name
	}
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error creating EntityAlertsConfig map", zap.Error(err))
		return err
	}

	// Iterate over predefined intervals
	for _, interval := range predefinedIntervals {
		// Calculate start and end time for the interval
		endTime := time.Now()
		startTime := endTime.Add(-interval)

		// Call getStatsByInterval for each interval
		aggObj, err1 := GetStatsByInterval(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
		if err1 != nil {
			logging.GetLoggerWithContext(ctx).Error("error getting stats by interval", zap.Error(err))
			return err1
		}
		logging.GetLoggerWithContext(ctx).Info("got response from statistics store", zap.Reflect("interval", interval.Minutes()), zap.Reflect("response", aggObj))

		// Compare the results with the data in the map
		var logsourceIdsStatsReceived []string
		for key, value := range aggObj.Agg {
			valueInt, ok := value.(float64)
			if !ok {
				logging.GetLoggerWithContext(ctx).Error("error while getting value of stats for source", zap.Error(err), zap.String("type", reflect.TypeOf(value).String()))
				continue
			}
			if valueInt > 0 {
				_, err = uuid.Parse(key)
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
				logging.GetLoggerWithContext(ctx).Info(formattedMessage)
				if !isPresent && isTimeAfter {
					config.TenantName = tenantMap[config.TenantID.String()]
					entityIdsToAlert = append(entityIdsToAlert, config)
				}
			}
		}
		var toPrint []string
		for _, configObject := range entityIdsToAlert {
			toPrint = append(toPrint, configObject.EntityID.String())
		}
		formattedMessage := fmt.Sprintf("Entity Details Interval: %v  entityIdsToAlert:  %v ", interval.Minutes(), toPrint)
		logging.GetLoggerWithContext(ctx).Info(formattedMessage)
		if len(entityIdsToAlert) > 0 {
			err = sendAlertsForInactivity(ctx, entityIdsToAlert)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func sendAlertsForInactivity(ctx context.Context, entityIdsToAlert []helper.EntityAlertsConfig) error {

	for _, configObject := range entityIdsToAlert {

		var emailTo []string
		emailTo = append(emailTo, config.GetAppConfiguration().GetString(awsemail.OPSGini))
		configJSON, err := json.Marshal(configObject)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error marshaling configObject to JSON", zap.Error(err))
			continue
		}
		title := fmt.Sprintf("Inactivity Alert Gen:V2 [Criticality: %s], Tenant: %s, Type : %s, Name: %s, No Data since: %.2f hr(s)",
			configObject.Criticality, configObject.TenantName, configObject.EntityType, configObject.EntityName, float64(configObject.Interval)/60)
		var email = awsemail.EmailNotification{
			Recipients: &awsemail.Recipient{
				To: emailTo,
			},
			Body:    aws.String(string(configJSON)),
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
