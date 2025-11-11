package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	cpcommon "github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

type DestinationToAlert struct {
	DestinationId   string
	SourceId        string
	TenantId        string
	DataPlaneId     string
	DestinationName string
	OutData         int64
	InData          int64
	FromTime        int64
	ToTime          int64
}

type DeliveredVolume struct {
	TenantId      string
	DestinationId string
	SourceId      string
	OutData       int64
}

func (d DestinationToAlert) GetEntityId() string {
	return d.DestinationId
}
func (d DestinationToAlert) GetEntityName() string {
	return d.DestinationName
}
func (d DestinationToAlert) GetDataPlaneId() string {
	return d.DataPlaneId
}
func (d DestinationToAlert) GetTenantId() string {
	return d.TenantId
}
func (d DestinationToAlert) GetSecondaryEntityId() string {
	return d.SourceId
}

func AlertDestinationsWithMoreDataDeliveredThanInjection(ctx context.Context) cpcommon.JobResult {
	var jobErrors []cpcommon.JobError
	db := config.GetDB()
	percentageThreshold := int64(utils.GetEnvInt("DESTINATION_DELIVERED_MORE_THAN_INJECTED_PERCENTAGE_THRESHOLD", 5))
	fromHourMinus := utils.GetEnvInt("DESTINATION_DELIVERED_MORE_THAN_INJECTED_TIME_FROM_HOURS_MINUS", 4)
	toHoursMinus := utils.GetEnvInt("DESTINATION_DELIVERED_MORE_THAN_INJECTED_TIME_TO_HOURS_MINUS", 1)
	minimumIngestionVolumeThreshold := int64(utils.GetEnvInt("DESTINATION_DELIVERED_MORE_MINIMUM_INGESTION_VOLUME_THRESHOLD", 500*1024*1024))   // Default: 500 MB
	minimumVolumeDifferenceThreshold := int64(utils.GetEnvInt("DESTINATION_DELIVERED_MORE_MINIMUM_VOLUME_DIFFERENCE_THRESHOLD", 100*1024*1024)) // Default: 100 MB
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting tenants: %v", err)
		jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return cpcommon.NewJobResultFromErrors(jobErrors)
	}

	osClient := os.GetClient()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating alerts manager: %v", err)
		jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return cpcommon.NewJobResultFromErrors(jobErrors)
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	fromTime, toTime := getTimeRange(fromHourMinus, toHoursMinus)
	for _, t := range tenants {
		tenantId := t.Id.String()
		sourceIdToSourceNameMap, err := common.GetLogSourceIdToNamesMap(ctx, tenantId)
		if err != nil {
			errorMsg := fmt.Sprintf("error while getting log source id to names map for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
			logger.GetLogger().Error("error while getting log source id to names map", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		destinations, err := destination.GetDestinationByTenantId(t.Id, db)
		if err != nil {
			errorMsg := fmt.Sprintf("error while getting destinations for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
			logger.GetLogger().Error("error while getting destinations for tenant", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		logSourceIdToInVolume, err := findIngestedVolumesForSources(ctx, tenantId, osClient, fromTime, toTime)
		if err != nil {
			errorMsg := fmt.Sprintf("error while finding ingested volumes per source for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, cpcommon.JobError{Message: errorMsg})
			logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		deliveredVolumeByDestIdSourceId, err := getDeliveredVolumeByDestinationAndSourceId(ctx, t, osClient, fromTime, toTime)
		if err != nil {
			logger.GetLogger().Error("error while getting delivered volume by destination and source id", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}

		var alertDestinations []DestinationToAlert
		// Track healthy destination-source pairs to auto-resolve open alerts
		type dstSrcPair struct{ dstId, srcId string }
		var healthyPairs []dstSrcPair
		for _, dest := range destinations {
			if dest.Status != "ACTIVE" {
				logger.GetLogger().Info("skipping inactive destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}
			deliveredVolumeForThisDestBySourceId, ok := deliveredVolumeByDestIdSourceId[dest.ID.String()]
			if !ok {
				logger.GetLogger().Info("no data delivered for destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}

			sourcesList, err := destination.GetSourceByDestinationId(dest.ID, db)
			if err != nil {
				logger.GetLogger().Error("error while getting sources for destination", zap.Error(err), zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}
			var sourceIds []string
			for _, source := range sourcesList {
				if source.Status == "ACTIVE" {
					sourceIds = append(sourceIds, source.ID.String())
				}
			}

			if len(sourceIds) == 0 {
				logger.GetLogger().Info("no valid sources found for destination:"+dest.ID.String(),
					zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}

			for _, sourceId := range sourceIds {
				inVolume, ok := logSourceIdToInVolume[sourceId]
				if !ok {
					logger.GetLogger().Info("no ingested volume found for source for destination:"+dest.ID.String(),
						zap.String("sourceId", sourceId), zap.String("tenantId", t.Id.String()))
					continue
				}
				outVolume, ok := deliveredVolumeForThisDestBySourceId[sourceId]
				if !ok {
					logger.GetLogger().Info("no delivered volume found for source in destination:"+dest.ID.String(),
						zap.String("sourceId", sourceId), zap.String("tenantId", t.Id.String()))
					continue
				}

				if inVolume == 0 {
					logger.GetLogger().Info("0 ingested data for source:"+dest.ID.String(), zap.String("sourceId", sourceId),
						zap.String("tenantId", t.Id.String()))
					continue
				}

				// Skip if ingestion volume is below minimum threshold AND absolute difference is below minimum threshold
				absoluteDifference := outVolume - inVolume
				if absoluteDifference < 0 {
					absoluteDifference = -absoluteDifference
				}

				if inVolume < minimumIngestionVolumeThreshold && absoluteDifference < minimumVolumeDifferenceThreshold {
					logger.GetLogger().Info("ignoring destination-source pair - both ingestion volume below minimum and difference below threshold",
						zap.String("destinationId", dest.ID.String()),
						zap.String("sourceId", sourceId),
						zap.String("tenantId", t.Id.String()),
						zap.Int64("inVolume", inVolume),
						zap.Int64("minimumIngestionVolumeThreshold", minimumIngestionVolumeThreshold),
						zap.Int64("absoluteDifference", absoluteDifference),
						zap.Int64("minimumVolumeDifferenceThreshold", minimumVolumeDifferenceThreshold))
					healthyPairs = append(healthyPairs, dstSrcPair{dstId: dest.ID.String(), srcId: sourceId})
					continue
				}

				logger.GetLogger().Info("destination data check volumes for destination"+dest.ID.String(), zap.String("tenantId", t.Id.String()),
					zap.Int64("outData", outVolume), zap.Int64("totalIngestedSize", inVolume), zap.Any("sourceIds", sourceIds))

				if outVolume > inVolume {
					morePercentage := ((float64(outVolume - inVolume)) / float64(inVolume)) * 100
					if morePercentage > float64(percentageThreshold) {
						destToAlert := DestinationToAlert{
							DestinationId:   dest.ID.String(),
							SourceId:        sourceId,
							TenantId:        t.Id.String(),
							DataPlaneId:     dest.DataPlaneId.String(),
							DestinationName: dest.Name,
							OutData:         outVolume,
							InData:          inVolume,
							FromTime:        fromTime,
							ToTime:          toTime,
						}
						alertDestinations = append(alertDestinations, destToAlert)
						logger.GetLogger().Info("alerting for destination, more out than in:"+dest.ID.String(),
							zap.Float64("percentage", morePercentage), zap.String("tenantId", t.Id.String()),
							zap.Int64("outData", outVolume), zap.Int64("inData", inVolume))
					} else {
						logger.GetLogger().Info("no alert for destination, more out than in but within limit:"+dest.ID.String(),
							zap.Float64("percentage", morePercentage), zap.String("tenantId", t.Id.String()), zap.Int64("outData", outVolume), zap.Int64("inData", inVolume))
						healthyPairs = append(healthyPairs, dstSrcPair{dstId: dest.ID.String(), srcId: sourceId})
						continue
					}
				} else {
					logger.GetLogger().Info("no alert for destination, lesser out than in:"+dest.ID.String(),
						zap.String("tenantId", t.Id.String()), zap.Int64("outData", outVolume), zap.Int64("inData", inVolume))
					healthyPairs = append(healthyPairs, dstSrcPair{dstId: dest.ID.String(), srcId: sourceId})
					continue
				}
			}
		}
		if len(alertDestinations) > 0 {
			var alerts []*alerts_async.Alert
			for _, alertDest := range alertDestinations {
				newAlert, err := buildMoreDeliveredAlert(alertDest, sourceIdToSourceNameMap)
				if err != nil {
					logger.GetLogger().Error("error while building alert", zap.Error(err), zap.String("destinationId", alertDest.DestinationId), zap.String("tenantId", t.Id.String()))
					continue
				}
				alerts = append(alerts, newAlert)
			}
			err = alertsManager.SendAlerts(alerts)
			if err != nil {
				logger.GetLogger().Error("error while sending alerts", zap.Error(err), zap.String("tenantId", t.Id.String()))
				continue
			}
			logger.GetLogger().Info("created destination delivered more than injected alerts", zap.Any("alertsCounts", len(alertDestinations)), zap.String("tenantId", t.Id.String()))
		} else {
			logger.GetLogger().Info("no destinations to alert for tenant", zap.String("tenantId", t.Id.String()))
		}

		// Auto-resolve any open "destination delivered more than injected" alerts for healthy pairs
		if len(healthyPairs) > 0 {
			var alertsToDismiss []string
			for _, pair := range healthyPairs {
				q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionality:dispenser AND functionalityType:%s AND functionalityEntityId:%s AND secondaryEntityId:%s",
					t.Id.String(), alerts_async.VolumeDeviationChecker.String(), pair.dstId, pair.srcId)
				openAlerts, _, err := os.Search(ctx, osClient, cpcommon.AlertsIndex, q)
				if err != nil {
					logger.GetLogger().Error("error while searching for destination delivered-more alerts to auto-resolve", zap.Error(err), zap.String("query", q))
					continue
				}
				alerts, err := statistics.ParseAlertDocuments(openAlerts)
				if err != nil {
					logger.GetLogger().Error("error while decoding OpenSearch alert response", zap.Error(err), zap.String("tenantId", t.Id.String()))
					continue
				}
				for _, alrt := range alerts {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
			if len(alertsToDismiss) > 0 {
				if err := alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
					logger.GetLogger().Error("error while auto-resolving destination delivered-more alerts", zap.Error(err), zap.String("tenantId", t.Id.String()))
				} else {
					logger.GetLogger().Info("auto-resolved destination delivered-more alerts", zap.String("tenantId", t.Id.String()), zap.Any("alertsToDismiss", alertsToDismiss))
				}
			}
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed destination delivered more check")
		return cpcommon.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("destination delivered more check completed with errors", zap.Int("error_count", len(jobErrors)))
		return cpcommon.NewJobResultFromErrors(jobErrors)
	}
}

func getDeliveredVolumeByDestinationAndSourceId(ctx context.Context, t tenant.Tenant, osClient *opensearch.Client, fromTime int64, toTime int64) (map[string]map[string]int64, error) {
	deliveredVolumes, err := findDeliveredVolumesForDestinations(ctx, t.Id.String(), osClient, fromTime, toTime)
	if err != nil {
		logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", t.Id.String()))
		return nil, err
	}
	deliveredVolumeByDestIdSourceId := make(map[string]map[string]int64)
	for _, dv := range deliveredVolumes {
		if dv.OutData <= 0 {
			logger.GetLogger().Info("skipping delivered volume with zero or negative out data", zap.String("destinationId", dv.DestinationId), zap.String("tenantId", t.Id.String()))
			return nil, err
		}
		if _, ok := deliveredVolumeByDestIdSourceId[dv.DestinationId]; !ok {
			deliveredVolumeByDestIdSourceId[dv.DestinationId] = make(map[string]int64)
		}
		deliveredVolumeByDestIdSourceId[dv.DestinationId][dv.SourceId] = dv.OutData
	}
	return deliveredVolumeByDestIdSourceId, nil
}

func buildMoreDeliveredAlert(toAlert DestinationToAlert, sourceIdToSourceNameMap map[string]string) (*alerts_async.Alert, error) {
	sourceName, ok := sourceIdToSourceNameMap[toAlert.SourceId]
	if !ok {
		return nil, fmt.Errorf("source name not found for source id: %s and tenant %s", toAlert.SourceId, toAlert.TenantId)
	}
	title := fmt.Sprintf("Destination '%s' delivered more data than injected for source '%s'", toAlert.DestinationName, sourceName)
	message := fmt.Sprintf("Destination '%s' delivered %s data, but injected volume by source '%s' was %s in the time from '%s' to '%s' as observed on '%s'",
		toAlert.DestinationName,
		util.HumanReadableBytes(toAlert.OutData),
		sourceName,
		util.HumanReadableBytes(toAlert.InData),
		util.HumanReadableTimeWithZone(time.UnixMilli(toAlert.FromTime)),
		util.HumanReadableTimeWithZone(time.UnixMilli(toAlert.ToTime)),
		util.HumanReadableTimeWithZone(time.Now().UTC()),
	)
	newAlert, err := alerts_async.NewAlert(alerts_async.Dispenser,
		alerts_async.WithEntity(toAlert),
		alerts_async.WithFunctionalityType(alerts_async.VolumeDeviationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "More data delivered than injected for destination."),
		alerts_async.WithAction("Please check destination's forward data type. If it is Databahn Object, what we send out is usually more than injected. You also can check transformation, if any, for that pipeline for what fields are selected like raw event or the derived fields."),
	)
	if err != nil {
		logger.GetLogger().Error("error while creating alert", zap.Error(err), zap.String("destinationId", toAlert.DestinationId), zap.String("tenantId", toAlert.TenantId))
		return nil, err
	}
	return newAlert, nil
}

func getTimeRange(fromHourMinus, toHoursMinus int) (int64, int64) {
	durationForFrom := time.Duration(fromHourMinus) * time.Hour
	durationForTo := time.Duration(toHoursMinus) * time.Hour
	now := time.Now().UTC()
	from := now.Add(-durationForFrom).UnixMilli()
	to := now.Add(-durationForTo).UnixMilli()
	return from, to
}

func findIngestedVolumesForSources(ctx context.Context, tenantId string, client *opensearch.Client, from, to int64) (map[string]int64, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	q := fmt.Sprintf(`name:"total_data_received" AND tags.component_name:"storage" AND tags.db_ts_win:{%d TO %d]`, from, to)
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}
	var after map[string]any = nil
	sourceIdToIngestionBytes := make(map[string]int64)
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, client, 100, statsAlias, q, []string{"tags.db_event_source_id.keyword"}, aggregations, after)
		if err != nil {
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			srcId := response.Key["tags.db_event_source_id.keyword"].(string)
			bytesIn := int64(response.Values["sum_value"].(float64))
			sourceIdToIngestionBytes[srcId] = bytesIn
		}

		after = newAfter
	}
	return sourceIdToIngestionBytes, nil
}

func findDeliveredVolumesForDestinations(ctx context.Context, tenantId string, client *opensearch.Client, from, to int64) ([]DeliveredVolume, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	q := fmt.Sprintf(`name:"total_bytes_delivered" AND tags.component_name:"dispenser" AND tags.db_ts_win:[%d TO %d}`, from, to)
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}
	var after map[string]any = nil
	var deliveredVolumes []DeliveredVolume
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, client, 100, statsAlias, q, []string{"tags.destination_id.keyword", "tags.db_event_source_id.keyword"}, aggregations, after)
		if err != nil {
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			destinationId := response.Key["tags.destination_id.keyword"].(string)
			sourceId := response.Key["tags.db_event_source_id.keyword"].(string)
			bytesOut := int64(response.Values["sum_value"].(float64))
			deliveredVolume := DeliveredVolume{
				TenantId:      tenantId,
				DestinationId: destinationId,
				SourceId:      sourceId,
				OutData:       bytesOut,
			}
			deliveredVolumes = append(deliveredVolumes, deliveredVolume)
		}

		after = newAfter
	}
	return deliveredVolumes, nil
}
