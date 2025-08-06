package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"time"
)

type DestinationToAlert struct {
	Id              string
	TenantId        string
	DataPlaneId     string
	DestinationName string
	OutData         int64
	InData          int64
	FromTime        int64
	ToTime          int64
}

func (d DestinationToAlert) GetEntityId() string {
	return d.Id
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

func AlertDestinationsWithMoreDataDeliveredThanInjection(ctx context.Context) error {
	db := config.GetDB()
	percentageThreshold := int64(utils.GetEnvInt("DESTINATION_DELIVERED_MORE_THAN_INJECTED_PERCENTAGE_THRESHOLD", 20))
	timeCheckHoursBack := utils.GetEnvInt("DESTINATION_DELIVERED_MORE_THAN_INJECTED_LAST_HOURS_CHECK", 24)
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return err
	}

	osClient := os.GetClient()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	fromTime, toTime := getTimeRange(timeCheckHoursBack)
	for _, t := range tenants {

		//todo remove please
		if t.Id.String() != "f5e31bb8-af80-40d8-a0e4-16f12187e4e4" {
			continue
		}

		destinations, err := destination.GetDestinationByTenantId(t.Id, db)
		if err != nil {
			logger.GetLogger().Error("error while getting destinations for tenant", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}

		destinationIdToOutVolume, err := findDeliveredVolumesForDestinations(ctx, t.Id.String(), osClient, fromTime, toTime)
		if err != nil {
			logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
		logSourceIdToInVolume, err := findIngestedVolumesForSources(ctx, t.Id.String(), osClient, fromTime, toTime)
		if err != nil {
			logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}

		var alertDestinations []DestinationToAlert
		for _, dest := range destinations {
			if dest.Status != "ACTIVE" {
				logger.GetLogger().Info("skipping inactive destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}

			if outData, ok := destinationIdToOutVolume[dest.ID.String()]; ok {
				sourcesList, err := destination.GetSourceByDestinationId(dest.ID, db)
				if err != nil {
					logger.GetLogger().Error("error while getting sources for destination", zap.Error(err), zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
					continue
				}
				if len(sourcesList) == 0 {
					logger.GetLogger().Info("no sources found for destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
					continue
				}
				var sourceIds []string
				for _, source := range sourcesList {
					if source.Status == "ACTIVE" {
						sourceIds = append(sourceIds, source.ID.String())
					}
				}

				if len(sourceIds) == 0 {
					logger.GetLogger().Info("no active sources found for destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
					continue
				}

				totalIngestedSize := int64(0)
				for _, sourceId := range sourceIds {
					if inVolume, ok := logSourceIdToInVolume[sourceId]; ok {
						totalIngestedSize += inVolume
					}
				}
				if totalIngestedSize == 0 {
					logger.GetLogger().Warn("no ingested data found for destination", zap.String("destinationId", dest.ID.String()),
						zap.String("tenantId", t.Id.String()), zap.Any("sourceIds", sourceIds), zap.Int64("outData", outData))
					continue
				}
				logger.GetLogger().Info("destination data check volumes for destination"+dest.ID.String(), zap.String("tenantId", t.Id.String()),
					zap.Int64("outData", outData), zap.Int64("totalIngestedSize", totalIngestedSize), zap.Any("sourceIds", sourceIds))

				if outData > totalIngestedSize {
					morePercentage := ((float64(outData - totalIngestedSize)) / float64(totalIngestedSize)) * 100
					if morePercentage > float64(percentageThreshold) {
						destToAlert := DestinationToAlert{
							Id:              dest.ID.String(),
							TenantId:        t.Id.String(),
							DataPlaneId:     dest.DataPlaneId.String(),
							DestinationName: dest.Name,
							OutData:         outData,
							InData:          totalIngestedSize,
							FromTime:        fromTime,
							ToTime:          toTime,
						}
						alertDestinations = append(alertDestinations, destToAlert)
					} else {
						logger.GetLogger().Info("no alert for destination, more out than in but within limit", zap.Float64("percentage", morePercentage),
							zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()), zap.Int64("outData", outData), zap.Int64("inData", totalIngestedSize))
						continue
					}
				} else {
					logger.GetLogger().Info("no alert for destination, lesser out than in", zap.String("destinationId", dest.ID.String()),
						zap.String("tenantId", t.Id.String()), zap.Int64("outData", outData), zap.Int64("inData", totalIngestedSize))
					continue
				}
			} else {
				logger.GetLogger().Info("no data delivered for destination", zap.String("destinationId", dest.ID.String()), zap.String("tenantId", t.Id.String()))
				continue
			}
		}
		if len(alertDestinations) > 0 {
			var alerts []*alerts_async.Alert
			for _, alertDest := range alertDestinations {
				newAlert, err := buildModeDeliveredAlert(alertDest)
				if err != nil {
					logger.GetLogger().Error("error while building alert", zap.Error(err), zap.String("destinationId", alertDest.Id), zap.String("tenantId", t.Id.String()))
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

	}

	alertsManager.Close(ctx)

	return nil
}

func buildModeDeliveredAlert(toAlert DestinationToAlert) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Destination '%s' delivered more data than injected", toAlert.DestinationName)
	message := fmt.Sprintf("Destination '%s' delivered %s data, but injected volume is %s in the time from %s to %s",
		toAlert.DestinationName,
		util.HumanReadableBytes(toAlert.OutData),
		util.HumanReadableBytes(toAlert.InData),
		util.HumanReadableTimeWithZone(time.UnixMilli(toAlert.FromTime)),
		util.HumanReadableTimeWithZone(time.UnixMilli(toAlert.ToTime)),
	)
	newAlert, err := alerts_async.NewAlert(alerts_async.Dispenser,
		alerts_async.WithEntity(toAlert),
		alerts_async.WithFunctionalityType(alerts_async.VolumeDeviationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "More data delivered than injected for destination."),
	)
	if err != nil {
		logger.GetLogger().Error("error while creating alert", zap.Error(err), zap.String("destinationId", toAlert.Id), zap.String("tenantId", toAlert.TenantId))
		return nil, err
	}
	return newAlert, nil
}

func getTimeRange(hoursBack int) (int64, int64) {
	duration := time.Duration(hoursBack) * time.Hour
	from := time.Now().UTC().Add(-duration).UnixMilli()
	to := time.Now().UTC().UnixMilli()
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

func findDeliveredVolumesForDestinations(ctx context.Context, tenantId string, client *opensearch.Client, from, to int64) (map[string]int64, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	q := fmt.Sprintf(`name:"total_bytes_delivered" AND tags.component_name:"dispenser" AND tags.db_ts_win:[%d TO %d}`, from, to)
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}
	var after map[string]any = nil
	destinationIdToBytesOut := make(map[string]int64)
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, client, 100, statsAlias, q, []string{"tags.destination_id.keyword"}, aggregations, after)
		if err != nil {
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			destinationId := response.Key["tags.destination_id.keyword"].(string)
			bytesOut := int64(response.Values["sum_value"].(float64))
			destinationIdToBytesOut[destinationId] = bytesOut
		}

		after = newAfter
	}
	return destinationIdToBytesOut, nil
}
