package tenant

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
)

type Digest struct {
	TenantId                    uuid.UUID                  `json:"tenant_id"`
	Name                        string                     `json:"name"`
	IngestionHealth             string                     `json:"ingestion_health"`
	DeliveryHealth              string                     `json:"delivery_health"`
	TotalEventsIngested         string                     `json:"total_events_ingested"`
	NoOfEventsIngested          float64                    `json:"-"`
	TotalDataIngested           string                     `json:"total_data_ingested"`
	AverageEPS                  string                     `json:"average_eps"`
	EventDeliveryBreakdown      []destination.Destination  `json:"event_delivery_breakdown"`
	SensitiveDataTracking       map[string]string          `json:"sensitive_data_tracking"`
	EventsIngestionBreakdown    map[string]float64         `json:"events_ingestion_breakdown"`
	VolumeReductionAchievements map[string]int             `json:"volume_reduction_achievements"`
	Alerts                      []statistics.AlertDocument `json:"alerts"`
	StartTime                   string                     `json:"start_time"`
	EndTime                     string                     `json:"end_time"`
}

func formatNumber(num float64) string {
	switch {
	case num >= 1_000_000_000:
		return strconv.FormatFloat(float64(num)/1_000_000_000, 'f', 1, 64) + "B"
	case num >= 1_000_000:
		return strconv.FormatFloat(float64(num)/1_000_000, 'f', 1, 64) + "M"
	case num >= 1_000:
		return strconv.FormatFloat(float64(num)/1_000, 'f', 1, 64) + "K"
	default:
		return strconv.FormatFloat(num, 'f', 1, 64)
	}
}

func GetDailyDigest(tenantId uuid.UUID, tenantName, startTime, endTime string, alerts []statistics.AlertDocument) *Digest {
	return &Digest{
		TenantId:  tenantId,
		Name:      tenantName,
		StartTime: startTime,
		EndTime:   endTime,
		Alerts:    alerts,
	}

}

func (d *Digest) CalculateVolumeReductionAchievements() {
	d.VolumeReductionAchievements = make(map[string]int)
	for _, dest := range d.EventDeliveryBreakdown {
		totalEventIngestedForSource := 0.0
		sources, err := destination.GetSourceByDestinationId(dest.ID, config.GetDB())
		if err != nil {
			logger.GetLogger().Error("error while getting sources by destination id", zap.Error(err), zap.String("destinationId", dest.ID.String()))
			continue
		}
		for _, source := range sources {
			if d.EventsIngestionBreakdown[source.ID.String()] > 0 {
				totalEventIngestedForSource += d.EventsIngestionBreakdown[source.ID.String()]
			}
		}
		if totalEventIngestedForSource > 0 {
			reduction := (1 - (dest.Stats / totalEventIngestedForSource)) * 100
			if reduction > 0 {
				d.VolumeReductionAchievements[dest.Name] = int(reduction)
			} else {
				d.VolumeReductionAchievements[dest.Name] = 0
			}
		}
	}
}

func (d *Digest) GetIngestionBreakdown() error {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	agg := "tags.db_event_source_id.keyword"

	ingestionStats, err := ExecuteAggQuery(context.Background(), os.GetClient(), q, agg, d.StartTime, d.EndTime, d.TenantId)
	if err != nil {
		return err
	}
	d.EventsIngestionBreakdown = make(map[string]float64)
	for key, value := range ingestionStats {
		if value != nil {
			d.EventsIngestionBreakdown[key] = value.(float64)
		}
	}
	return nil
}

func (d *Digest) GetEventDeliveryBreakdown() error {
	d.DeliveryHealth = "Unhealthy"
	destinations, err := destination.GetDestinationByTenantId(d.TenantId, config.GetDB())
	if err != nil {
		return err
	}
	q := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	agg := "tags.destination_id.keyword"

	aggResponse, err := statistics.GetStatsAggregate(context.Background(), q, d.TenantId, agg, d.StartTime, d.EndTime)
	if err != nil {
		return err
	}
	destinationStats := aggResponse.Agg
	d.EventDeliveryBreakdown = make([]destination.Destination, 0)
	for _, dest := range destinations {
		if value, ok := destinationStats[dest.ID.String()]; ok {
			d.DeliveryHealth = "Healthy"
			// check if value is of type float64
			if reflect.TypeOf(value).Kind() == reflect.Float64 {
				dest.Stats = value.(float64)
			} else {
				dest.Stats = 0
			}
			dest.Count = formatNumber(dest.Stats)
			d.EventDeliveryBreakdown = append(d.EventDeliveryBreakdown, dest)
		}
	}
	return nil
}

func (d *Digest) CalculateEPS() {
	d.AverageEPS = "0"
	totalSecondsInDay := float64(24 * 60 * 60)
	if d.NoOfEventsIngested > 0 {
		d.AverageEPS = formatNumber(d.NoOfEventsIngested / totalSecondsInDay)
	}
}

func (d *Digest) CalculateIngestionStats(eventsIngested any, sizeIngested any) {
	d.IngestionHealth = "Healthy"
	if eventsIngested == nil {
		d.TotalEventsIngested = "0"
		d.IngestionHealth = "Unhealthy"
	} else {
		d.TotalEventsIngested = formatNumber(eventsIngested.(float64))
		d.NoOfEventsIngested = eventsIngested.(float64)
	}

	if sizeIngested == nil {
		d.TotalEventsIngested = "0"
	} else {
		d.TotalDataIngested = humanize.Bytes(uint64(sizeIngested.(float64)))
	}
}

func (d *Digest) GetSensitiveDataTrackingStats() error {
	d.SensitiveDataTracking = make(map[string]string)
	q := fmt.Sprintf(`name: "sensitive_total" AND tags.db_tenant_id.keyword: "%s"`, d.TenantId.String())
	agg := "tags.sensitive_type.keyword"
	s, err := ExecuteAggQuery(context.Background(), os.GetClient(), q, agg, d.StartTime, d.EndTime, d.TenantId)
	if err != nil {
		return err
	}
	for key, value := range s {
		if value != nil {
			d.SensitiveDataTracking[key] = formatNumber(value.(float64))
		}
	}
	return nil
}

func GetAlertsFromOpenSearch(ctx context.Context) (map[string][]statistics.AlertDocument, error) {
	checkTime := time.Now().Add(-24 * time.Hour)
	q := `lastObservedAt:>` + strconv.FormatInt(checkTime.UnixMilli(), 10)

	var allAlerts []statistics.AlertDocument
	var searchAfter []any

	for {
		res, newSearchAfter, err := os.SearchPaginated(ctx, os.GetClient(), common.AlertsIndex, q, 100, searchAfter, []os.Sort{{Field: "lastObservedAt", Order: "asc"}})
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", common.AlertsIndex))
			return nil, err
		}

		var alerts []statistics.AlertDocument
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
		err = decoder.Decode(res)

		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
			return nil, err
		}

		allAlerts = append(allAlerts, alerts...)

		if len(res) == 0 || newSearchAfter == nil {
			break
		}

		searchAfter = newSearchAfter
	}

	alertsByTenant := make(map[string][]statistics.AlertDocument)
	for _, alert := range allAlerts {
		alertsByTenant[alert.TenantId] = append(alertsByTenant[alert.TenantId], alert)
	}
	return alertsByTenant, nil
}
