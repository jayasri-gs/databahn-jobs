package jobs

import (
	"bytes"
	"context"
	"fmt"
	notification_common "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"text/template"
	"time"
)

type SilentDeviceEmailStruct struct {
	TenantName string
	Devices    []model.Device
	Grouped    map[string][]model.Device
	Title      string
}

const SilentDeviceAlertModule = "SILENT_DEVICE_ALERT"

func SendSilentDeviceNotification(ctx context.Context) error {
	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		return err
	}

	notificationManager, err := notification.NewNotificationManager(ctx)
	if err != nil {
		logging.GetLogger().Error("error while creating notification manager", zap.Error(err))
		return err
	}

	for _, t := range tenants {
		targets, err := entities.GetTargetsForModule(db, t.Id, SilentDeviceAlertModule)
		if err != nil {
			logging.GetLogger().Error("error while getting targets for tenant", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
		if len(targets) == 0 {
			logging.GetLogger().Info("no targets found for tenant, skipping", zap.String("tenantId", t.Id.String()))
			continue
		}

		silentDevice, err := buildSilentDeviceDigest(ctx, t)
		if err != nil {
			logging.GetLogger().Error("failed to build silent device Alerts", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
		if len(silentDevice.Devices) == 0 {
			logging.GetLogger().Info("no silent devices found, skipping notification", zap.String("tenantId", t.Id.String()))
			continue
		}
		err = sendSilentDeviceNotification(silentDevice, targets, t, notificationManager)
		if err != nil {
			logging.GetLogger().Error("failed to send silent device notification", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
	}
	notificationManager.Close(ctx)
	return nil
}

func buildSilentDeviceDigest(ctx context.Context, t tenant.Tenant) (*SilentDeviceEmailStruct, error) {
	// Fetch silent devices for this tenant (reuse your FetchSilentDevices logic)
	logSourceIds, err := getLogSourceIdsFromSilentDeviceConfig(ctx)
	if err != nil {
		return nil, err
	}
	sourceIDs := logSourceIds[t.Id.String()]
	if len(sourceIDs) == 0 {
		return &SilentDeviceEmailStruct{TenantName: t.Name, Devices: nil, Grouped: nil, Title: ""}, nil
	}
	devices, err := FetchSilentDevices(ctx, t.Id.String(), t.Name, sourceIDs)
	if err != nil {
		return nil, err
	}
	return &SilentDeviceEmailStruct{
		TenantName: t.Name,
		Devices:    devices,
		Grouped:    groupDevicesBySource(devices),
		Title:      "Silent Devices Alert for Tenant: " + t.Name,
	}, nil
}

func sendSilentDeviceNotification(digest *SilentDeviceEmailStruct, targets []entities.Targets, t tenant.Tenant, notificationManager *notification.NotificationManager) error {
	templatePath := EmailTemplatesBasePath + "silent_device_alert.html"
	temp, err := template.ParseFiles(templatePath)
	if err != nil {
		logging.GetLogger().Error("error while parsing template", zap.Error(err))
		return err
	}
	buf := new(bytes.Buffer)
	err = temp.Execute(buf, digest)
	if err != nil {
		logging.GetLogger().Error("error while executing template", zap.Error(err))
		return err
	}
	emailBody := buf.String()
	subject := "Silent Device Alert - " + time.Now().Format(time.DateOnly)
	var databahnTargets []*notification_common.DatabahnTarget
	for _, target := range targets {
		databahnTargets = append(databahnTargets, &notification_common.DatabahnTarget{
			TenantId: t.Id.String(),
			TargetId: target.ID.String(),
		})
	}
	emailRequest := notification_common.EmailNotificationRequest{
		Targets: databahnTargets,
		Body:    emailBody,
		Subject: subject,
	}
	err = notificationManager.SendEmailNotification(emailRequest)
	if err != nil {
		logging.GetLogger().Error("error while sending email notification", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
	return nil
}

func FetchSilentDevices(ctx context.Context, tenantId string, tenantName string, sources []string) ([]model.Device, error) {

	query, err := getQueryFromFilters(sources, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while building query", zap.Error(err))
		return nil, err
	}

	logging.GetLoggerWithContext(ctx).Info("query built", zap.String("query", query))

	var searchAfter []any
	pageSize := 100
	index := "db_insights_sights_sourcehostname_" + tenantId

	var allSilentDevices []model.Device
	for {
		silentDevices, newSearchAfter, err := getSilentDevices(ctx, os.GetClient(), index, query, pageSize, searchAfter, tenantName)
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
func getQueryFromFilters(sources []string, tenantId string) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		q += ` AND source_id: ` + "(" + strings.Join(sources, " OR ") + ")"
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

func groupDevicesBySource(devices []model.Device) map[string][]model.Device {
	groupedDevices := make(map[string][]model.Device)
	for _, device := range devices {
		groupedDevices[device.SourceName] = append(groupedDevices[device.SourceName], device)
	}
	return groupedDevices
}

func getSilentDevices(ctx context.Context, client *opensearch.Client, index string, query string, pageSize int, searchAfter []any, tenantName string) ([]model.Device, []any, error) {

	logging.GetLogger().Info("query", zap.String("query", query))
	logging.GetLogger().Info("pageSize", zap.Int("pageSize", pageSize))
	logging.GetLogger().Info("searchAfter", zap.Any("searchAfter", searchAfter))
	logging.GetLogger().Info("index", zap.String("index", index))

	var silentDevices []model.Device
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
		silentDevices = append(silentDevices, model.Device{
			Hostname:   device.Hostname,
			MinTime:    device.MinTime,
			MaxTime:    device.MaxTime,
			SourceID:   device.SourceId,
			TenantId:   device.TenantId,
			TenantName: tenantName,
		})
	}
	logging.GetLoggerWithContext(ctx).Info("silent devices fetched", zap.Int("count", len(silentDevices)))

	return silentDevices, newSearchAfter, nil
}

func getLogSourceIdsFromSilentDeviceConfig(ctx context.Context) (map[string][]string, error) {
	db := config.GetDB()
	var silentDeviceConfigs []model.SilentDevicesConfig
	err := db.Model(&model.SilentDevicesConfig{}).Find(&silentDeviceConfigs).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error fetching silent device configs", zap.Error(err))
		return nil, err
	}
	logging.GetLoggerWithContext(ctx).Info("silent device configs fetched", zap.Int("count", len(silentDeviceConfigs)))
	sourceIds := make(map[string][]string)
	for _, con := range silentDeviceConfigs {
		tenantId := con.TenantID.String()
		sourceId := con.SourceId.String()
		if _, ok := sourceIds[tenantId]; !ok {
			sourceIds[tenantId] = []string{}
		}
		sourceIds[tenantId] = append(sourceIds[tenantId], sourceId)
	}
	return sourceIds, nil
}
