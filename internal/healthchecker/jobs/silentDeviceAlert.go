package jobs

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"time"

	cn "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	awsemail "github.com/databahn-ai/databahn-jobs/internal/healthchecker/aws"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	SilentReputation = "silent"
)

type Device struct {
	Hostname         string `json:"hostname"`
	MinTime          int64  `json:"min_time"`
	MaxTime          int64  `json:"max_time"`
	MinTimeFormatted string `json:"min_time_formatted,omitempty"`
	MaxTimeFormatted string `json:"max_time_formatted,omitempty"`
	SourceID         string `json:"source_id"`
	TenantId         string `json:"tenant_id"`
	TenantName       string `json:"tenant_name"`
	SourceName       string `json:"source_name"`
	Summary          string `json:"summary,omitempty"`
}

type SilentDevicesConfig struct {
	ID         uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	SourceId   uuid.UUID `gorm:"type:uuid" json:"source_id"`
	TenantID   uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	CustomerID uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	CreatedAt  time.Time `gorm:"type:timestamp" json:"created_at"`
	UpdatedAt  time.Time `gorm:"-" json:"updated_at"`
}

func GetLogSourceIdsFromSilentDeviceConfig(ctx context.Context) (map[string][]string, error) {
	db := config.GetDB()
	var silentDeviceConfigs []SilentDevicesConfig
	err := db.Model(&SilentDevicesConfig{}).Find(&silentDeviceConfigs).Error
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

func getSilentDevices(ctx context.Context, client *opensearch.Client, index string, query string, pageSize int, searchAfter []any, tenantName string) ([]Device, []any, error) {

	logging.GetLogger().Info("query", zap.String("query", query))
	logging.GetLogger().Info("pageSize", zap.Int("pageSize", pageSize))
	logging.GetLogger().Info("searchAfter", zap.Any("searchAfter", searchAfter))
	logging.GetLogger().Info("index", zap.String("index", index))

	var silentDevices []Device
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
		if device.Reputation != SilentReputation {
			continue
		}
		silentDevices = append(silentDevices, Device{
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

func ProcessSilentDevices(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	notificationMgr, err := notification.NewNotificationManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error initializing notification manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error initializing notification manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	defer notificationMgr.Close(ctx)

	tenants, err := tenant.GetTenants(ctx, config.GetDB())
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error while getting tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	logSourceIds, err := GetLogSourceIdsFromSilentDeviceConfig(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error fetching log source IDs: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error fetching log source IDs", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	for _, t := range tenants {
		logging.GetLoggerWithContext(ctx).Info("processing tenant", zap.String("tenantId", t.Id.String()))
		logging.GetLoggerWithContext(ctx).Info("log sources for the tenant ", zap.String("tenantId", t.Id.String()), zap.Any("logSourceIds", logSourceIds[t.Id.String()]))
		if _, ok := logSourceIds[t.Id.String()]; !ok {
			logging.GetLoggerWithContext(ctx).Info("no log source IDs found for tenant", zap.String("tenantId", t.Id.String()))
			continue
		}
		tenantId := t.Id.String()
		silentDevices, err := FetchSilentDevices(ctx, tenantId, t.Name, logSourceIds[tenantId])
		if err != nil {
			errorMsg := fmt.Sprintf("failed to fetch silent devices for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logging.GetLoggerWithContext(ctx).Error("failed to fetch silent devices", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		if len(silentDevices) > 0 {
			logging.GetLoggerWithContext(ctx).Info("silent devices found", zap.String("tenantId", tenantId), zap.Int("count", len(silentDevices)))
		} else {
			logging.GetLogger().Info("no silent devices found", zap.String("tenantId", tenantId))
			continue
		}

		if err := sendAlertsForSilentDevices(ctx, notificationMgr, silentDevices, logSourceIds, tenantId); err != nil {
			errorMsg := fmt.Sprintf("failed to send alerts for silent devices for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logging.GetLoggerWithContext(ctx).Error("error sending alerts for silent devices", zap.Error(err), zap.String("tenantId", tenantId))
		}
	}

	if len(jobErrors) == 0 {
		logging.GetLoggerWithContext(ctx).Info("successfully completed silent device processing")
		return common.NewJobResultSuccess()
	} else {
		logging.GetLoggerWithContext(ctx).Info("silent device processing completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}
func FetchSilentDevices(ctx context.Context, tenantId string, tenantName string, sources []string) ([]Device, error) {
	// Build the query using getQueryFromFilters
	query, err := getQueryFromFilters(sources, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while building query", zap.Error(err))
		return nil, err
	}

	logging.GetLoggerWithContext(ctx).Info("query built", zap.String("query", query))

	var searchAfter []any
	pageSize := 100
	index := "db_insights_sights_sourcehostname_" + tenantId

	var allSilentDevices []Device
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

	// Devices silent for up to 14 days (last seen between now -14d and now)
	maxThresholdTime := time.Now().UTC().Add(-14 * 24 * time.Hour)
	maxThresholdEpoch := maxThresholdTime.UnixMilli()
	currentEpoch := time.Now().UTC().UnixMilli()

	q += ` AND max_time:>=` + strconv.FormatInt(maxThresholdEpoch, 10) +
		` AND max_time:<` + strconv.FormatInt(currentEpoch, 10)

	logging.GetLogger().Info("final query", zap.String("query", q))

	return q, nil
}

func sendAlertsForSilentDevices(ctx context.Context, notificationMgr *notification.NotificationManager, silentDevices []Device, logSourceIds map[string][]string, tenantId string) error {
	sourceNames, err := GetSourceNames(ctx, config.GetDB(), logSourceIds[tenantId])
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error fetching source names", zap.Error(err))
		return err
	}

	for i := range silentDevices {
		silentDevices[i].MinTimeFormatted = formatUnixMillis(silentDevices[i].MinTime)
		silentDevices[i].MaxTimeFormatted = formatUnixMillis(silentDevices[i].MaxTime)
		silentDevices[i].SourceName = sourceNames[silentDevices[i].SourceID]
		durationDays := (silentDevices[i].MaxTime - silentDevices[i].MinTime) / (24 * 60 * 60 * 1000)
		silentDevices[i].Summary = fmt.Sprintf("%d days", durationDays)
	}

	emailData := EmailData{
		Title:           fmt.Sprintf("Silent Devices Alert for Tenant: %s", silentDevices[0].TenantName),
		BulkDataRequest: silentDevices,
		GroupedDevices:  groupDevicesBySource(silentDevices),
	}

	tmpl, err := template.New("emailTemplate").Funcs(template.FuncMap{
		"calculateDuration": func(maxTime int64) string {
			currentTime := time.Now().UTC().UnixMilli()
			durationDays := (currentTime - maxTime) / (24 * 60 * 60 * 1000)
			if durationDays == 0 {
				durationHours := (currentTime - maxTime) / (60 * 60 * 1000)
				return fmt.Sprintf("%d hours", durationHours)
			}
			return fmt.Sprintf("%d days", durationDays)
		},
	}).Parse(emailTemplate)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error parsing email template", zap.Error(err))
		return err
	}

	var body bytes.Buffer
	err = tmpl.Execute(&body, emailData)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error executing email template", zap.Error(err))
		return err
	}

	emailTo := []string{config.GetAppConfiguration().GetString(awsemail.OPSGini)}
	emailRequest := cn.EmailNotificationRequest{
		Recipients: &cn.EmailRecipients{
			To: emailTo,
		},
		Body:    body.String(),
		Subject: emailData.Title,
	}

	err = notificationMgr.SendEmailNotification(emailRequest)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error publishing email notification to kafka", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("published email notification to kafka for tenant", zap.String("tenantId", tenantId))
	return nil
}

type EmailData struct {
	Title           string
	BulkDataRequest []Device
	GroupedDevices  map[string][]Device
}

func groupDevicesBySource(devices []Device) map[string][]Device {
	groupedDevices := make(map[string][]Device)
	for _, device := range devices {
		groupedDevices[device.SourceName] = append(groupedDevices[device.SourceName], device)
	}
	return groupedDevices
}

func formatUnixMillis(ms int64) string {
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05")
}

func GetSourceNames(ctx context.Context, db *gorm.DB, sourceIDs []string) (map[string]string, error) {
	var sources []source.Source
	result := db.WithContext(ctx).Where("id IN ?", sourceIDs).Find(&sources)
	if result.Error != nil {
		return nil, result.Error
	}

	sourceMap := make(map[string]string)
	for _, src := range sources {
		sourceMap[src.ID.String()] = src.Name
	}

	return sourceMap, nil
}

const emailTemplate = `<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
          "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <style>
        body {
            background-color: #282F3B;
            font-family: Arial, sans-serif;
            color: #ffffff;
        }

        .left {
            text-align: left;
        }

        td {
            padding: 10px;
            border: 1px solid #ddd;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            background-color: #ffffff;
            color: #000000;
        }

        th {
            background-color: #f4f4f4;
            font-weight: bold;
            text-align: left;
            padding: 10px;
            border: 1px solid #ddd;
        }

        .summary {
            font-size: 16px;
            font-weight: bold;
            margin-bottom: 20px;
        }

        .content {
            width: 600px;
        }

        @media only screen and (max-width: 600px) {
            .content {
                width: 100%;
            }
        }
    </style>
</head>

<body style="margin: 0; padding: 0">
<table style="border: none" cellpadding="0" cellspacing="0" width="100%">
    <tr>
        <td style="padding: 15px 0">
            <table
                    style="border: none; margin-left: auto; margin-right: auto"
                    cellpadding="0"
                    cellspacing="0"
                    width="600"
                    class="content"
            >
                <!-- Start: Header -->
                <tr>
                    <td style="padding: 0px 0px 0px 0px; text-align: center;">
                        <img src="https://databahn.ai/wp-content/uploads/2024/02/DB-logo-reversed-final-1024x237-1-1.webp"
                             alt="DataBahn Inc" style="max-width: 500px;">
                    </td>
                </tr>
                <tr>
                    <td class="summary">
                        {{.Title}}
                    </td>
                </tr>
                <tr>
                    <td>
                        <p>Total Silent Devices Detected: {{len .BulkDataRequest}}</p>
                        <ul>
                            {{range $sourceName, $devices := .GroupedDevices}}
                            <li>{{len $devices}} silent devices detected for source: {{$sourceName}}</li>
                            {{end}}
                        </ul>
                    </td>
                </tr>
                <!-- End: Header -->

                <!-- Start: Grouped Device Details -->
                {{range $sourceName, $devices := .GroupedDevices}}
                <tr>
                    <td>
                        <h3>Source Name: {{$sourceName}}</h3>
                        <table>
                            <thead>
                            <tr>
                                <th>Device Hostname</th>
                                <th>First Seen</th>
                                <th>Last Seen</th>
                                <th>Duration</th>
                            </tr>
                            </thead>
                            <tbody>
                            {{range $devices}}
                            <tr>
                                <td>{{.Hostname}}</td>
                                <td>{{.MinTimeFormatted}}</td>
                                <td>{{.MaxTimeFormatted}}</td>
                                <td>{{calculateDuration .MaxTime}}</td>
                            </tr>
                            {{end}}
                            </tbody>
                        </table>
                    </td>
                </tr>
                {{end}}
                <!-- End: Grouped Device Details -->

                <!-- Start: Footer -->
                <tr>
                    <td>
                        <p>Please investigate the source and take necessary actions.</p>
                        <p>Regards,</p>
                        <p>DataBahn Team</p>
                    </td>
                </tr>
                <!-- End: Footer -->
            </table>
        </td>
    </tr>
</table>
</body>
</html>`
