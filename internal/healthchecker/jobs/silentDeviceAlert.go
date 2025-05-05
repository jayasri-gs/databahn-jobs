package jobs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/databahn-jobs/internal/config"
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
	"html/template"
	"strconv"
	"strings"
	"time"
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
		silentDevices = append(silentDevices, Device{
			Hostname:   device.Hostname,
			MinTime:    device.MinTime,
			MaxTime:    device.MaxTime,
			SourceID:   device.SourceId,
			TenantId:   device.TenantId,
			TenantName: tenantName,
		})
	}

	return silentDevices, newSearchAfter, nil
}

func ProcessSilentDevices(ctx context.Context) error {
	tenants, err := tenant.GetTenants(ctx, config.GetDB())
	if err != nil {
		return err
	}

	logSourceIds, err := GetLogSourceIdsFromSilentDeviceConfig(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error fetching log source IDs", zap.Error(err))
		return err
	}

	for _, t := range tenants {
		logging.GetLoggerWithContext(ctx).Info("processing tenant", zap.String("tenantId", t.Id.String()))
		logging.GetLoggerWithContext(ctx).Info("log sources for the tenant ", zap.String("tenantId", t.Id.String()), zap.Any("logSourceIds", logSourceIds[t.Id.String()]))
		if _, ok := logSourceIds[t.Id.String()]; !ok {
			logging.GetLoggerWithContext(ctx).Info("no log source IDs found for tenant", zap.String("tenantId", t.Id.String()))
			continue
		}
		silentDevices, err := FetchSilentDevices(ctx, t.Id.String(), t.Name, logSourceIds[t.Id.String()])
		if err != nil {
			return fmt.Errorf("failed to fetch silent devices: %w", err)
		}

		if len(silentDevices) > 0 {
			logging.GetLoggerWithContext(ctx).Info("silent devices found", zap.String("tenantId", t.Id.String()), zap.Int("count", len(silentDevices)))
		} else {
			logging.GetLogger().Info("no silent devices found", zap.String("tenantId", t.Id.String()))
			continue
		}

		if err := sendAlertsForSilentDevices(ctx, silentDevices, logSourceIds, t.Id.String()); err != nil {
			logging.GetLoggerWithContext(ctx).Error("error sending alerts for silent devices", zap.String("tenantId", t.Id.String()), zap.Error(err))
			return fmt.Errorf("failed to send alerts for silent devices: %w", err)
		}
	}

	return nil
}
func FetchSilentDevices(ctx context.Context, tenantId string, tenantName string, sources []string) ([]Device, error) {

	// Build the query using getQueryFromFilters
	query, err := getQueryFromFilters(sources, tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while building query", zap.Error(err))
		return nil, err
	}

	var searchAfter []any
	pageSize := 100 // Adjust page size as needed
	index := "db_insights_sights_sourcehostname_" + tenantId

	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}

	var allSilentDevices []Device
	for {
		silentDevices, newSearchAfter, err := getSilentDevices(ctx, client, index, query, pageSize, searchAfter, tenantName)
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

	return allSilentDevices, nil
}

func getQueryFromFilters(sources []string, tenantId string) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		q += ` AND source_id: ` + "(" + strings.Join(sources, " OR ") + ")"
	}

	// Set endTime to 24 hours before the current time (previous day)
	endTime := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)

	// Parse endTime and add it to the query
	t, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return "", fmt.Errorf("error parsing endTime: %v", err)
	}
	endTimeEpoch := t.UnixMilli()
	q += ` AND max_time:<` + strconv.FormatInt(endTimeEpoch, 10)

	return q, nil
}

func sendAlertsForSilentDevices(ctx context.Context, silentDevices []Device, logSourceIds map[string][]string, tenantId string) error {
	sourceNames, err := GetSourceNames(ctx, config.GetDB(), logSourceIds[tenantId])
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error fetching source names", zap.Error(err))
		return err
	}

	for _, configObject := range silentDevices {
		var emailTo []string
		emailTo = append(emailTo, config.GetAppConfiguration().GetString(awsemail.OPSGini))

		configObject.MinTimeFormatted = formatUnixMillis(configObject.MinTime)
		configObject.MaxTimeFormatted = formatUnixMillis(configObject.MaxTime)
		configObject.SourceName = sourceNames[configObject.SourceID]

		emailData := EmailData{
			Title:           fmt.Sprintf("Device :%s:%s", configObject.Hostname, configObject.TenantName),
			BulkDataRequest: []Device{configObject},
		}

		tmpl, err := template.New("emailTemplate").Parse(emailTemplate)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error parsing email template", zap.Error(err))
			continue
		}

		var body bytes.Buffer
		err = tmpl.Execute(&body, emailData)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error executing email template", zap.Error(err))
			continue
		}

		var emailNotification = awsemail.EmailNotification{
			Recipients: &awsemail.Recipient{
				To: emailTo,
			},
			Body:    aws.String(body.String()),
			Subject: aws.String(emailData.Title),
		}

		err = awsemail.SendEmail(ctx, emailNotification)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Info("Error sending notification")
			return err
		}

		logging.GetLoggerWithContext(ctx).Info("sent notification")

		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

type EmailData struct {
	Title           string
	BulkDataRequest []Device
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
		            padding: 20px 50px 30px 50px;
		        }
		
		        small,
		        .small {
		            font-size: 12px;
		        }
		
		        a,
		        a:hover,
		        a:visited {
		            color: #000000;
		            text-decoration: underline;
		        }
		
		        h1,
		        h2 {
		            font-size: 22px;
		            color: #404040;
		            font-weight: normal;
		            padding-top: 25px;
		        }
		
		        p {
		            font-size: 15px;
		            color: #606060;
		        }
		
		        .general {
		            background-color: #ffffff;
		        }
		
		        .icon {
		            margin: -40px 0px 15px 0px;
		            width: 60px;
		            height: 60px;
		            line-height: 60px;
		            display: inline-block;
		            text-align: center;
		            border-radius: 30px;
		            color: #ffa523;
		            font-style: oblique;
		            font-size: 24px;
		            font-weight: bold;
		            font-family: serif;
		        }
		
		        .information p {
		            color: #273c47;
		        }
		
		        .information .icon {
		            font-family: Georgia, "Times New Roman", Times, serif;
		            font-style: italic;
		            color: black;
		        }
		
		        .content {
		            width: 600px;
		        }
		
		        @media only screen and (max-width: 600px) {
		            .content {
		                width: 100%;
		            }
		        }
		
		        @media only screen and (max-width: 400px) {
		            td {
		                padding: 15px 25px;
		            }
		
		            h1,
		            h2 {
		                font-size: 20px;
		            }
		
		            p {
		                font-size: 12px;
		            }
		
		            small,
		            .small {
		                font-size: 12px;
		            }
		
		            .icon {
		                display: block;
		                margin: 10px auto 10px auto;
		            }
		        }
		    </style>
		    <link rel="stylesheet"
		          href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-icons/1.10.5/font/bootstrap-icons.min.css">
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
		                <!-- Start: Small header text in pale grey email background -->
		                <tr>
		                    <td style="padding: 0px 0px 0px 0px; text-align: center;">
		                        <img src="https://databahn.ai/wp-content/uploads/2024/02/DB-logo-reversed-final-1024x237-1-1.webp"
		                             alt="DataBahn Inc" style="max-width: 500px;">
		                    </td>
		                </tr>
		                <!-- End: Small header text in pale grey email background -->
		
		                <!-- Start: Notice line with icon -->
		                <tr>
		                    <td class="general left">
		                        <span class="information icon"><i class="bi bi-info-circle"></i></span>
		                        <p class="infocolor" style="color: #ffa523">{{.Title}}</p>
		                    </td>
		                </tr>
		                <!-- End: Notice line with icon -->
		
		                <!-- Start: Iterate through all items in the email to be notified -->
		                {{range .BulkDataRequest}}
		                <tr align="left">
		                    <td class="general" style="padding: 10px 20px">
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Device Hostname</span>:
		                            {{.Hostname}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Tenant Name</span>: {{.TenantName}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Source ID</span>: {{.SourceID}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Source Name</span>: {{.SourceName}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">First Seen</span>: {{.MinTimeFormatted}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Last Seen</span>: {{.MaxTimeFormatted}}
		                        </p>
		                        <p>
		                            <span style="font-size: 14px; font-weight: 600">Message</span>: Silent device detected
		                        </p>
		                    </td>
		                </tr>
		                <tr>
		                    <td class="general" style="padding: 10px 20px">
		                        <hr width="80%" color="#fc5858" size="1">
		                    </td>
		                </tr>
		                {{end}}
		                <!-- End: Iterate through all items in the email to be notified -->
		
		                <!-- Start: Closeout line and contact -->
		                <tr>
		                    <td class="general left">
		                        <p>
		                            Please investigate the source and take necessary actions.
		                        </p>
		                    </td>
		                </tr>
		                <tr>
		                    <td class="general left">
		                        <p class="small">Regards,</p>
		                        <p class="small">DataBahn Team</p>
		                    </td>
		                </tr>
		                <!-- End: Closeout line and contact -->
		            </table>
		        </td>
		    </tr>
		</table>
		</body>
		</html>`
