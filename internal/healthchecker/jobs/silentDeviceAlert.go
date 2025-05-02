package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	awsemail "github.com/databahn-ai/databahn-jobs/internal/healthchecker/aws"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type Device struct {
	Hostname   string `json:"hostname"`
	MinTime    int64  `json:"min_time"`
	MaxTime    int64  `json:"max_time"`
	SourceID   string `json:"source_id"`
	TenantId   string `json:"tenant_id"`
	TenantName string `json:"tenant_name"`
	Summary    string `json:"summary,omitempty"`
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

	//logSourceIds, err := GetLogSourceIdsFromCollectionProfile(ctx)
	//if err != nil {
	//	logging.GetLoggerWithContext(ctx).Error("error fetching log source IDs", zap.Error(err))
	//	return err
	//}

	for _, t := range tenants {

		silentDevices, err := FetchSilentDevices(ctx, t.Id.String(), t.Name, logSourceIds)
		if err != nil {
			return fmt.Errorf("failed to fetch silent devices: %w", err)
		}

		if len(silentDevices) > 0 {
			logging.GetLoggerWithContext(ctx).Info("silent devices found", zap.String("tenantId", t.Id.String()), zap.Int("count", len(silentDevices)))
		} else {
			logging.GetLogger().Info("no silent devices found", zap.String("tenantId", t.Id.String()))
			continue
		}

		if err := sendAlertsForSilentDevices(ctx, silentDevices); err != nil {
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

// Helper function to build the query from filters
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

// Write a function to get configured log sources

func sendAlertsForSilentDevices(ctx context.Context, silentDevices []Device) error {

	for _, configObject := range silentDevices {

		var emailTo []string
		emailTo = append(emailTo, config.GetAppConfiguration().GetString(awsemail.OPSGini))
		configObject.Summary = fmt.Sprintf("No data received for the last 24 hours (1440 minutes)")
		formattedConfigObject, err := json.MarshalIndent(configObject, "", "  ")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error marshaling configObject to JSON", zap.Error(err))
			continue
		}

		title := fmt.Sprintf("Device :%s:%s", configObject.Hostname, configObject.TenantName)
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

		time.Sleep(500 * time.Millisecond)
	}

	return nil
}
