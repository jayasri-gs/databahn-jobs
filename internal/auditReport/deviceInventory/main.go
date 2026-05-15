package deviceInventory

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	opensearch "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
)

func WriteDeviceInventoryReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing device inventory report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	sources, startTime, endTime, err := getDeviceInventoryReportConfigFromRequest(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, startTime, endTime, sources, file)
	if err != nil {
		return err
	}
	return nil
}
func getDeviceInventoryReportConfigFromRequest(ctx context.Context, req models.AuditReport) ([]string, string, string, error) {
	var deviceInventoryReportConfiguration map[string]interface{}
	var sources []string
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &deviceInventoryReportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling audit report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return []string{}, "", "", err
	}
	var timeFilters, orFilters map[string]interface{}
	timeFilters = deviceInventoryReportConfiguration["time_filters"].(map[string]interface{})
	orFilters = deviceInventoryReportConfiguration["or_filters"].(map[string]interface{})
	if _, ok := orFilters["sources"]; ok {
		sources, _ = convertInterfaceSliceToStringSlice(orFilters["sources"].([]interface{}))
	}
	if v, ok := timeFilters["startTime"].(string); ok {
		startTime = v
	}
	if v, ok := timeFilters["endTime"].(string); ok {
		endTime = v
	}
	if startTime == "" || endTime == "" {
		logging.GetLoggerWithContext(ctx).Error("startTime or endTime missing or empty in device inventory report config", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return nil, "", "", fmt.Errorf("startTime or endTime missing or empty in device inventory report config")
	}
	return sources, startTime, endTime, nil
}
func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, startTime, endTime string, sources []string, file *os.File) error {
	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()

	logSources, err := helper.GetAllLogSourcesByTenantId(ctx, config.GetDB(), req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching log sources", zap.Error(err), zap.String("tenant_id", req.TenantId))
		return err
	}
	var sourceIdsToNames = make(map[string]string)
	for _, source := range logSources {
		sourceIdsToNames[source.ID.String()] = source.Name
	}

	headers := []string{"Hostname", "FQDN", "First Seen", "Last Seen", "Source Name", "Reputation"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	var searchAfter []any
	pageSize := utils.GetEnvInt("DEVICE_INVENTORY_REPORT_PAGE_SIZE", 1000)
	index := "db_insights_sights_sourcehostname_" + req.TenantId
	query, err := getQueryFromFilters(sources, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting query from filters", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	for {
		res, newSearchAfter, err := opensearch.SearchPaginated(ctx, opensearch.GetClient(), index, query, pageSize, searchAfter, []opensearch.Sort{{Field: "updated_at", Order: "asc"}, {Field: "id", Order: "asc"}})
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", index), zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		var deviceInventoryList []statistics.DeviceInventoryDocument
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &deviceInventoryList})
		err = decoder.Decode(res)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		err = writeDeviceInventoryRowsToFile(deviceInventoryList, sourceIdsToNames, writer)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}
		if len(res) == 0 || newSearchAfter == nil {
			break
		}
		searchAfter = newSearchAfter
	}
	return nil
}
func writeDeviceInventoryRowsToFile(deviceInventoryList []statistics.DeviceInventoryDocument, sourceIdsToNames map[string]string, writer *csv.Writer) error {
	for _, deviceInventory := range deviceInventoryList {
		firstSeen := time.Unix(0, deviceInventory.MinTime*int64(time.Millisecond)).Format(time.RFC3339)
		lastSeen := time.Unix(0, deviceInventory.MaxTime*int64(time.Millisecond)).Format(time.RFC3339)
		sourceName := sourceIdsToNames[deviceInventory.SourceId]
		fqdn := deviceInventory.Hostname // key1 from _source; Hostname may be replaced by key3 for display
		if deviceInventory.SmallName != "" {
			deviceInventory.Hostname = deviceInventory.SmallName
		}
		row := []string{deviceInventory.Hostname, fqdn, firstSeen, lastSeen, sourceName, deviceInventory.Reputation}
		err := writer.Write(row)
		if err != nil {
			logging.GetLogger().Error("error while writing row to the file", zap.Error(err))
			return err
		}
	}
	logging.GetLogger().Info("Writing rows to the file completed")
	writer.Flush()
	return writer.Error()
}
func convertInterfaceSliceToStringSlice(interfaceSlice []interface{}) ([]string, error) {
	stringSlice := make([]string, len(interfaceSlice))
	for i, v := range interfaceSlice {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("element at index %d is not a string", i)
		}
		stringSlice[i] = str
	}
	return stringSlice, nil
}
func getQueryFromFilters(sources []string, startTime, endTime, tenantId string) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		q += ` AND source_id: ` + "(" + strings.Join(sources, " OR ") + ")"
	}
	if startTime != "" {
		// convert startTime to epoch
		t, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return "", err
		}
		startTimeEpoch := t.UnixMilli()
		q += ` AND max_time:>` + strconv.FormatInt(startTimeEpoch, 10)
	}
	if endTime != "" {
		// convert endTime to epoch
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return "", err
		}
		endTimeEpoch := t.UnixMilli()
		q += ` AND max_time:<` + strconv.FormatInt(endTimeEpoch, 10)
	}
	return q, nil
}
