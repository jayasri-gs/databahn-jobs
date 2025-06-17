package logsource

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strings"
	"sync"
)

func FetchLogSourceReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
	var file *os.File
	var writer *csv.Writer
	defer func() {
		file.Close()
		writer.Flush()
		wg.Done()
		<-parallelismCntrl
	}()

	var failedRequestsTemp []models.FailedRequests
	var successAlertsTemp []alerts_common.AlertBaseObjectV2
	logging.GetLogger().Info("Fetching logsource report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
	err := models.UpdateRequestStatus(config.GetDB(), req.Id.String(), consts.INPROGRESS)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		failedRequestsTemp = append(failedRequestsTemp, errRequest)
		return
	}

	file, writer, err = gatherDataAndWriteToFile(ctx, req, file, &failedRequestsTemp, writer)
	if err != nil {
		return
	}
	bucketName, objectKey := common.GetBucketNameAndObjectKey(req.Id.String())
	err = common.UploadFileToS3AndUpdateInDb(ctx, file, req, bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return
	} else {
		successAlertsTemp = append(successAlertsTemp, alerts_common.AlertBaseObjectV2{EntityName: req.Name, EntityId: utils.UUIDFromStringOrNil(req.Id.String()), EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId), AlertType: alerts_common.AlertTypeExternalAndExternal})
	}

	// Lock the mutex before updating the success alerts
	successAlertsMutex.Lock()
	*successAlerts = append(*successAlerts, successAlertsTemp...)
	successAlertsMutex.Unlock()

	// Lock the mutex before updating the failed requests
	failedRequestMutex.Lock()
	*failedRequests = append(*failedRequests, failedRequestsTemp...)
	failedRequestMutex.Unlock()
}

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests, writer *csv.Writer) (*os.File, *csv.Writer, error) {
	pageSize := utils.GetEnvInt("LOGSOURCE_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	query, startTime, endTime, file, err := getFileAndRequestConfig(ctx, req, file, failedRequests)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}

	// get ingestion stats
	logsourceIdsToIngestionStats, err := getAggStatsForLogSourcePaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	// get data ingestion stats and destination stats

	logsourceIdsToDestinationStats, err := getAggStatsForLogSourceToDestinationPaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}

	// get destiantion id to names
	failedRequests, destinationIdToNameMap, err := GetDestinationIdToNamesMap(ctx, req, failedRequests)
	if err != nil {
		return nil, nil, err
	}
	file, writer, err = writeToFIle(ctx, req, file, failedRequests, writer, writeHeader, offset, pageSize, query, logsourceIdsToIngestionStats, logsourceIdsToDestinationStats, destinationIdToNameMap)
	if err != nil {
		return nil, nil, err
	}
	return file, writer, nil
}

func writeToFIle(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests, writer *csv.Writer, writeHeader bool, offset int, pageSize int, query string, logsourceIdsToIngestionStats map[string]string, logsourceIdsToDestinationStats map[string]map[string]string, destinationIdToNameMap map[string]string) (*os.File, *csv.Writer, error) {
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, query)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from audit table", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}
		fetchedRowsCount := 0
		if writeHeader {
			headerColumns := append(columns, "ingestion_stats", "destination_stats")
			// Add additional columns for ingestion stats and destination stats
			writer = csv.NewWriter(file)
			err = writer.Write(headerColumns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
				errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
				*failedRequests = append(*failedRequests, errRequest)
				return nil, nil, err
			}

		}

		fetchedRowsCount, err = writeRowToTheFileOneByOne(columns, rows, logsourceIdsToIngestionStats, logsourceIdsToDestinationStats, destinationIdToNameMap, writer, fetchedRowsCount)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}
		if fetchedRowsCount < pageSize {
			break
		}
		offset += pageSize + 1
	}
	return file, writer, nil
}

func GetDestinationIdToNamesMap(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests) (*[]models.FailedRequests, map[string]string, error) {
	destinations, err := destination.GetDestinationByTenantId(utils.UUIDFromStringOrNil(req.TenantId), config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching destinations", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	destinationIdToNameMap := make(map[string]string)
	for _, dest := range destinations {
		destinationIdToNameMap[dest.ID.String()] = dest.Name
	}
	return failedRequests, destinationIdToNameMap, nil
}
func writeRowToTheFileOneByOne(columns []string, rows *sql.Rows, logsourceIdsToIngestionStats map[string]string, logsourceIdsToDestinationStats map[string]map[string]string, destinationIdToName map[string]string, writer *csv.Writer, fetchedRowsCount int) (int, error) {

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		lsId := ""
		columnValue := ""
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for i, col := range values {
			if columns[i] == "id" {
				lsId = string(*col.(*sql.RawBytes))
			}
			columnValue = string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}
		row = append(row, logsourceIdsToIngestionStats[lsId])
		row = append(row, getDestinationStatsString(logsourceIdsToDestinationStats, lsId, destinationIdToName))
		err := writer.Write(row)
		if err != nil {
			logging.GetLoggerWithContext(context.Background()).Error("error while writing row to the file", zap.Error(err))
			return 0, err
		}
		fetchedRowsCount++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return fetchedRowsCount, nil
}

func getDestinationStatsString(logsourceIdsToDestinationStats map[string]map[string]string, logSourceId string, destinationIdToName map[string]string) string {
	if destStats, ok := logsourceIdsToDestinationStats[logSourceId]; ok {
		var destinationStats []string
		for destId, value := range destStats {
			if name, exists := destinationIdToName[destId]; exists {
				destinationStats = append(destinationStats, fmt.Sprintf("%s: %s", name, value))
			} else {
				destinationStats = append(destinationStats, fmt.Sprintf("%s: %s", destId, value))
			}
		}
		return fmt.Sprintf("%v", strings.Join(destinationStats, "\n"))
	}
	return ""
}
func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests) (string, string, string, *os.File, error) {
	file, err := common.CreateTempFile(req.Id.String())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	query, startTime, endTime, err := getQueryFromConfig(ctx, req, failedRequests)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	return query, startTime, endTime, file, err
}
func getRowsAndColumnsFromAuditTable(pageSize int, offset int, query string) (*sql.Rows, []string, error) {
	rows, err := config.GetDB().Table("log_source").Limit(pageSize).Offset(offset).Where(query).Order("updated_at").Rows()
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	return rows, columns, nil
}
func getQueryFromConfig(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		return "", "", "", err
	}
	var configData map[string]interface{}
	configData = reportConfiguration["filter"].(map[string]interface{})

	startTime := configData["startTime"].(string)
	endTime := configData["endTime"].(string)

	err = common.ValidateConfig(startTime, endTime)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error in validating startime and endtime", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	query := fmt.Sprintf("tenant_id = '%s' and updated_at >= '%s' and updated_at <= '%s'", req.TenantId, startTime, endTime)

	otherParamsAdded := false

	filterMappings := map[string]string{
		"scope":       "scope",
		"vendor":      "vendor",
		"device":      "device",
		"logType":     "log_type",
		"dataplaneId": "data_plane_id",
	}

	for filterKey, dbField := range filterMappings {
		otherParamsAdded, query = common.UpdateQueryFromFilter(configData, otherParamsAdded, query, filterKey, dbField)
	}
	if otherParamsAdded {
		query += ")"
	}
	return query, startTime, endTime, nil
}
