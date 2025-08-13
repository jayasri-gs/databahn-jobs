package roiReport

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
)

func WriteROIReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing roi report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	startTime, endTime, err := getROIReportConfigFromRequest(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
}
func getROIReportConfigFromRequest(ctx context.Context, req models.AuditReport) (string, string, error) {
	var roiConfiguration map[string]interface{}
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &roiConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling ROI configuration", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", err
	}
	var timeFilters map[string]interface{}
	timeFilters = roiConfiguration["time_filters"].(map[string]interface{})
	if _, ok := timeFilters["startTime"].(string); ok {
		startTime = timeFilters["startTime"].(string)
	} else {
		logging.GetLoggerWithContext(ctx).Error("startTime is not a valid string in the configuration", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", fmt.Errorf("startTime is not a valid string in the configuration")
	}
	if _, ok := timeFilters["endTime"].(string); ok {
		endTime = timeFilters["endTime"].(string)
	} else {
		logging.GetLoggerWithContext(ctx).Error("endTime is not a valid string in the configuration", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", fmt.Errorf("endTime is not a valid string in the configuration")
	}
	return startTime, endTime, nil
}
func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, startTime, endTime string, file *os.File) error {
	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()
	destinationIdToNameMap, err := common.GetDestinationIdToNamesMap(ctx, req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting destination id to names map", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	logsourceIdToNameMap, err := common.GetLogSourceIdToNamesMap(ctx, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting log source id to names map", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	queryResponse, err := getAggStatsForLogSourceToDestinationPaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting roi stats", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	headers := []string{"Source", "Destination", "Incoming Data ", "Outgoing Data", "Reduction percentage"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
		return err
	}
	err = writeRowsToFileForROIReport(queryResponse, destinationIdToNameMap, logsourceIdToNameMap, writer)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
		return err
	}

	return nil
}
func writeRowsToFileForROIReport(queryResponse []Response, destinationIdsToName map[string]string, sourceIdsToNames map[string]string, writer *csv.Writer) error {
	for _, resp := range queryResponse {
		sourceName, ok := sourceIdsToNames[resp.LogSourceId]
		if !ok {
			sourceName = resp.LogSourceId
		}
		destinationName, ok := destinationIdsToName[resp.DestinationId]
		if !ok {
			destinationName = resp.DestinationId
		}
		row := []string{
			sourceName,
			destinationName,
			resp.Incoming,
			resp.Outgoing,
			resp.ReductionPercentage,
		}
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
