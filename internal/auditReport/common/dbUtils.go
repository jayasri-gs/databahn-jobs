package common

import (
	"context"
	"database/sql"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func GetRowsAndColumnsByQueryFromTable(tableName, query string, page int, offset int) (*sql.Rows, []string, error) {
	rows, err := config.GetDB().Table(tableName).Limit(page).Offset(offset).Where(query).Order("updated_at").Rows()
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	return rows, columns, nil
}

func GetLogSourceIdToNamesMap(ctx context.Context, tenantId string) (map[string]string, error) {
	logSources, err := helper.GetAllLogSourcesByTenantId(ctx, config.GetDB(), tenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching logsources", zap.Error(err))
		return nil, err
	}
	logsourceIdToNameMap := make(map[string]string)
	for _, ls := range logSources {
		logsourceIdToNameMap[ls.ID.String()] = ls.Name
	}
	return logsourceIdToNameMap, nil
}

func GetDestinationIdToNamesMap(ctx context.Context, req models.AuditReport) (map[string]string, error) {
	destinations, err := destination.GetDestinationByTenantId(utils.UUIDFromStringOrNil(req.TenantId), config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching destinations", zap.Error(err))
		return nil, err
	}
	destinationIdToNameMap := make(map[string]string)
	for _, dest := range destinations {
		destinationIdToNameMap[dest.ID.String()] = dest.Name
	}
	destinationIdToNameMap["dbd00000-0000-0000-0000-000000000000"] = "Databahn Sandbox"
	return destinationIdToNameMap, nil
}
