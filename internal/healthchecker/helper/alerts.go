package helper

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/store/opensearch"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

func SaveAlertToOpenSearch(ctx context.Context, entityArray []alerts_common.AlertEntityObject, title string, message string, functionalityType string, functionality string, severity string) error {
	var alertDocs []opensearch.AlertDoc
	for _, entity := range entityArray {
		osId := fmt.Sprintf("tenantId=%s&entityId=%s&entityName=%s&functionality=%s&type=%s", entity.EntityTenantUUId, entity.EntityId, entity.EntityName, functionalityType, functionality)
		// calculate sha for osId
		sha := sha256.New()
		sha.Write([]byte(osId))
		bs := fmt.Sprintf("%x", sha.Sum(nil))
		temp := opensearch.AlertDoc{
			Id:                      bs,
			Title:                   title,
			Message:                 message,
			CreatedAt:               time.Now().UnixMilli(),
			UpdatedAt:               time.Now().UnixMilli(),
			FirstObservedAt:         time.Now().UnixMilli(),
			LastObservedAt:          time.Now().UnixMilli(),
			TenantId:                entity.EntityTenantUUId.String(),
			FunctionalityType:       functionalityType,
			Functionality:           functionality,
			FunctionalityEntityId:   entity.EntityId.String(),
			FunctionalityEntityName: entity.EntityName,
			Dismissed:               false,
			DismissedBy:             "",
			Criticality:             severity,
		}
		alertDocs = append(alertDocs, temp)
	}
	conf := opensearch.GetConf()
	client, err := opensearch.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return err
	}

	err = opensearch.SaveAlertToOpenSearch(ctx, alertDocs, opensearch.AlertIndex, client)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while saving alert to opensearch", zap.Error(err))
		return err
	}
	return nil
}
