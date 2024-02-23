package db

import (
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type ChangeFlag struct {
	RequestId   string `json:"request_id"`
	EntityId    string `json:"entity_id"`
	Timestamp   string `json:"timestamp"`
	TenantId    string `json:"tenant_id"`
	EntityType  string `json:"entity_type"`
	Action      string `json:"action"`
	IsProcessed bool   `json:"is_processed"`
}

func GetChangeFlagRequest(cfRequestIds []string) ([]ChangeFlag, error) {
	var records []ChangeFlag
	err := config.GetDB().Table("change_flag_request").Where("entity_id IN (?)", cfRequestIds).Find(&records).Error
	if err != nil {
		logger.GetLogger().Error("error while getting change flags", zap.Error(err))
		return nil, err
	}
	return records, nil
}
