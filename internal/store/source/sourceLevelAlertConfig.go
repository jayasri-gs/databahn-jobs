package source

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"strings"
)

type ModuleTenantMapping struct {
	ID       uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primary_key;column:id"`
	TenantID uuid.UUID `gorm:"type:uuid;not null;column:tenant_id"`
	ModuleID uuid.UUID `gorm:"type:uuid;not null;column:module_id"`
	Enabled  bool      `gorm:"type:boolean;not null;column:enabled"`
	Config   string    `json:"config" gorm:"type:text"`
}

func (m *ModuleTenantMapping) TableName() string {
	return "module_tenant_mapping"
}

type ConfigLogSource struct {
	TenantID string
	SourceID string
}

func GetConfigLogSourceIds(ctx context.Context) ([]ConfigLogSource, error) {
	var mappings []ModuleTenantMapping
	err := config.GetDB().Model(&ModuleTenantMapping{}).Select("tenant_id, config").Scan(&mappings).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching module tenant mappings", zap.Error(err))
		return nil, err
	}

	var configLogSources []ConfigLogSource
	for _, mapping := range mappings {
		if mapping.Config == "" {
			continue
		}
		ids := strings.Split(mapping.Config, ",")
		for _, id := range ids {
			configLogSources = append(configLogSources, ConfigLogSource{
				TenantID: mapping.TenantID.String(),
				SourceID: id,
			})
		}
	}
	return configLogSources, nil
}
