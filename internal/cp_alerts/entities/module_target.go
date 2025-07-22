package entities

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type EmailConfig struct {
	To  string `json:"to"`
	CC  string `json:"cc"`
	BCC string `json:"bcc"`
}

type ModuleTenantConfigData struct {
	SourceList struct {
		SourceIds      []string `json:"sourceIds"`
		IncludeExclude string   `json:"includeExclude"`
	} `json:"sourceList"`
	IncludeExclude string `json:"includeExclude"`
}

type Modules struct {
	ID          uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primary_key"`
	Name        string    `json:"name" gorm:"type:varchar(255);not null"`
	Description string    `json:"desc" gorm:"type:varchar(255);not null"`
}

type Targets struct {
	ID            uuid.UUID      `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primary_key"`
	Name          string         `json:"name" gorm:"type:varchar(255);not null"`
	Description   string         `json:"description" gorm:"type:varchar(255)"`
	Type          string         `json:"type" gorm:"type:varchar(50);not null"`
	Configuration datatypes.JSON `json:"configuration" gorm:"type:jsonb"`
	TenantID      uuid.UUID      `json:"tenant_id" gorm:"type:uuid;not null"`
	CreatedBy     uuid.UUID      `json:"created_by" gorm:"type:uuid;not null"`
	UpdatedBy     uuid.UUID      `json:"updated_by" gorm:"type:uuid"`
	CreatedAt     time.Time      `json:"created_at" gorm:"type:timestamp;not null;default:current_timestamp"`
	UpdatedAt     time.Time      `json:"updated_at" gorm:"type:timestamp;default:current_timestamp"`
}

type ModuleTargets struct {
	ID        uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primary_key"`
	ModuleID  uuid.UUID `json:"module_id" gorm:"type:uuid;not null"`
	TargetID  uuid.UUID `json:"target_id" gorm:"type:uuid;not null"`
	Module    Modules   `json:"module" gorm:"foreignKey:ModuleID"`
	Target    Targets   `json:"target" gorm:"foreignKey:TargetID"`
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamp;not null;default:current_timestamp"`
	CreatedBy uuid.UUID `json:"created_by" gorm:"type:uuid;not null"`
}

type ModuleTenantMapping struct {
	ID                 uuid.UUID               `gorm:"type:uuid;default:uuid_generate_v4();primary_key;column:id"`
	TenantID           uuid.UUID               `gorm:"type:uuid;not null;column:tenant_id"`
	ModuleID           uuid.UUID               `gorm:"type:uuid;not null;column:module_id"`
	Module             Modules                 `json:"module" gorm:"foreignKey:ModuleID"`
	Enabled            bool                    `gorm:"type:boolean;not null;column:enabled"`
	Config             string                  `gorm:"type:text;not null;column:config"`
	ModuleTenantConfig *ModuleTenantConfigData `gorm:"type:json"`
}

func (m *ModuleTenantConfigData) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}
	return json.Unmarshal(bytes, m)
}

func (m ModuleTenantConfigData) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func GetAllModuleTenantConfigToTenantId(db *gorm.DB, tenantId uuid.UUID) (map[string][]ModuleTenantMapping, error) {
	var moduleTenantMappings []ModuleTenantMapping
	var moduleTenantConfigToTenantId = make(map[string][]ModuleTenantMapping)
	err := db.Where("tenant_id = ? ", tenantId).Find(&moduleTenantMappings).Error
	if err != nil {
		return nil, err
	}

	for _, mp := range moduleTenantMappings {
		moduleTenantConfigToTenantId[mp.TenantID.String()] = append(moduleTenantConfigToTenantId[mp.TenantID.String()], mp)
	}
	return moduleTenantConfigToTenantId, nil
}

func GetTargetsForModule(db *gorm.DB, tenantId uuid.UUID, moduleName string) ([]Targets, error) {
	var moduleTenants []ModuleTenantMapping
	err := db.Preload("Module").Where("tenant_id = ?", tenantId).Find(&moduleTenants).Error
	if err != nil {
		return nil, err
	}
	var enabledMatchingModuleIds []uuid.UUID
	for _, moduleTenant := range moduleTenants {
		if moduleTenant.Enabled && moduleTenant.Module.Name == moduleName {
			enabledMatchingModuleIds = append(enabledMatchingModuleIds, moduleTenant.ModuleID)
		}
	}
	if len(enabledMatchingModuleIds) == 0 {
		return []Targets{}, nil
	}
	var moduleTargets []ModuleTargets
	err = db.Joins("join targets on targets.id = module_targets.target_id").Preload("Target").Preload("Module").Where("module_id IN ? and targets.tenant_id = ?", enabledMatchingModuleIds, tenantId).Find(&moduleTargets).Error
	if err != nil {
		return nil, err
	}

	var result []Targets
	for _, moduleTarget := range moduleTargets {
		result = append(result, moduleTarget.Target)
	}
	return result, nil
}

func GetTargetsForTenantByModule(db *gorm.DB, tenantId uuid.UUID) (map[string][]Targets, error) {
	var moduleTenants []ModuleTenantMapping
	err := db.Where("tenant_id = ?", tenantId).Find(&moduleTenants).Error
	if err != nil {
		return nil, err
	}
	var enabledModuleIds []uuid.UUID
	for _, moduleTenant := range moduleTenants {
		if moduleTenant.Enabled {
			enabledModuleIds = append(enabledModuleIds, moduleTenant.ModuleID)
		}
	}
	if len(enabledModuleIds) == 0 {
		return map[string][]Targets{}, nil
	}
	var moduleTargets []ModuleTargets
	err = db.Joins("join targets on targets.id = module_targets.target_id").Preload("Target").Preload("Module").Where("module_id IN ? and targets.tenant_id = ?", enabledModuleIds, tenantId).Find(&moduleTargets).Error
	if err != nil {
		return nil, err
	}

	targetsByModuleName := make(map[string][]Targets)
	for _, moduleTarget := range moduleTargets {
		moduleName := moduleTarget.Module.Name
		targetsByModuleName[moduleName] = append(targetsByModuleName[moduleName], moduleTarget.Target)
	}
	return targetsByModuleName, nil
}
