package entities

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

type EmailConfig struct {
	To  string `json:"to"`
	CC  string `json:"cc"`
	BCC string `json:"bcc"`
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
	ID       uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primary_key;column:id"`
	TenantID uuid.UUID `gorm:"type:uuid;not null;column:tenant_id"`
	ModuleID uuid.UUID `gorm:"type:uuid;not null;column:module_id"`
	Module   Modules   `json:"module" gorm:"foreignKey:ModuleID"`
	Enabled  bool      `gorm:"type:boolean;not null;column:enabled"`
}

func GetTargetsForModule(db *gorm.DB, tenantId uuid.UUID, moduleName string) ([]Targets, error) {
	var modules []Modules
	err := db.Where("name = ?", moduleName).Find(&modules).Error
	if err != nil {
		return nil, err
	}
	var moduleIds []uuid.UUID
	for _, module := range modules {
		moduleIds = append(moduleIds, module.ID)
	}
	var moduleTenants []ModuleTenantMapping
	err = db.Where("tenant_id = ? AND module_id IN ?", tenantId, moduleIds).Find(&moduleTenants).Error
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
		return []Targets{}, nil
	}
	var moduleTargets []ModuleTargets
	err = db.Where("module_id IN ?", enabledModuleIds).Find(&moduleTargets).Error
	if err != nil {
		return nil, err
	}
	var targetIds []uuid.UUID
	for _, moduleTarget := range moduleTargets {
		targetIds = append(targetIds, moduleTarget.TargetID)
	}
	var targets []Targets
	err = db.Where("id IN ?", targetIds).Find(&targets).Error
	if err != nil {
		return nil, err
	}
	return targets, nil
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
