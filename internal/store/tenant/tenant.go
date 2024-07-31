package tenant

import (
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Tenant struct {
	Id   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

func (t *Tenant) TableName() string {
	return "tenants"
}

func GetTenants(ctx context.Context, db *gorm.DB) ([]Tenant, error) {
	var tenants []Tenant
	err := db.WithContext(ctx).Where("active = ?", true).Find(&tenants).Error
	if err != nil {
		return nil, err
	}
	return tenants, nil
}

func GetTenantDetailsById(ctx context.Context, db *gorm.DB, id uuid.UUID) (*Tenant, error) {
	var tenant *Tenant
	err := db.WithContext(ctx).Where("active = ? AND id = ?", true, id).First(&tenant).Error
	if err != nil {
		return nil, err
	}
	return tenant, nil
}
