package destination

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Configuration struct {
	Configuration map[string]interface{} `json:"configuration"`
}

type Destination struct {
	ID               uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	Name             string    `gorm:"type:varchar(100)" json:"name"`
	DestinationType  string    `gorm:"type:varchar(30)" json:"destination_type"`
	ForwardDataTypes string    `gorm:"type:varchar(30)[]" json:"forward_data_types"`
	TenantID         uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	Count            string    `json:"count" gorm:"-"`
}

func (s *Destination) TableName() string {
	return "destination"
}

func GetDestinationById(id uuid.UUID, db *gorm.DB) (Destination, error) {
	var destination Destination
	err := db.Where("id = ?", id).First(&destination).Error
	return destination, err
}

func GetDestinationByTenantId(tenantId uuid.UUID, db *gorm.DB) ([]Destination, error) {
	var destinations []Destination
	err := db.Where("tenant_id = ?", tenantId).Find(&destinations).Error
	return destinations, err
}
