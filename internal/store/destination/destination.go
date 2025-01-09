package destination

import (
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
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
	Stats            float64   `json:"-" gorm:"-"`
	DataPlaneId      uuid.UUID `gorm:"type:uuid" json:"data_plane_id"`
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

func GetSourceByDestinationId(dId uuid.UUID, db *gorm.DB) ([]source.Source, error) {
	var sources []source.Source
	err := db.Raw(`select l.* from log_source l
					join pipeline_log_sources_mapping pls on l.id = pls.log_source_id
					join pipeline_destinations_mapping pd on pd.pipeline_id = pls.pipeline_id
					join pipelines p on pd.pipeline_id = p.id where pd.destination_id = ?;`, dId).Scan(&sources).Error
	return sources, err
}
