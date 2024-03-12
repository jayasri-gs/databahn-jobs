package rule

import (
	"context"
	"time"

	event_dependency "github.com/databahn-ai/db-models/event-dependency"
	"github.com/databahn-ai/db-models/tags"
	uuid "github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Rule struct {
	ID                   uuid.UUID   `gorm:"primarykey;type:uuid"`
	Name                 string      `gorm:"type:VARCHAR(100);uniqueIndex:unique_name_per_tenant" validate:"required"`
	HistoryVersion       int         `gorm:"type:INT;default:1"`
	Conditions           string      `gorm:"type:TEXT" validate:"required"`
	Description          string      `gorm:"type:VARCHAR(512)"`
	TenantUUID           uuid.UUID   `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" validate:"required" json:"-"`
	EventDependencyID    uuid.UUID   `gorm:"type:uuid" validate:"required"`
	PipelineId           string      `gorm:"type:VARCHAR(36)"`
	DestinationId        uuid.UUID   `gorm:"type:uuid" validate:"required"`
	Scope                string      `gorm:"type:VARCHAR(16);default:CLOUD" validate:"required"`
	Priority             int         `gorm:"type:INT;default:301" validate:"required"`
	ActionType           string      `gorm:"type:VARCHAR(32);default:FORWARD" validate:"required"`
	SourceDevice         string      `gorm:"type:VARCHAR(32)"`
	SourceVendor         string      `gorm:"type:VARCHAR(32)"`
	Status               int         `validate:"min=0,max=5"`
	Type                 string      `gorm:"type:VARCHAR(32)" validate:"required"`
	Tags                 []tags.Tags `json:"tags,omitempty" gorm:"foreignKey:entity_id;references:id"`
	SamplingRate         int64       `gorm:"type:BIGINT;"`
	State                string      `gorm:"type:VARCHAR(20)"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	CreatedBy            string                           `gorm:"type:VARCHAR(40)"`
	UpdatedBy            string                           `gorm:"type:VARCHAR(40)"`
	EventDependency      event_dependency.EventDependency `json:"event_dependency,omitempty" gorm:"foreignKey:EventDependencyID;references:ID"`
	RuleFilters          datatypes.JSON
	RuleFilterQuery      string
	AggregationConfig    datatypes.JSONType[AggregationConfig]
	SuppressionConfig    datatypes.JSONType[SuppressionConfig]
	AggregationQuery     string
	AggregationTableName string `gorm:"type:VARCHAR(64)"`
	StudioRuleID         string `gorm:"type:VARCHAR(36)"`
	IsTaggingRule        bool
	UserModified         bool
	ReleaseNumber        string `gorm:"type:VARCHAR(32)"`
	ReferencedAttributes datatypes.JSON
}

func GetAll(ctx context.Context, db *gorm.DB) (rules []Rule, err error) {
	err = db.WithContext(ctx).Find(&rules).Error
	return rules, err
}

func GetAllRulesBySourceId(ctx context.Context, db *gorm.DB, sourceId string, tenantId string) (rules []Rule, err error) {
	err = db.WithContext(ctx).Table("rule").
		Select("db_rule.*").
		Joins("JOIN db_destination ON db_rule.destination_id = db_destination.id").
		Joins("JOIN db_event_dependency ON db_destination.id = db_event_dependency.destination_id").
		Where("db_event_dependency.event_source_id = ? AND db_event_dependency.tenant_uuid = ?", sourceId, tenantId).
		Find(&rules).Error
	return rules, err
}

func GetAllWithTags(ctx context.Context, db *gorm.DB) (rules []Rule, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules).Error
	return rules, err
}

func GetAllRulesByScopeAndStatus(ctx context.Context, db *gorm.DB, scope string, status int) (rules []Rule, err error) {
	err = db.WithContext(ctx).Find(&rules, "scope = ? AND status = ?", scope, status).Error
	return rules, err
}

func GetRulesByColumn(ctx context.Context, db *gorm.DB, column string, value any) (d []Rule, err error) {
	err = db.WithContext(ctx).Where(column, value).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []Rule{}
		return d, nil
	}
	return d, err
}

func GetRulesByScopeAndStatus(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, scope string, status int) (rules []Rule, err error) {
	err = db.WithContext(ctx).Find(&rules, "tenant_uuid = ? AND scope = ? AND status = ?", tenantId, scope, status).Error
	return rules, err
}

func GetRulesByScopeAndStatusWithTags(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, scope string, status int) (rules []Rule, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules, "tenant_uuid = ? AND scope = ? AND status = ?", tenantId, scope, status).Error
	return rules, err
}

func GetRuleByDestinationId(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, dId uuid.UUID) (rules []Rule, err error) {
	err = db.WithContext(ctx).Find(&rules, "destination_id = ? AND tenant_uuid = ?", dId, tenantId).Error
	return rules, err
}

func Get(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId string) (rule Rule, err error) {
	err = db.WithContext(ctx).First(&rule, "id = ? AND tenant_uuid = ?", id, tenantId).Error
	return rule, err
}

func GetWithTags(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId string) (rule Rule, err error) {
	err = db.WithContext(ctx).Preload("Tags").First(&rule, "id = ? AND tenant_uuid = ?", id, tenantId).Error
	return rule, err
}

func (r *Rule) Save(ctx context.Context, db *gorm.DB) (err error) {
	return db.WithContext(ctx).Create(&r).Error
}

func (r *Rule) Delete(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) error {
	return db.WithContext(ctx).Delete(&r, "tenant_uuid = ?", tenantId).Error
}

func (r *Rule) Update(ctx context.Context, db *gorm.DB, updated any) error {
	return db.WithContext(ctx).Model(&r).Updates(updated).Error
}

func GetAllByTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (rules []Rule, err error) {
	err = db.WithContext(ctx).Find(&rules, "tenant_uuid = ?", tenantId).Error
	return rules, err
}

func GetAllByTenantWithTags(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (rules []Rule, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules, "tenant_uuid = ?", tenantId).Error
	return rules, err
}
