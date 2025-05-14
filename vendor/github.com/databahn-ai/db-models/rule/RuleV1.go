package rule

import (
	"context"
	uuid "github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

type RuleV1 struct {
	ID                   uuid.UUID `gorm:"primarykey;type:uuid"`
	Name                 string    `gorm:"type:VARCHAR(100);uniqueIndex:unique_name_per_tenant" validate:"required"`
	HistoryVersion       int       `gorm:"type:INT;default:1"`
	Conditions           string    `gorm:"type:TEXT" validate:"required"`
	Description          string    `gorm:"type:VARCHAR(512)"`
	TenantUUID           uuid.UUID `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" validate:"required" json:"-"`
	CustomerID           uuid.UUID `gorm:"type:uuid;"`
	Scope                string    `gorm:"type:VARCHAR(16);default:CLOUD" validate:"required"`
	Priority             int       `gorm:"type:INT;default:301" validate:"required"`
	ActionType           string    `gorm:"type:VARCHAR(32);default:FORWARD" validate:"required"`
	SourceDevice         string    `gorm:"type:VARCHAR(32)"`
	SourceVendor         string    `gorm:"type:VARCHAR(32)"`
	Status               int       `validate:"min=0,max=5"`
	Type                 string    `gorm:"type:VARCHAR(32)" validate:"required"`
	SamplingRate         int64     `gorm:"type:BIGINT;"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	CreatedBy            string `gorm:"type:VARCHAR(40)"`
	UpdatedBy            string `gorm:"type:VARCHAR(40)"`
	RuleFilters          datatypes.JSON
	RuleFilterQuery      string
	AggregationConfig    datatypes.JSONType[AggregationConfig]
	SuppressionConfig    datatypes.JSONType[SuppressionConfig]
	AggregationQuery     string
	AggregationTableName string `gorm:"type:VARCHAR(64)"`
	StudioRuleID         string `gorm:"type:VARCHAR(36)"`
	IsTaggingRule        bool
	StudioRuleId         string `gorm:"type:VARCHAR(36)"`
	UserModified         bool
	ReleaseNumber        string `gorm:"type:VARCHAR(32)"`
	ReferencedAttributes datatypes.JSON
}

func GetAllV1(ctx context.Context, db *gorm.DB) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Find(&rules).Error
	return rules, err
}

func GetAllRulesBySourceIdV1(ctx context.Context, db *gorm.DB, sourceId string, tenantId string) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Table("rule").
		Select("db_rule.*").
		Joins("JOIN db_destination ON db_rule.destination_id = db_destination.id").
		Joins("JOIN db_event_dependency ON db_destination.id = db_event_dependency.destination_id").
		Where("db_event_dependency.event_source_id = ? AND db_event_dependency.tenant_uuid = ?", sourceId, tenantId).
		Find(&rules).Error
	return rules, err
}

func GetAllWithTagsV1(ctx context.Context, db *gorm.DB) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules).Error
	return rules, err
}

func GetAllRulesByScopeAndStatusV1(ctx context.Context, db *gorm.DB, scope string, status int) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Find(&rules, "scope = ? AND status = ?", scope, status).Error
	return rules, err
}

func GetRulesByColumnV1(ctx context.Context, db *gorm.DB, column string, value any) (d []RuleV1, err error) {
	err = db.WithContext(ctx).Where(column, value).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []RuleV1{}
		return d, nil
	}
	return d, err
}

func GetRulesByScopeAndStatusV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, scope string, status int) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Find(&rules, "tenant_uuid = ? AND scope = ? AND status = ?", tenantId, scope, status).Error
	return rules, err
}

func GetRulesByScopeAndStatusWithTagsV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, scope string, status int) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules, "tenant_uuid = ? AND scope = ? AND status = ?", tenantId, scope, status).Error
	return rules, err
}

func GetRuleByDestinationIdV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, dId uuid.UUID) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Find(&rules, "destination_id = ? AND tenant_uuid = ?", dId, tenantId).Error
	return rules, err
}

func GetV1(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId string) (rule RuleV1, err error) {
	err = db.WithContext(ctx).First(&rule, "id = ? AND tenant_uuid = ?", id, tenantId).Error
	return rule, err
}

func GetWithTagsV1(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId string) (rule RuleV1, err error) {
	err = db.WithContext(ctx).Preload("Tags").First(&rule, "id = ? AND tenant_uuid = ?", id, tenantId).Error
	return rule, err
}

func (r *RuleV1) SaveV1(ctx context.Context, db *gorm.DB) (err error) {
	return db.WithContext(ctx).Create(&r).Error
}

func (r *RuleV1) DeleteV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) error {
	return db.WithContext(ctx).Delete(&r, "tenant_uuid = ?", tenantId).Error
}

func (r *RuleV1) UpdateV1(ctx context.Context, db *gorm.DB, updated any) error {
	return db.WithContext(ctx).Model(&r).Updates(updated).Error
}

func GetAllByTenantV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Find(&rules, "tenant_uuid = ?", tenantId).Error
	return rules, err
}

func GetAllByTenantWithTagsV1(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (rules []RuleV1, err error) {
	err = db.WithContext(ctx).Preload("Tags").Find(&rules, "tenant_uuid = ?", tenantId).Error
	return rules, err
}

func GetNewRuleV1Object(newRule Rule) RuleV1 {
	ruleV1 := RuleV1{
		ID:                   newRule.ID,
		Name:                 newRule.Name,
		HistoryVersion:       newRule.HistoryVersion,
		Conditions:           newRule.Conditions,
		Description:          newRule.Description,
		TenantUUID:           newRule.TenantUUID,
		CustomerID:           uuid.New(),
		Scope:                newRule.Scope,
		Priority:             newRule.Priority,
		ActionType:           newRule.ActionType,
		SourceDevice:         newRule.SourceDevice,
		SourceVendor:         newRule.SourceVendor,
		Status:               newRule.Status,
		Type:                 newRule.Type,
		SamplingRate:         newRule.SamplingRate,
		CreatedAt:            newRule.CreatedAt,
		UpdatedAt:            newRule.UpdatedAt,
		CreatedBy:            newRule.CreatedBy,
		UpdatedBy:            newRule.UpdatedBy,
		RuleFilters:          newRule.RuleFilters,
		RuleFilterQuery:      newRule.RuleFilterQuery,
		AggregationConfig:    newRule.AggregationConfig,
		SuppressionConfig:    newRule.SuppressionConfig,
		AggregationQuery:     newRule.AggregationQuery,
		AggregationTableName: newRule.AggregationTableName,
		StudioRuleID:         newRule.StudioRuleID,
		IsTaggingRule:        newRule.IsTaggingRule,
		StudioRuleId:         newRule.StudioRuleID,
		ReleaseNumber:        newRule.ReleaseNumber,
		ReferencedAttributes: newRule.ReferencedAttributes,
	}
	return ruleV1
}
