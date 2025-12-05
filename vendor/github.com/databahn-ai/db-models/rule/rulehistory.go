package rule

import (
	"time"

	"github.com/databahn-ai/db-models/tags"
	uuid "github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type RuleHistory struct {
	ID                   uuid.UUID `gorm:"type:uuid;primaryKey"`
	Name                 string    `gorm:"type:VARCHAR(100)"`
	HistoryVersion       int       `gorm:"type:INT;default:1;primaryKey"`
	ModelVersion         string    `gorm:"type:VARCHAR(8)"`
	Conditions           string    `gorm:"type:TEXT"`
	Description          string    `gorm:"type:VARCHAR(512)"`
	TenantUUID           uuid.UUID `gorm:"type:uuid;primaryKey" json:"-"`
	EventDependencyID    uuid.UUID `gorm:"type:uuid"`
	DestinationId        uuid.UUID `gorm:"type:uuid"`
	EventSourceId        uuid.UUID `gorm:"type:uuid"`
	Scope                string    `gorm:"type:VARCHAR(16)"`
	Priority             int       `gorm:"type:INT"`
	ActionType           string    `gorm:"type:VARCHAR(16)"`
	SourceDevice         string    `gorm:"type:VARCHAR(32)"`
	SourceVendor         string    `gorm:"type:VARCHAR(32)"`
	Status               int       `gorm:"type:INT"`
	Type                 string    `gorm:"type:VARCHAR(30)"`
	SamplingRate         int64     `gorm:"type:BIGINT;"`
	State                string    `gorm:"type:VARCHAR(20)"`
	CreatedBy            string    `gorm:"type:VARCHAR(40)"`
	UpdatedBy            string    `gorm:"type:VARCHAR(40)"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	RuleFilterQuery      string
	RuleFilters          datatypes.JSON
	Tags                 datatypes.JSONType[[]tags.Tags]
	SuppressionConfig    datatypes.JSONType[SuppressionConfig]
	AggregationConfig    datatypes.JSONType[AggregationConfig]
	AggregationQuery     string
	AggregationTableName string `gorm:"type:VARCHAR(64)"`
	StudioRuleID         string `gorm:"type:VARCHAR(36)"`
	IsTaggingRule        bool
	UserModified         bool
	ReleaseNumber        string `gorm:"type:VARCHAR(16)"`
	ReferencedAttributes datatypes.JSON
}

func NewRuleHistory(rule Rule, eventSourceId uuid.UUID) *RuleHistory {
	h := RuleHistory{}
	h.ID = rule.ID
	h.Name = rule.Name
	h.HistoryVersion = rule.HistoryVersion
	h.ModelVersion = ModelV1
	h.Conditions = rule.Conditions
	h.Description = rule.Description
	h.TenantUUID = rule.TenantUUID
	h.EventDependencyID = rule.EventDependencyID
	h.DestinationId = rule.DestinationId
	h.EventSourceId = eventSourceId
	h.Scope = rule.Scope
	h.Priority = rule.Priority
	h.ActionType = rule.ActionType
	h.SourceDevice = rule.SourceDevice
	h.SourceVendor = rule.SourceVendor
	h.Status = rule.Status
	h.Type = rule.Type
	h.SamplingRate = rule.SamplingRate
	h.State = rule.State
	h.CreatedBy = rule.CreatedBy
	h.UpdatedBy = rule.UpdatedBy
	h.CreatedAt = rule.CreatedAt
	h.UpdatedAt = rule.UpdatedAt
	h.RuleFilterQuery = rule.RuleFilterQuery
	h.RuleFilters = rule.RuleFilters
	h.Tags = datatypes.NewJSONType(rule.Tags)
	h.AggregationConfig = rule.AggregationConfig
	h.SuppressionConfig = rule.SuppressionConfig
	h.AggregationQuery = rule.AggregationQuery
	h.AggregationTableName = rule.AggregationTableName
	h.IsTaggingRule = rule.IsTaggingRule
	h.StudioRuleID = rule.StudioRuleID
	h.UserModified = rule.UserModified
	h.ReferencedAttributes = rule.ReferencedAttributes
	h.ReleaseNumber = rule.ReleaseNumber
	return &h
}

func GetAllRuleHistoryByRuleIdAndTenantId(db *gorm.DB, ruleId, tenantId string) ([]RuleHistory, error) {
	var history []RuleHistory
	err := db.Where("id = ? AND tenant_uuid = ?", ruleId, tenantId).Order("history_version DESC").Find(&history).Error
	return history, err
}

func GetRuleHistoryByRuleIdAndTenantIdAndVersion(db *gorm.DB, ruleId, tenantId string, version int) (RuleHistory, error) {
	history := RuleHistory{}
	err := db.Where("id = ? AND tenant_uuid = ? AND history_version = ?", ruleId, tenantId, version).First(&history).Error
	return history, err
}
func (h *RuleHistory) Save(db *gorm.DB) error {
	return db.Create(h).Error
}
