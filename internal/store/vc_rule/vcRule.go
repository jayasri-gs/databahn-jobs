package vc_rule

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Constants for VC Rule enums
const (
	// ActionType values
	ActionTypePublish ActionType = "PUBLISH"
	ActionTypeDrop    ActionType = "DROP"

	// Status values
	StatusActive       Status = "ACTIVE"
	StatusDeleted      Status = "DELETED"
	StatusDeploying    Status = "DEPLOYING"
	StatusDisabled     Status = "DISABLED"
	StatusDisableError Status = "DISABLE_ERROR"

	// Scope values
	ScopeTest  Scope = "TEST"
	ScopeCloud Scope = "CLOUD"

	// Type values
	TypeAggregation    Type = "AGGREGATION"
	TypeTransformation Type = "TRANSFORMATION"
	TypeRouteProcessor Type = "ROUTE_PROCESSOR"
	TypeDataEnrichment Type = "DATA_ENRICHMENT"
)

// Type definitions
type ActionType string
type Status string
type Scope string
type Type string

// Supporting structs for JSON fields
type AggregationConfig struct {
	Interval          Interval          `json:"interval"`
	OutputMode        OutputMode        `json:"outputMode"`
	SelectAttributes  []SelectAttribute `json:"selectAttributes"`
	GroupByAttributes []string          `json:"groupByAttributes"`
}

type Interval struct {
	Unit  string `json:"unit"`
	Value int    `json:"value"`
}

type OutputMode struct {
	Stats       bool `json:"stats"`
	PassThrough bool `json:"passThrough"`
}

type SelectAttribute struct {
	Label     string        `json:"label"`
	Function  string        `json:"function"`
	Implicit  bool          `json:"implicit"`
	Arguments []interface{} `json:"arguments"`
	Attribute string        `json:"attribute"`
}

type RuleFilters struct {
	Filter FilterConfig `json:"filter"`
	Schema string       `json:"schema"`
}

type FilterConfig struct {
	Rules      []FilterRule `json:"rules"`
	Combinator string       `json:"combinator"`
}

type FilterRule struct {
	Field    string      `json:"field"`
	Value    interface{} `json:"value"`
	Operator string      `json:"operator"`
}

// VCRule represents a Volume Control rule in the database
type VCRule struct {
	ID                   uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	ActionType           ActionType     `gorm:"type:varchar(50)" json:"action_type"`
	AggregationConfig    datatypes.JSON `gorm:"type:jsonb" json:"aggregation_config"`
	AggregationQuery     string         `gorm:"type:text" json:"aggregation_query"`
	AggregationTableName string         `gorm:"type:varchar(255)" json:"aggregation_table_name"`
	Conditions           string         `gorm:"type:text" json:"conditions"`
	CreatedAt            *time.Time     `gorm:"type:timestamp" json:"created_at"`
	CreatedBy            uuid.UUID      `gorm:"type:uuid" json:"created_by"`
	CustomerID           uuid.UUID      `gorm:"type:uuid" json:"customer_id"`
	Description          string         `gorm:"type:text" json:"description"`
	DestinationID        uuid.UUID      `gorm:"type:uuid" json:"destination_id"`
	LogSourceID          uuid.UUID      `gorm:"type:uuid" json:"log_source_id"`
	Name                 string         `gorm:"type:varchar(255)" json:"name"`
	Priority             int            `gorm:"type:int" json:"priority"`
	ReferencedAttributes datatypes.JSON `gorm:"type:jsonb" json:"referenced_attributes"`
	ReleaseNumber        string         `gorm:"type:varchar(100)" json:"release_number"`
	Query                string         `gorm:"type:text" json:"query"`
	RuleFilters          datatypes.JSON `gorm:"type:jsonb" json:"rule_filters"`
	SamplingRate         int            `gorm:"type:int" json:"sampling_rate"`
	Scope                Scope          `gorm:"type:varchar(50)" json:"scope"`
	Status               Status         `gorm:"type:varchar(50)" json:"status"`
	StudioRuleID         string         `gorm:"type:varchar(255)" json:"studio_rule_id"`
	SuppressionConfig    string         `gorm:"type:text" json:"suppression_config"`
	Tag                  string         `gorm:"type:varchar(255)" json:"tag"`
	TenantID             uuid.UUID      `gorm:"type:uuid" json:"tenant_id"`
	Type                 Type           `gorm:"type:varchar(50)" json:"type"`
	UpdatedAt            *time.Time     `gorm:"type:timestamp" json:"updated_at"`
	UpdatedBy            uuid.UUID      `gorm:"type:uuid" json:"updated_by"`
	UserModified         bool           `gorm:"type:boolean;default:false" json:"user_modified"`
	PipelineID           uuid.UUID      `gorm:"type:uuid" json:"pipeline_id"`
	DataPlaneID          uuid.UUID      `gorm:"type:uuid" json:"data_plane_id"`
}

// TableName returns the custom table name for VCRule
func (v VCRule) TableName() string {
	return "vc_rule"
}

// GetAggregationConfig parses and returns the aggregation config from JSON
func (v VCRule) GetAggregationConfig() (*AggregationConfig, error) {
	if len(v.AggregationConfig) == 0 {
		return nil, nil
	}

	var config AggregationConfig
	err := json.Unmarshal([]byte(v.AggregationConfig), &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// GetRuleFilters parses and returns the rule filters from JSON
func (v VCRule) GetRuleFilters() (*RuleFilters, error) {
	if len(v.RuleFilters) == 0 {
		return nil, nil
	}

	var filters RuleFilters
	err := json.Unmarshal([]byte(v.RuleFilters), &filters)
	if err != nil {
		return nil, err
	}

	return &filters, nil
}

// GetReferencedAttributes parses and returns the referenced attributes from JSON
func (v VCRule) GetReferencedAttributes() ([]string, error) {
	if len(v.ReferencedAttributes) == 0 {
		return nil, nil
	}

	var attributes []string
	err := json.Unmarshal([]byte(v.ReferencedAttributes), &attributes)
	if err != nil {
		return nil, err
	}

	return attributes, nil
}

// GetVCRuleByID retrieves a VC rule by ID
func GetVCRuleByID(id uuid.UUID, db *gorm.DB) (*VCRule, error) {
	var rule VCRule
	err := db.Where("id = ?", id).First(&rule).Error
	if err != nil {
		return nil, err
	}
	return &rule, nil
}

// GetActiveVCRulesByTenantID retrieves all active VC rules for a tenant
func GetActiveVCRulesByTenantID(tenantID uuid.UUID, db *gorm.DB) ([]VCRule, error) {
	var rules []VCRule
	err := db.Where("tenant_id = ? AND status = ?", tenantID, StatusActive).Find(&rules).Error
	return rules, err
}

// GetVCRulesByPipelineID retrieves all VC rules for a specific pipeline
func GetVCRulesByPipelineID(pipelineID uuid.UUID, db *gorm.DB) ([]VCRule, error) {
	var rules []VCRule
	err := db.Where("pipeline_id = ?", pipelineID).Find(&rules).Error
	return rules, err
}

// GetOperationalVCRulesByTenantID retrieves all operational (active or disabled) VC rules for a tenant
func GetOperationalVCRulesByTenantID(tenantID uuid.UUID, db *gorm.DB) ([]VCRule, error) {
	var rules []VCRule
	err := db.Where("tenant_id = ? AND status IN (?, ?)", tenantID, StatusActive, StatusDisabled).Find(&rules).Error
	return rules, err
}

// GetActiveVCRulesByPipelineAndTenant retrieves all active VC rules for a specific pipeline and tenant
func GetActiveVCRulesByPipelineAndTenant(pipelineID, tenantID uuid.UUID, db *gorm.DB) ([]VCRule, error) {
	var rules []VCRule
	err := db.Where("pipeline_id = ? AND tenant_id = ? AND status = ?", pipelineID, tenantID, StatusActive).Find(&rules).Error
	return rules, err
}

// GetLowestPriorityActiveRule retrieves the active VC rule with the lowest priority (highest priority number) for a specific pipeline and tenant
func GetLowestPriorityActiveRule(pipelineID, tenantID uuid.UUID, db *gorm.DB) (*VCRule, error) {
	var rule VCRule
	err := db.Where("pipeline_id = ? AND tenant_id = ? AND status = ?", pipelineID, tenantID, StatusActive).
		Order("priority DESC"). // Higher priority number = lower priority
		First(&rule).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil // No rules found
		}
		return nil, err
	}

	return &rule, nil
}
