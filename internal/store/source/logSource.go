package source

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Configuration struct {
	Configuration map[string]interface{} `json:"configuration"`
}

type AdvancedConfiguration struct {
	SendUnmatchedEventToPrimaryDestination bool `json:"sendUnmatchedEventToPrimaryDestination"`
}

type Source struct {
	ID                           uuid.UUID              `gorm:"type:uuid;primary_key" json:"id"`
	Configuration                datatypes.JSON         `json:"configuration"`
	CreatedAt                    time.Time              `gorm:"type:timestamp" json:"created_at"`
	CreatedBy                    uuid.UUID              `gorm:"type:uuid" json:"created_by"`
	CustomerID                   uuid.UUID              `gorm:"type:uuid" json:"customer_id"`
	Description                  string                 `gorm:"type:varchar(512)" json:"description"`
	Device                       string                 `gorm:"type:varchar(30)" json:"device"`
	LogType                      string                 `gorm:"type:varchar(30)" json:"log_type"`
	Name                         string                 `gorm:"type:varchar(100)" json:"name"`
	ReplaySource                 bool                   `json:"replay_source"`
	Reputation                   string                 `gorm:"type:varchar(255)" json:"reputation"`
	Scope                        string                 `gorm:"type:varchar(255)" json:"scope"`
	Status                       string                 `gorm:"type:varchar(255)" json:"status"`
	TenantID                     uuid.UUID              `gorm:"type:uuid" json:"tenant_id"`
	TimestampOverrideEnabled     bool                   `json:"timestamp_override_enabled"`
	TimezoneNormalizationEnabled bool                   `json:"timezone_normalization_enabled"`
	UpdatedAt                    time.Time              `gorm:"type:timestamp" json:"updated_at"`
	UpdatedBy                    uuid.UUID              `gorm:"type:uuid" json:"updated_by"`
	Vendor                       string                 `gorm:"type:varchar(30)" json:"vendor"`
	Version                      string                 `gorm:"type:varchar(36)" json:"version"`
	Config                       map[string]interface{} `gorm:"-" json:"-"`
	DataPlaneId                  uuid.UUID              `gorm:"type:uuid" json:"data_plane_id"`
	AdvancedConfiguration        datatypes.JSON         `gorm:"type:jsonb;column:advanced_configuration" json:"advanced_configuration"`
}

func (s *Source) TableName() string {
	return "log_source"
}

// GetAdvancedConfiguration parses and returns the advanced configuration from JSON
func (s *Source) GetAdvancedConfiguration() (*AdvancedConfiguration, error) {
	if len(s.AdvancedConfiguration) == 0 {
		return &AdvancedConfiguration{}, nil
	}

	var config AdvancedConfiguration
	err := json.Unmarshal([]byte(s.AdvancedConfiguration), &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// GetSourcesByTenantAndStatus gets all log sources for a tenant with the given status
func GetSourcesByTenantAndStatus(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, status string) ([]Source, error) {
	var sources []Source
	err := db.WithContext(ctx).
		Where("tenant_id = ? AND status = ?", tenantId, status).
		Find(&sources).Error
	if err != nil {
		return nil, fmt.Errorf("error getting sources for tenant %s with status %s: %w", tenantId.String(), status, err)
	}
	return sources, nil
}

// GetSourceByID gets a log source by its ID
func GetSourceByID(ctx context.Context, db *gorm.DB, sourceID uuid.UUID) (*Source, error) {
	var source Source
	err := db.WithContext(ctx).Where("id = ?", sourceID).First(&source).Error
	if err != nil {
		return nil, fmt.Errorf("error getting source with ID %s: %w", sourceID.String(), err)
	}
	return &source, nil
}

// ReadSourcesPaginated reads sources in paginated fashion for a given tenant
// Returns sources with specified statuses for the given tenant with pagination support
func ReadSourcesPaginated(db *gorm.DB, tenantId uuid.UUID, page, pageSize int, statuses []string) ([]Source, error) {
	var sources []Source
	offset := page * pageSize

	result := db.
		Where("tenant_id = ? AND status IN ?", tenantId, statuses).
		Limit(pageSize).
		Offset(offset).
		Find(&sources)

	return sources, result.Error
}

type SourceFleetInfo struct {
	FleetId   string
	FleetName string
}

type SourceAgentInfo struct {
	AgentId   string
	AgentName string
}

// GetFleetsBySourceIDs returns a map of source ID -> list of fleet info for the given source IDs.
// The relationship is: log_source -> source_connector_mapping -> connector -> fleet
func GetFleetsBySourceIDs(db *gorm.DB, sourceIDs []uuid.UUID) (map[string][]SourceFleetInfo, error) {
	if len(sourceIDs) == 0 {
		return make(map[string][]SourceFleetInfo), nil
	}

	type row struct {
		SourceId  string `gorm:"column:source_id"`
		FleetId   string `gorm:"column:fleet_id"`
		FleetName string `gorm:"column:fleet_name"`
	}

	var rows []row
	err := db.Raw(`
		SELECT DISTINCT scm.source_id::text AS source_id, f.id::text AS fleet_id, f.name AS fleet_name
		FROM source_connector_mapping scm
		JOIN connector c ON scm.connector_id = c.id AND scm.tenant_id = c.tenant_id
		JOIN fleet f ON c.fleet_id = f.id AND c.tenant_id = f.tenant_id
		WHERE scm.source_id IN ?
	`, sourceIDs).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("error getting fleets for sources: %w", err)
	}

	result := make(map[string][]SourceFleetInfo)
	for _, r := range rows {
		result[r.SourceId] = append(result[r.SourceId], SourceFleetInfo{
			FleetId:   r.FleetId,
			FleetName: r.FleetName,
		})
	}
	return result, nil
}

// GetAgentsBySourceIDs returns a map of source ID -> list of agent info for the given source IDs.
// The relationship is: log_source -> collection_profile_log_source_mapping -> collection_profile
//
//	-> collection_profile_tag_mapping -> tag -> agent_tag_mapping -> agent_node
func GetAgentsBySourceIDs(db *gorm.DB, sourceIDs []uuid.UUID) (map[string][]SourceAgentInfo, error) {
	if len(sourceIDs) == 0 {
		return make(map[string][]SourceAgentInfo), nil
	}

	type row struct {
		SourceId  string `gorm:"column:source_id"`
		AgentId   string `gorm:"column:agent_id"`
		AgentName string `gorm:"column:agent_name"`
	}

	var rows []row
	err := db.Raw(`
		SELECT DISTINCT cplsm.log_source_id::text AS source_id, a.id::text AS agent_id, a.name AS agent_name
		FROM collection_profile_log_source_mapping cplsm
		JOIN collection_profile cp ON cplsm.collection_profile_id = cp.id AND cplsm.tenant_id = cp.tenant_id
		JOIN collection_profile_tag_mapping cptm ON cp.id = cptm.collection_profile_id AND cp.tenant_id = cptm.tenant_id
		JOIN agent_tag_mapping atm ON cptm.tag_id = atm.tag_id AND cptm.tenant_id = atm.tenant_id
		JOIN agent_node a ON atm.agent_id = a.id AND atm.tenant_id = a.tenant_id
		WHERE cplsm.log_source_id IN ?
		AND a.status IN ('ACTIVE', 'ERRORED')
	`, sourceIDs).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("error getting agents for sources: %w", err)
	}

	result := make(map[string][]SourceAgentInfo)
	for _, r := range rows {
		result[r.SourceId] = append(result[r.SourceId], SourceAgentInfo{
			AgentId:   r.AgentId,
			AgentName: r.AgentName,
		})
	}
	return result, nil
}
