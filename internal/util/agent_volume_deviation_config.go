package util

import (
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	// DefaultConfigBatchSize is the default batch size for fetching configs from database
	DefaultConfigBatchSize = 100
)

// AgentVolumeDeviationConfig represents the resolved configuration for agent volume deviation alerts
type AgentVolumeDeviationConfig struct {
	Enabled                     bool
	PercentageIncreaseThreshold float64
	PercentageDecreaseThreshold float64
	MinimumVolumeThreshold      float64 // in bytes
	MinimumVolumeThresholdUnit  string  // unit (MB, GB, TB) - stored for reference
}

// ResolveAgentVolumeDeviationConfig resolves configuration following the hierarchy:
// 1. Agent-level config (highest priority)
// 2. Tenant-level config (medium priority)
func ResolveAgentVolumeDeviationConfig(
	db *gorm.DB,
	tenantId, agentId uuid.UUID,
) (*AgentVolumeDeviationConfig, error) {
	configs, err := ResolveAgentVolumeDeviationConfigsBatch(db, map[uuid.UUID]uuid.UUID{agentId: tenantId}, nil)
	if err != nil {
		return nil, err
	}
	return configs[agentId], nil
}

// ResolveAgentVolumeDeviationConfigsBatch resolves configurations for multiple agents in batch
func ResolveAgentVolumeDeviationConfigsBatch(
	db *gorm.DB,
	agentToTenantMap map[uuid.UUID]uuid.UUID,
	tenantConfigCache map[uuid.UUID]*entities.EntityAlertsConfig,
) (map[uuid.UUID]*AgentVolumeDeviationConfig, error) {
	result := make(map[uuid.UUID]*AgentVolumeDeviationConfig)

	if len(agentToTenantMap) == 0 {
		return result, nil
	}

	tenantToAgents := groupAgentsByTenant(agentToTenantMap)

	agentConfigsByAgentId, err := fetchAgentLevelConfigs(db, tenantToAgents)
	if err != nil {
		return nil, err
	}

	tenantConfigsByTenantId, err := fetchTenantLevelConfigs(db, tenantToAgents, tenantConfigCache)
	if err != nil {
		return nil, err
	}

	// Resolve config for each agent following hierarchy
	for agentId, tenantId := range agentToTenantMap {
		resolvedConfig := resolveConfigForAgent(
			agentId,
			tenantId,
			agentConfigsByAgentId,
			tenantConfigsByTenantId,
		)
		if resolvedConfig != nil {
			result[agentId] = resolvedConfig
		}
	}

	return result, nil
}

// groupAgentsByTenant groups agents by their tenant ID
func groupAgentsByTenant(agentToTenantMap map[uuid.UUID]uuid.UUID) map[uuid.UUID][]uuid.UUID {
	tenantToAgents := make(map[uuid.UUID][]uuid.UUID)
	for agentId, tenantId := range agentToTenantMap {
		tenantToAgents[tenantId] = append(tenantToAgents[tenantId], agentId)
	}
	return tenantToAgents
}

// fetchAgentLevelConfigs fetches agent-level configs in batches
func fetchAgentLevelConfigs(
	db *gorm.DB,
	tenantToAgents map[uuid.UUID][]uuid.UUID,
) (map[uuid.UUID]entities.EntityAlertsConfig, error) {
	agentConfigsByAgentId := make(map[uuid.UUID]entities.EntityAlertsConfig)

	for tenantId, agentIds := range tenantToAgents {
		// Split agent IDs into chunks to avoid unbounded queries
		for i := 0; i < len(agentIds); i += DefaultConfigBatchSize {
			end := i + DefaultConfigBatchSize
			if end > len(agentIds) {
				end = len(agentIds)
			}
			agentIdsChunk := agentIds[i:end]

			agentConfigs, err := entities.ReadEntityConfigs(
				db,
				entities.AgentEntityType,
				"AGENT_VOLUME_DEVIATION",
				tenantId,
				agentIdsChunk,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to read agent-level configs for tenant %s (chunk %d-%d): %w", tenantId.String(), i, end, err)
			}

			for _, agentConfig := range agentConfigs {
				agentConfigsByAgentId[agentConfig.EntityID] = agentConfig
			}
		}
	}

	return agentConfigsByAgentId, nil
}

// fetchTenantLevelConfigs fetches tenant-level configs, using cache when available
func fetchTenantLevelConfigs(
	db *gorm.DB,
	tenantToAgents map[uuid.UUID][]uuid.UUID,
	tenantConfigCache map[uuid.UUID]*entities.EntityAlertsConfig,
) (map[uuid.UUID]*entities.EntityAlertsConfig, error) {
	tenantConfigsByTenantId := make(map[uuid.UUID]*entities.EntityAlertsConfig)

	for tenantId := range tenantToAgents {
		// Check cache first
		if cachedConfig := getCachedTenantConfig(tenantId, tenantConfigCache); cachedConfig != nil {
			tenantConfigsByTenantId[tenantId] = cachedConfig
			continue
		}

		// Cache miss - fetch from database
		tenantConfigs, err := entities.ReadTenantLevelConfigs(
			db,
			"AGENT_VOLUME_DEVIATION",
			tenantId,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to read tenant-level config for tenant %s: %w", tenantId.String(), err)
		}

		if len(tenantConfigs) > 0 {
			config := &tenantConfigs[0]
			tenantConfigsByTenantId[tenantId] = config
			// Populate cache for future use
			if tenantConfigCache != nil {
				tenantConfigCache[tenantId] = config
			}
		}
	}

	return tenantConfigsByTenantId, nil
}

// getCachedTenantConfig retrieves tenant config from cache if available
func getCachedTenantConfig(
	tenantId uuid.UUID,
	tenantConfigCache map[uuid.UUID]*entities.EntityAlertsConfig,
) *entities.EntityAlertsConfig {
	if tenantConfigCache != nil {
		if cachedConfig, ok := tenantConfigCache[tenantId]; ok {
			return cachedConfig
		}
	}
	return nil
}

// resolveConfigForAgent resolves configuration for a single agent following the hierarchy:
// 1. Agent-level config (highest priority)
// 2. Tenant-level config (medium priority)
// Returns nil if alerts are disabled at all levels
func resolveConfigForAgent(
	agentId, tenantId uuid.UUID,
	agentConfigsByAgentId map[uuid.UUID]entities.EntityAlertsConfig,
	tenantConfigsByTenantId map[uuid.UUID]*entities.EntityAlertsConfig,
) *AgentVolumeDeviationConfig {
	agentConfig, agentConfigExists := agentConfigsByAgentId[agentId]
	if agentConfigExists {
		if agentConfig.Config != nil && !agentConfig.Config.Enabled {
			return nil
		}
		if config := resolveAgentLevelConfig(agentId, agentConfigsByAgentId); config != nil {
			return config
		}
	}
	if config := resolveTenantLevelConfig(tenantId, tenantConfigsByTenantId); config != nil {
		return config
	}

	return nil
}

// resolveAgentLevelConfig resolves agent-level config if available and enabled
func resolveAgentLevelConfig(
	agentId uuid.UUID,
	agentConfigsByAgentId map[uuid.UUID]entities.EntityAlertsConfig,
) *AgentVolumeDeviationConfig {
	agentConfig, ok := agentConfigsByAgentId[agentId]
	if !ok {
		return nil
	}

	if agentConfig.Config == nil {
		return nil
	}

	if !agentConfig.Config.Enabled {
		return nil // Agent-level config disabled
	}

	if agentConfig.Config.AgentVolumeDeviationAlertConfig == nil {
		return nil // Agent config enabled but alert config is nil - fall through to tenant/default
	}

	return buildConfigFromAlertConfig(agentConfig.Config.AgentVolumeDeviationAlertConfig, true)
}

// resolveTenantLevelConfig resolves tenant-level config if available and enabled
func resolveTenantLevelConfig(
	tenantId uuid.UUID,
	tenantConfigsByTenantId map[uuid.UUID]*entities.EntityAlertsConfig,
) *AgentVolumeDeviationConfig {
	tenantConfig, ok := tenantConfigsByTenantId[tenantId]
	if !ok {
		return nil
	}

	if tenantConfig.Config == nil {
		return nil
	}

	if !tenantConfig.Config.Enabled {
		return nil // Tenant-level config disabled
	}

	if tenantConfig.Config.AgentVolumeDeviationAlertConfig == nil {
		return nil // Tenant config enabled but alert config is nil - fall through to default
	}

	return buildConfigFromAlertConfig(tenantConfig.Config.AgentVolumeDeviationAlertConfig, true)
}

// buildConfigFromAlertConfig builds AgentVolumeDeviationConfig from entities.AgentVolumeDeviationAlertConfig
func buildConfigFromAlertConfig(
	alertConfig *entities.AgentVolumeDeviationAlertConfig,
	enabled bool,
) *AgentVolumeDeviationConfig {
	config := &AgentVolumeDeviationConfig{
		Enabled:                     enabled,
		PercentageIncreaseThreshold: float64(alertConfig.PercentageIncreaseThreshold),
		PercentageDecreaseThreshold: float64(alertConfig.PercentageDecreaseThreshold),
		MinimumVolumeThresholdUnit:  alertConfig.MinimumVolumeThresholdUnit,
	}

	if alertConfig.MinimumVolumeThreshold > 0 {
		config.MinimumVolumeThreshold = float64(DataVolumeToBytes(
			alertConfig.MinimumVolumeThreshold,
			alertConfig.MinimumVolumeThresholdUnit,
		))
	} else {
		config.MinimumVolumeThreshold = 0
	}

	return config
}
