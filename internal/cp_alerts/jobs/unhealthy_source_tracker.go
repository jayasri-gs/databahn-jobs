package jobs

import (
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AgentUnhealthySourceInfo represents an agent that is not sending data for a source
type AgentUnhealthySourceInfo struct {
	AgentId       string
	AgentName     string
	LastEventTime time.Time
}

// AgentUnhealthySourceTracker tracks unhealthy agents per source across the job run
type AgentUnhealthySourceTracker struct {
	// sourceId -> source metadata + list of unhealthy agents
	sourceUnhealthyAgents map[string]*SourceUnhealthyData
	// All processed source IDs (to clear stale entries even if source is now healthy)
	processedSourceIds map[string]bool
}

// SourceUnhealthyData holds source metadata and its unhealthy agents
type SourceUnhealthyData struct {
	SourceName      string
	UnhealthyAgents []AgentUnhealthySourceInfo
}

// NewAgentUnhealthySourceTracker creates a new tracker
func NewAgentUnhealthySourceTracker() *AgentUnhealthySourceTracker {
	return &AgentUnhealthySourceTracker{
		sourceUnhealthyAgents: make(map[string]*SourceUnhealthyData),
		processedSourceIds:    make(map[string]bool),
	}
}

// MarkSourceProcessed tracks that a source was processed (for clearing stale entries)
func (t *AgentUnhealthySourceTracker) MarkSourceProcessed(sourceId string) {
	t.processedSourceIds[sourceId] = true
}

// AddUnhealthyAgents marks agents as unhealthy for a source
func (t *AgentUnhealthySourceTracker) AddUnhealthyAgents(
	sourceId string,
	sourceName string,
	agents []AgentUnhealthySourceInfo) {

	t.processedSourceIds[sourceId] = true
	if _, exists := t.sourceUnhealthyAgents[sourceId]; !exists {
		t.sourceUnhealthyAgents[sourceId] = &SourceUnhealthyData{
			SourceName:      sourceName,
			UnhealthyAgents: []AgentUnhealthySourceInfo{},
		}
	}
	t.sourceUnhealthyAgents[sourceId].UnhealthyAgents = append(
		t.sourceUnhealthyAgents[sourceId].UnhealthyAgents, agents...)
}

// SaveToDatabase inserts agent_silent_sources table entries for a tenant.
// Clears existing entries for all processed sources (including healthy ones) before inserting new ones.
func (t *AgentUnhealthySourceTracker) SaveToDatabase(db *gorm.DB, tenantId string) error {
	if len(t.processedSourceIds) == 0 {
		logger.GetLogger().Info("no agent-scoped sources processed, skipping db update")
		return nil
	}

	// Delete old entries for ALL processed sources (including healthy ones to clear stale data)
	for sourceId := range t.processedSourceIds {
		if err := db.
			Table("agent_silent_sources").
			Where("tenant_id = ? AND source_id = ?", tenantId, sourceId).
			Delete(nil).
			Error; err != nil {
			logger.GetLogger().Error("error deleting old silent sources from db", zap.Error(err), zap.String("sourceId", sourceId))
		}
	}

	// Insert new entries only for sources with unhealthy agents
	now := time.Now().UTC()
	for sourceId, sourceData := range t.sourceUnhealthyAgents {
		for _, agentInfo := range sourceData.UnhealthyAgents {
			if err := db.
				Table("agent_silent_sources").
				Create(map[string]interface{}{
					"id":                      uuid.NewString(),
					"tenant_id":               tenantId,
					"agent_id":                agentInfo.AgentId,
					"source_id":               sourceId,
					"source_name":             sourceData.SourceName,
					"last_inactive_timestamp": agentInfo.LastEventTime.UnixMilli(),
					"created_at":              now,
					"updated_at":              now,
				}).
				Error; err != nil {
				logger.GetLogger().Error("error inserting silent source to db",
					zap.Error(err),
					zap.String("agentId", agentInfo.AgentId),
					zap.String("sourceId", sourceId))
				continue
			}
		}
	}

	totalAgents := 0
	for _, data := range t.sourceUnhealthyAgents {
		totalAgents += len(data.UnhealthyAgents)
	}
	logger.GetLogger().Info("saved agent silent sources to database",
		zap.Int("processedSourceCount", len(t.processedSourceIds)),
		zap.Int("unhealthySourceCount", len(t.sourceUnhealthyAgents)),
		zap.Int("totalAgentEntries", totalAgents))
	return nil
}
