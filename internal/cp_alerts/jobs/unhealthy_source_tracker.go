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
// Clears ALL existing entries for the tenant before inserting new ones (full refresh).
// This ensures deleted/disabled sources don't leave orphaned rows.
func (t *AgentUnhealthySourceTracker) SaveToDatabase(db *gorm.DB, tenantId string) error {
	// Always clear tenant's entries first (even if no sources processed, to clean up deleted/disabled sources)
	if err := db.
		Table("agent_silent_sources").
		Where("tenant_id = ?", tenantId).
		Delete(nil).
		Error; err != nil {
		logger.GetLogger().Error("error deleting old silent sources for tenant", zap.Error(err), zap.String("tenantId", tenantId))
		return err
	}

	if len(t.sourceUnhealthyAgents) == 0 {
		logger.GetLogger().Info("no unhealthy agent-scoped sources, cleared stale entries only", zap.String("tenantId", tenantId))
		return nil
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
