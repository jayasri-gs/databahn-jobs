package jobs

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/util"
)

type GroupedAgents struct {
	DisplayEntries []AgentDisplayEntry // first 5 agents with last event time
	TotalCount     int                 // total count of inactive agents
}

type AgentDisplayEntry struct {
	AgentName     string
	LastEventTime time.Time
}

// FormatGroupedAgentsList formats agents for display (first 5 with last event time + count of remaining)
func FormatGroupedAgentsList(agents []model.InactiveAgentInfo) GroupedAgents {
	const maxDisplay = 5

	displayEntries := make([]AgentDisplayEntry, 0, maxDisplay)

	for i, agent := range agents {
		if i < maxDisplay {
			displayEntries = append(displayEntries, AgentDisplayEntry{
				AgentName:     agent.AgentName,
				LastEventTime: agent.LastEventTime,
			})
		}
	}

	return GroupedAgents{
		DisplayEntries: displayEntries,
		TotalCount:     len(agents),
	}
}

// BuildAgentListDeepLink creates URL to agent list filtered by source name.
// Returns empty string if app URL not configured.
func BuildAgentListDeepLink(sourceName string) string {
	baseURL := strings.TrimSpace(config.GetAppConfiguration().GetString(configuration.DataBahnAppUrl))
	if baseURL == "" {
		return ""
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + strings.TrimPrefix(baseURL, "/")
	}

	queryParam := url.QueryEscape(sourceName)
	return baseURL + "/agent?silentSourceName=" + queryParam
}

// FormatGroupedAgentsMessage creates alert message with agent names and their last event times.
// If deepLink is empty, still includes agent list but no link.
func FormatGroupedAgentsMessage(grouped GroupedAgents, deepLink string) string {
	var agentDetails []string
	for _, entry := range grouped.DisplayEntries {
		agentDetails = append(agentDetails, fmt.Sprintf("%s (last seen: %s)",
			entry.AgentName, util.HumanReadableTimeWithZone(entry.LastEventTime)))
	}

	agentList := strings.Join(agentDetails, ", ")
	if grouped.TotalCount > 5 {
		agentList += fmt.Sprintf(", and %d more", grouped.TotalCount-5)
	}

	message := fmt.Sprintf(
		"The following agents are not sending data: %s.",
		agentList)

	if deepLink != "" {
		message += fmt.Sprintf(
			" Click to view and manage all affected agents: %s",
			deepLink)
	}

	return message
}
