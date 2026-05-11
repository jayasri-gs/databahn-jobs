package jobs

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
)

// groupedAlertMaxAgentsInMessage is how many agents to list by name in grouped inactivity alerts.
const groupedAlertMaxAgentsInMessage = 2

type GroupedAgents struct {
	DisplayEntries []AgentDisplayEntry // first N agents with last event time (see groupedAlertMaxAgentsInMessage)
	TotalCount     int                 // total count of inactive agents
}

type AgentDisplayEntry struct {
	AgentName     string
	LastEventTime time.Time
}

// FormatGroupedAgentsList formats agents for display (first groupedAlertMaxAgentsInMessage with last event time + count of remaining)
func FormatGroupedAgentsList(agents []model.InactiveAgentInfo) GroupedAgents {
	maxDisplay := groupedAlertMaxAgentsInMessage

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

// formatGroupedAlertLastSeen formats last-event time for grouped inactivity alert text only
func formatGroupedAlertLastSeen(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.In(time.UTC).Format("02 Jan 2006, 15:04 UTC")
}

// FormatGroupedAgentsMessage creates alert message with agent names and their last event times.
// If deepLink is empty, still includes agent list but no link.
func FormatGroupedAgentsMessage(grouped GroupedAgents, deepLink string) string {
	var agentDetails []string
	for _, entry := range grouped.DisplayEntries {
		agentDetails = append(agentDetails, fmt.Sprintf("%s (last seen: %s)",
			entry.AgentName, formatGroupedAlertLastSeen(entry.LastEventTime)))
	}

	agentList := strings.Join(agentDetails, ", ")
	if grouped.TotalCount > groupedAlertMaxAgentsInMessage {
		agentList += fmt.Sprintf(", and %d more", grouped.TotalCount-groupedAlertMaxAgentsInMessage)
	}

	message := fmt.Sprintf(
		"The following agents are not sending data: %s.",
		agentList)

	if deepLink != "" {
		message += fmt.Sprintf(
			" Open this URL in a browser to view all affected agents: %s",
			deepLink)
	}

	return message
}
