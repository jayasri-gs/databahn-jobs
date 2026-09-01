package query

import "strings"

// SentinelQueryPlan is one Log Analytics request: the KQL to run, plus a label for logs.
type SentinelQueryPlan struct {
	KQL   string
	Label string
}

// PlanSentinelQueries returns the sequence of Log Analytics requests that together satisfy
// one export.
//
// Today it always returns a single request running the planned KQL verbatim. backend-service
// caps the injected `take` (MAX_SENTINEL_EXPORT_TAKE_LIMIT) low enough that one response
// stays inside the service's 500k-row / ~64 MB / 10-minute limits, and a query that exceeds
// them anyway is reported to the user — see sentinelResultError — rather than worked around.
//
// This function is the seam for chunking. Splitting a long range into windows means
// replacing it with an adaptive planner (count each window, bisect while it exceeds a page
// cap) and nothing else in the Sentinel path changes: the transport, decoder, encoder, and
// uploader already run once per plan and stream rows through a single callback.
//
// A chunked planner will need one thing this one does not: the KQL *without* its trailing
// `sort` and `take`, plus the dataset's table and time column, so it can insert a per-window
// `| where <t> >= .. and <t> < ..` after the table reference. Appending that filter to a
// pipeline that already ends in `| sort by TimeGenerated desc | take N` would filter after
// the truncation and hand every window the same global top-N. SearchExportConfig carries
// KqlTable and KqlTimeColumn for exactly that future, unread today.
func PlanSentinelQueries(kql string) []SentinelQueryPlan {
	return []SentinelQueryPlan{{KQL: strings.TrimSpace(kql), Label: "full-range"}}
}
