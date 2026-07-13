package constants

import "strings"

const (
	TrafficTypeHeader = "db_traffic_type"
	ReplayTypeHeader  = "db_replay_type"
	ReplayJobIdHeader = "db_replay_job_id"

	TrafficTypeLive   = "live"
	TrafficTypeReplay = "replay"
	ReplayTypeNone    = "none"
)

// ReplayTypeFromJobType maps databahn-jobs replay job types to tag values.
func ReplayTypeFromJobType(jobType string) string {
	switch strings.ToUpper(strings.TrimSpace(jobType)) {
	case "UNPARSED":
		return "unparsed"
	case "UNDELIVERED":
		return "undelivered"
	case "CUSTOM":
		return "custom"
	default:
		return ReplayTypeNone
	}
}
