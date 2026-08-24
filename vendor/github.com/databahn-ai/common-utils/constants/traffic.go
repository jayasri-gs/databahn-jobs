package constants

import "strings"

const (
	TrafficTypeLive   = "live"
	TrafficTypeReplay = "replay"
	ReplayTypeNone    = "none"
)

// ResolveTrafficType returns live when the header is absent so legacy traffic
// continues to aggregate under the live bucket.
func ResolveTrafficType(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "", TrafficTypeLive:
		return TrafficTypeLive
	case TrafficTypeReplay:
		return TrafficTypeReplay
	default:
		return TrafficTypeLive
	}
}

// ResolveReplayType normalizes replay-type headers to a bounded allowlist.
func ResolveReplayType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", ReplayTypeNone:
		return ReplayTypeNone
	case "unparsed", "undelivered", "custom":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ReplayTypeNone
	}
}

// ResolveReplayTypeForTraffic ignores replay-type headers on non-replay traffic
// so stale values cannot split live aggregation buckets.
func ResolveReplayTypeForTraffic(trafficType, replayType string) string {
	if ResolveTrafficType(trafficType) != TrafficTypeReplay {
		return ReplayTypeNone
	}
	return ResolveReplayType(replayType)
}

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
