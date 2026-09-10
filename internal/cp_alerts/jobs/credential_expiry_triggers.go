package jobs

import (
	"encoding/json"
	"time"

	"gorm.io/datatypes"
)

// Trigger status values shared with backend-service NotificationTriggersSupport.
const (
	CredExpiryStatusPending = "PENDING"
	CredExpiryStatusSent    = "SENT"
	CredExpiryStatusSkipped = "SKIPPED"
	CredExpiryStatusFailed  = "FAILED"

	CredExpiryKindWarning = "WARNING"
	CredExpiryKindExpired = "EXPIRED"

	// CredExpiryMaxAttempts caps retries for a single trigger before it is skipped.
	CredExpiryMaxAttempts = 5

	credExpiryDateLayout = "2006-01-02"
)

// CredExpiryTrigger is one row of the triggers[] array on secrets.notification_triggers.
// The jsonb shape is authored by backend-service (Java) and mutated by this job.
type CredExpiryTrigger struct {
	Days          int                                `json:"days"`
	Kind          string                             `json:"kind"`
	Status        string                             `json:"status"`
	ScheduledFor  string                             `json:"scheduled_for"`
	SentAt        *string                            `json:"sent_at"`
	Attempts      int                                `json:"attempts"`
	SkippedReason string                             `json:"skipped_reason,omitempty"`
	Channels      map[string]CredExpiryChannelStatus `json:"channels"`
}

// CredExpiryChannelStatus tracks per-channel dispatch state (email, ui, ...).
type CredExpiryChannelStatus struct {
	Status string  `json:"status"`
	At     *string `json:"at,omitempty"`
	Error  string  `json:"error,omitempty"`
}

// CredExpirySeries is the top-level jsonb container.
type CredExpirySeries struct {
	SeriesId          string              `json:"series_id"`
	ExpiryDate        string              `json:"expiry_date"`
	SeededAt          string              `json:"seeded_at"`
	NextDueAt         *string             `json:"next_due_at"`
	Triggers          []CredExpiryTrigger `json:"triggers"`
	InvalidatedSeries []json.RawMessage   `json:"invalidated_series,omitempty"`
}

// DecodeSeries parses the jsonb column. Returns nil if the column is empty.
func DecodeSeries(raw datatypes.JSON) (*CredExpirySeries, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var s CredExpirySeries
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// EncodeSeries serialises the container back into a jsonb value.
func EncodeSeries(s *CredExpirySeries) (datatypes.JSON, error) {
	if s == nil {
		return nil, nil
	}
	return json.Marshal(s)
}

// DueTriggers returns triggers that are ready to fire (PENDING or FAILED with attempts left,
// scheduled_for <= today UTC). Order matches the seeded order (largest to smallest days).
func DueTriggers(series *CredExpirySeries, today time.Time) []int {
	if series == nil {
		return nil
	}
	todayStr := today.UTC().Format(credExpiryDateLayout)
	var due []int
	for i, t := range series.Triggers {
		if t.Status == CredExpiryStatusSent || t.Status == CredExpiryStatusSkipped {
			continue
		}
		if t.Status == CredExpiryStatusFailed && t.Attempts >= CredExpiryMaxAttempts {
			continue
		}
		if t.ScheduledFor == "" {
			continue
		}
		// String comparison works for YYYY-MM-DD.
		if t.ScheduledFor <= todayStr {
			due = append(due, i)
		}
	}
	return due
}

// MarkTriggerSent stamps a trigger as SENT, records the send timestamp, and increments attempts.
func MarkTriggerSent(series *CredExpirySeries, index int, at time.Time) {
	if series == nil || index < 0 || index >= len(series.Triggers) {
		return
	}
	stamp := at.UTC().Format(time.RFC3339)
	series.Triggers[index].Status = CredExpiryStatusSent
	series.Triggers[index].SentAt = &stamp
	series.Triggers[index].Attempts = series.Triggers[index].Attempts + 1
	RecomputeNextDueAt(series)
}

// MarkTriggerFailed increments attempts and either keeps the trigger retryable
// (status FAILED so a future run picks it up) or gives up permanently by marking
// it SKIPPED after MAX_ATTEMPTS.
func MarkTriggerFailed(series *CredExpirySeries, index int, err error) {
	if series == nil || index < 0 || index >= len(series.Triggers) {
		return
	}
	series.Triggers[index].Attempts = series.Triggers[index].Attempts + 1
	if series.Triggers[index].Attempts >= CredExpiryMaxAttempts {
		series.Triggers[index].Status = CredExpiryStatusSkipped
		series.Triggers[index].SkippedReason = "MAX_ATTEMPTS"
	} else {
		series.Triggers[index].Status = CredExpiryStatusFailed
	}
	RecomputeNextDueAt(series)
}

// RecomputeNextDueAt refreshes the top-level next_due_at from remaining PENDING/FAILED triggers.
// When nothing remains, sets it to nil so the row drops out of the job's indexed query.
func RecomputeNextDueAt(series *CredExpirySeries) {
	if series == nil {
		return
	}
	var next *string
	for _, t := range series.Triggers {
		if t.Status != CredExpiryStatusPending && t.Status != CredExpiryStatusFailed {
			continue
		}
		if t.Status == CredExpiryStatusFailed && t.Attempts >= CredExpiryMaxAttempts {
			continue
		}
		if t.ScheduledFor == "" {
			continue
		}
		if next == nil || t.ScheduledFor < *next {
			s := t.ScheduledFor
			next = &s
		}
	}
	series.NextDueAt = next
}

// RecordChannel writes the per-channel status entry for a trigger.
func RecordChannel(series *CredExpirySeries, index int, channelName, status string, at time.Time, sendErr error) {
	if series == nil || index < 0 || index >= len(series.Triggers) {
		return
	}
	if series.Triggers[index].Channels == nil {
		series.Triggers[index].Channels = map[string]CredExpiryChannelStatus{}
	}
	stamp := at.UTC().Format(time.RFC3339)
	entry := CredExpiryChannelStatus{Status: status, At: &stamp}
	if sendErr != nil {
		entry.Error = sendErr.Error()
	}
	series.Triggers[index].Channels[channelName] = entry
}
