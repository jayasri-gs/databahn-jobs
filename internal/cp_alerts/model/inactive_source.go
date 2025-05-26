package model

import (
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"strings"
	"time"
)

type InActiveSource struct {
	Source        *source.Source
	LastEventTime time.Time
	AlertDuration time.Duration
	CheckedAt     time.Time
}

func (ias InActiveSource) GetEntityId() string {
	return ias.Source.ID.String()
}
func (ias InActiveSource) GetEntityName() string {
	return ias.Source.Name
}
func (ias InActiveSource) GetDataPlaneId() string {
	return ias.Source.DataPlaneId.String()
}
func (ias InActiveSource) GetTenantId() string {
	return ias.Source.TenantID.String()
}

func (ias InActiveSource) InactivityDurationStr() string {
	d := ias.AlertDuration
	d = d.Round(time.Second)

	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour

	hours := d / time.Hour
	d -= hours * time.Hour

	minutes := d / time.Minute
	d -= minutes * time.Minute

	seconds := d / time.Second

	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour%s", hours, plural(hours)))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minute%s", minutes, plural(minutes)))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d second%s", seconds, plural(seconds)))
	}

	return strings.Join(parts, " ")
}

func plural(v time.Duration) string {
	if v == 1 {
		return ""
	}
	return "s"
}

func NewInActiveSource(source *source.Source, lastEventTime time.Time, alertDuration time.Duration) *InActiveSource {
	return &InActiveSource{
		Source:        source,
		LastEventTime: lastEventTime,
		AlertDuration: alertDuration,
		CheckedAt:     time.Now().UTC(),
	}
}
