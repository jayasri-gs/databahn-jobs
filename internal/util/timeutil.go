package util

import (
	"fmt"
	"strings"
	"time"
)

func FindWindow(time time.Time, duration time.Duration) (int64, int64) {
	return FindWindowForMillis(time.UnixMilli(), duration)
}

func FindWindowForMillis(timeMillis int64, duration time.Duration) (int64, int64) {
	durationMillis := duration.Milliseconds()
	d := timeMillis / durationMillis
	start := d * durationMillis
	end := start + durationMillis
	return start, end
}

func GetDayEndTimestamp(millis int64) int64 {
	t := time.UnixMilli(millis).In(time.UTC)
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, t.Location()).UnixMilli()
}

func HumanReadableTimeWithZone(t time.Time) string {
	return t.Format(time.RFC3339)
}

func HumanReadableDuration(d time.Duration) string {
	d = d.Round(time.Minute)

	weeks := d / (7 * 24 * time.Hour)
	d -= weeks * 7 * 24 * time.Hour

	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour

	hours := d / time.Hour
	d -= hours * time.Hour

	minutes := d / time.Minute
	d -= minutes * time.Minute

	var parts []string
	if weeks > 0 {
		parts = append(parts, fmt.Sprintf("%d week%s", weeks, plural(weeks)))
	}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour%s", hours, plural(hours)))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minute%s", minutes, plural(minutes)))
	}

	return strings.Join(parts, " ")
}

func plural(v time.Duration) string {
	if v == 1 {
		return ""
	}
	return "s"
}

func FormatDuration(d time.Duration) string {
	days := d / (24 * time.Hour)
	d -= days * (24 * time.Hour)
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second

	result := ""
	if days > 0 {
		result += fmt.Sprintf("%dd", days)
	}
	if hours > 0 {
		result += fmt.Sprintf("%dh", hours)
	}
	if minutes > 0 {
		result += fmt.Sprintf("%dm", minutes)
	}
	if seconds > 0 {
		result += fmt.Sprintf("%ds", seconds)
	}

	if result == "" {
		return "0s"
	}
	return result
}
