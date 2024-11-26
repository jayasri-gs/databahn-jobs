package util

import (
	"fmt"
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

func FormatDuration(d time.Duration) string {
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second

	result := ""
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
