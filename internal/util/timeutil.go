package util

import (
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
