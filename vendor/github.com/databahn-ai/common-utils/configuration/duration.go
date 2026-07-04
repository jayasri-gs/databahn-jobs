package configuration

import "time"

// DurationFromReaderIfPresent returns (d, true) when key is non-empty and parses as a Go duration.
// If r is nil, the value is empty, or parsing fails, it returns (0, false).
func DurationFromReaderIfPresent(r ConfigReader, key string) (time.Duration, bool) {
	if r == nil {
		return 0, false
	}
	s := r.GetString(key)
	if s == "" {
		return 0, false
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false
	}
	return d, true
}

// DurationFromReader parses key as a time.ParseDuration value. If r is nil, the value
// is empty, or parsing fails, it returns fallback.
func DurationFromReader(r ConfigReader, key string, fallback time.Duration) time.Duration {
	if d, ok := DurationFromReaderIfPresent(r, key); ok {
		return d
	}
	return fallback
}
