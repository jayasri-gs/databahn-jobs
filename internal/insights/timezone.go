package insights

import (
	"fmt"
	"strings"
	"time"
)

// ValidateIANATimezone returns an error when value is empty or not a valid IANA timezone.
func ValidateIANATimezone(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("timezone is required")
	}
	if !IsValidIANATimezone(value) {
		return fmt.Errorf("invalid IANA time zone %q", value)
	}
	return nil
}

// IsValidIANATimezone reports whether value is a non-empty IANA timezone identifier.
func IsValidIANATimezone(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if !isIANATimeZoneName(value) {
		return false
	}
	_, err := time.LoadLocation(value)
	return err == nil
}

func isIANATimeZoneName(name string) bool {
	if name == "Local" {
		return false
	}
	switch name {
	case "UTC", "GMT", "Zulu":
		return true
	}
	if strings.HasPrefix(name, "Etc/") {
		return strings.Contains(name, "/")
	}
	parts := strings.Split(name, "/")
	// Area/Location and Area/Location/Region (e.g. America/Argentina/Buenos_Aires) are valid.
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
	}
	return true
}
