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
	if !strings.Contains(name, "/") {
		return false
	}
	parts := strings.Split(name, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}
