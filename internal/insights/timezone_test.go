package insights

import "testing"

func TestValidateTimezone(t *testing.T) {
	valid := []string{"UTC", "America/New_York", "Europe/London", "Asia/Kolkata", "US/Eastern"}
	for _, tz := range valid {
		if err := ValidateIANATimezone(tz); err != nil {
			t.Fatalf("ValidateIANATimezone(%q) error = %v", tz, err)
		}
	}

	invalid := []string{"", "EST", "Not/AZone", "+05:30"}
	for _, tz := range invalid {
		if err := ValidateIANATimezone(tz); err == nil {
			t.Fatalf("ValidateIANATimezone(%q) expected error", tz)
		}
	}
}

func TestIsValidIANATimezone(t *testing.T) {
	valid := []string{"UTC", "America/New_York", "Europe/London", "Asia/Tokyo", "Etc/UTC"}
	for _, tz := range valid {
		if !IsValidIANATimezone(tz) {
			t.Fatalf("IsValidIANATimezone(%q) = false, want true", tz)
		}
	}

	invalid := []string{"", "EST", "Not/A/Zone", "Local", "+05:30"}
	for _, tz := range invalid {
		if IsValidIANATimezone(tz) {
			t.Fatalf("IsValidIANATimezone(%q) = true, want false", tz)
		}
	}
}

func TestPopulateAgentDetectedTimezoneSkipsInvalidKey4(t *testing.T) {
	doc := DeviceDocument{}
	populateAgentDetectedTimezone(&doc, "EST")
	if doc.Key4 != "" {
		t.Fatalf("Key4 = %q, want empty for invalid timezone", doc.Key4)
	}
	if doc.TimezoneUpdateReason != "" {
		t.Fatalf("timezone_update_reason = %q, want empty", doc.TimezoneUpdateReason)
	}
}
