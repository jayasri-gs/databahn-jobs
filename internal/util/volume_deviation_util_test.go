package util

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestCalculateVolumeDeviationDateRange(t *testing.T) {
	// We can't easily mock time.Now(), so we'll test the logic by checking relative relationships
	dateRange := CalculateVolumeDeviationDateRange()

	// Verify that dateRange is not nil
	if dateRange == nil {
		t.Fatal("CalculateVolumeDeviationDateRange() returned nil")
	}

	// Verify that CurrentDayEnd is after CurrentDayStart
	if !dateRange.CurrentDayEnd.After(dateRange.CurrentDayStart) {
		t.Error("CurrentDayEnd should be after CurrentDayStart")
	}

	// Verify that YesterdayEnd is before CurrentDayStart
	if !dateRange.YesterdayEnd.Before(dateRange.CurrentDayStart) {
		t.Error("YesterdayEnd should be before CurrentDayStart")
	}

	// Verify that PreviousWeekSameDayStart is approximately 7 days before CurrentDayStart
	expectedDiff := dateRange.CurrentDayStart.Sub(dateRange.PreviousWeekSameDayStart)
	expectedDays := 7 * 24 * time.Hour
	tolerance := 1 * time.Hour // Allow 1 hour tolerance

	if math.Abs(float64(expectedDiff-expectedDays)) > float64(tolerance) {
		t.Errorf("PreviousWeekSameDayStart should be ~7 days before CurrentDayStart, got %v", expectedDiff)
	}

	// Verify that CurrentDayStart is at midnight (00:00:00)
	if dateRange.CurrentDayStart.Hour() != 0 || dateRange.CurrentDayStart.Minute() != 0 || dateRange.CurrentDayStart.Second() != 0 {
		t.Error("CurrentDayStart should be at midnight (00:00:00)")
	}

	// Verify that CurrentDayEnd is at end of day (23:59:59.999999999)
	if dateRange.CurrentDayEnd.Hour() != 23 || dateRange.CurrentDayEnd.Minute() != 59 || dateRange.CurrentDayEnd.Second() != 59 {
		t.Error("CurrentDayEnd should be at end of day (23:59:59)")
	}
}

func TestCalculateVolumeDeviationPercentage(t *testing.T) {
	tests := []struct {
		name        string
		oldVolume   float64
		newVolume   float64
		expected    float64
		description string
	}{
		// Scenario 4.1.1: Current > Yesterday, deviation = 60% (above threshold)
		{
			name:        "60% increase",
			oldVolume:   100.0,
			newVolume:   160.0,
			expected:    60.0,
			description: "60% increase should return 60.0",
		},
		// Scenario 4.1.2: Current > Yesterday, deviation = 30% (below threshold)
		{
			name:        "30% increase",
			oldVolume:   100.0,
			newVolume:   130.0,
			expected:    30.0,
			description: "30% increase should return 30.0",
		},
		// Scenario 4.1.3: Current > Yesterday, deviation = exactly threshold (50%)
		{
			name:        "exactly 50% increase",
			oldVolume:   100.0,
			newVolume:   150.0,
			expected:    50.0,
			description: "Exactly 50% increase should return 50.0",
		},
		// Scenario 4.1.4: Current = Yesterday (no change)
		{
			name:        "no change",
			oldVolume:   100.0,
			newVolume:   100.0,
			expected:    0.0,
			description: "No change should return 0.0",
		},
		// Scenario 4.1.5: Current >> Yesterday (very large spike, e.g., 500%)
		{
			name:        "500% increase",
			oldVolume:   100.0,
			newVolume:   600.0,
			expected:    500.0,
			description: "500% increase should return 500.0",
		},
		// Scenario 4.2.1: Current < Yesterday, deviation = -60% (above threshold)
		{
			name:        "60% decrease",
			oldVolume:   100.0,
			newVolume:   40.0,
			expected:    -60.0,
			description: "60% decrease should return -60.0",
		},
		// Scenario 4.2.2: Current < Yesterday, deviation = -30% (below threshold)
		{
			name:        "30% decrease",
			oldVolume:   100.0,
			newVolume:   70.0,
			expected:    -30.0,
			description: "30% decrease should return -30.0",
		},
		// Scenario 4.2.3: Current < Yesterday, deviation = exactly -threshold (-50%)
		{
			name:        "exactly 50% decrease",
			oldVolume:   100.0,
			newVolume:   50.0,
			expected:    -50.0,
			description: "Exactly 50% decrease should return -50.0",
		},
		// Scenario 4.2.4: Current << Yesterday (very large drop, e.g., -90%)
		{
			name:        "90% decrease",
			oldVolume:   100.0,
			newVolume:   10.0,
			expected:    -90.0,
			description: "90% decrease should return -90.0",
		},
		// Scenario 7.1.1: CurrentDayVolume = 0, YesterdayVolume = 100MB
		{
			name:        "current volume zero, previous 100MB",
			oldVolume:   100.0,
			newVolume:   0.0,
			expected:    -100.0,
			description: "100% drop from 100MB to 0 should return -100.0",
		},
		// Scenario 7.1.2: CurrentDayVolume = 0, YesterdayVolume = 0
		{
			name:        "both volumes zero",
			oldVolume:   0.0,
			newVolume:   0.0,
			expected:    0.0,
			description: "Both volumes zero should return 0.0",
		},
		// Scenario 7.2.1: CurrentDayVolume = 100MB, YesterdayVolume = 0
		{
			name:        "previous volume zero, current 100MB",
			oldVolume:   0.0,
			newVolume:   100.0,
			expected:    0.0,
			description: "No meaningful percentage comparison when baseline is zero, returns 0.0",
		},
		// Edge case: small volumes
		{
			name:        "small volumes",
			oldVolume:   1.0,
			newVolume:   2.0,
			expected:    100.0,
			description: "Doubling small volume should return 100.0",
		},
		// Edge case: very large volumes
		{
			name:        "very large volumes",
			oldVolume:   1000000000.0, // 1GB
			newVolume:   1500000000.0, // 1.5GB
			expected:    50.0,
			description: "50% increase in large volumes should return 50.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateVolumeDeviationPercentage(tt.oldVolume, tt.newVolume)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("%s: CalculateVolumeDeviationPercentage(%v, %v) = %v, want %v",
					tt.description, tt.oldVolume, tt.newVolume, result, tt.expected)
			}
		})
	}
}

func TestCheckSpikeOrDrop(t *testing.T) {
	tests := []struct {
		name           string
		currentVolume  float64
		previousVolume float64
		spikeThreshold float64
		dropThreshold  float64
		expectedSpike  bool
		expectedDrop   bool
		expectedDevMin float64
		expectedDevMax float64
		description    string
	}{
		// Scenario 4.1.1: Current > Yesterday, deviation = 60% (above threshold)
		{
			name:           "spike above threshold",
			currentVolume:  160.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  true,
			expectedDrop:   false,
			expectedDevMin: 60.0,
			expectedDevMax: 60.0,
			description:    "60% increase above 50% threshold should trigger spike",
		},
		// Scenario 4.1.2: Current > Yesterday, deviation = 30% (below threshold)
		{
			name:           "spike below threshold",
			currentVolume:  130.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   false,
			expectedDevMin: 30.0,
			expectedDevMax: 30.0,
			description:    "30% increase below 50% threshold should not trigger spike",
		},
		// Scenario 4.1.3: Current > Yesterday, deviation = exactly threshold (50%)
		{
			name:           "spike exactly at threshold",
			currentVolume:  150.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false, // Exclusive threshold
			expectedDrop:   false,
			expectedDevMin: 50.0,
			expectedDevMax: 50.0,
			description:    "Exactly 50% increase at threshold should not trigger spike (exclusive)",
		},
		// Scenario 4.1.4: Current = Yesterday (no change)
		{
			name:           "no change",
			currentVolume:  100.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   false,
			expectedDevMin: 0.0,
			expectedDevMax: 0.0,
			description:    "No change should not trigger spike or drop",
		},
		// Scenario 4.1.5: Current >> Yesterday (very large spike, e.g., 500%)
		{
			name:           "very large spike",
			currentVolume:  600.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  true,
			expectedDrop:   false,
			expectedDevMin: 500.0,
			expectedDevMax: 500.0,
			description:    "500% increase should trigger spike",
		},
		// Scenario 4.2.1: Current < Yesterday, deviation = -60% (above threshold)
		{
			name:           "drop above threshold",
			currentVolume:  40.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   true,
			expectedDevMin: -60.0,
			expectedDevMax: -60.0,
			description:    "60% decrease above 50% threshold should trigger drop",
		},
		// Scenario 4.2.2: Current < Yesterday, deviation = -30% (below threshold)
		{
			name:           "drop below threshold",
			currentVolume:  70.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   false,
			expectedDevMin: -30.0,
			expectedDevMax: -30.0,
			description:    "30% decrease below 50% threshold should not trigger drop",
		},
		// Scenario 4.2.3: Current < Yesterday, deviation = exactly -threshold (-50%)
		{
			name:           "drop exactly at threshold",
			currentVolume:  50.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   false, // Exclusive threshold
			expectedDevMin: -50.0,
			expectedDevMax: -50.0,
			description:    "Exactly 50% decrease at threshold should not trigger drop (exclusive)",
		},
		// Scenario 4.2.4: Current << Yesterday (very large drop, e.g., -90%)
		{
			name:           "very large drop",
			currentVolume:  10.0,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false,
			expectedDrop:   true,
			expectedDevMin: -90.0,
			expectedDevMax: -90.0,
			description:    "90% decrease should trigger drop",
		},
		// Scenario 2.1.1: PercentageIncreaseThreshold = 0%
		{
			name:           "zero spike threshold",
			currentVolume:  100.1,
			previousVolume: 100.0,
			spikeThreshold: 0.0,
			dropThreshold:  50.0,
			expectedSpike:  true,
			expectedDrop:   false,
			expectedDevMin: 0.1,
			expectedDevMax: 0.1,
			description:    "Any increase with 0% threshold should trigger spike",
		},
		// Scenario 2.1.4: PercentageDecreaseThreshold = 0%
		{
			name:           "zero drop threshold",
			currentVolume:  99.9,
			previousVolume: 100.0,
			spikeThreshold: 50.0,
			dropThreshold:  0.0,
			expectedSpike:  false,
			expectedDrop:   true,
			expectedDevMin: -0.1,
			expectedDevMax: -0.1,
			description:    "Any decrease with 0% threshold should trigger drop",
		},
		// Scenario 2.1.7: Different thresholds for increase vs decrease
		{
			name:           "different thresholds",
			currentVolume:  140.0,
			previousVolume: 100.0,
			spikeThreshold: 30.0,
			dropThreshold:  70.0,
			expectedSpike:  true,
			expectedDrop:   false,
			expectedDevMin: 40.0,
			expectedDevMax: 40.0,
			description:    "40% increase should trigger spike with 30% threshold",
		},
		// Scenario 7.2.1: CurrentDayVolume = 100MB, YesterdayVolume = 0
		{
			name:           "previous volume zero",
			currentVolume:  100.0,
			previousVolume: 0.0,
			spikeThreshold: 50.0,
			dropThreshold:  50.0,
			expectedSpike:  false, // No meaningful percentage comparison when baseline is zero
			expectedDrop:   false,
			expectedDevMin: 0.0,
			expectedDevMax: 0.0,
			description:    "No spike detected when baseline is zero (handled separately in alert logic)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isSpike, isDrop, deviation := CheckSpikeOrDrop(
				tt.currentVolume,
				tt.previousVolume,
				tt.spikeThreshold,
				tt.dropThreshold,
			)

			if isSpike != tt.expectedSpike {
				t.Errorf("%s: isSpike = %v, want %v", tt.description, isSpike, tt.expectedSpike)
			}
			if isDrop != tt.expectedDrop {
				t.Errorf("%s: isDrop = %v, want %v", tt.description, isDrop, tt.expectedDrop)
			}
			// Use tolerance for floating point comparison
			tolerance := 0.0001
			if math.Abs(deviation-tt.expectedDevMin) > tolerance && math.Abs(deviation-tt.expectedDevMax) > tolerance {
				// Check if deviation is within the expected range
				if tt.expectedDevMin != tt.expectedDevMax {
					// Range check
					if deviation < tt.expectedDevMin-tolerance || deviation > tt.expectedDevMax+tolerance {
						t.Errorf("%s: deviation = %v, want between %v and %v (tolerance: %v)",
							tt.description, deviation, tt.expectedDevMin, tt.expectedDevMax, tolerance)
					}
				} else {
					// Exact value check with tolerance
					if math.Abs(deviation-tt.expectedDevMin) > tolerance {
						t.Errorf("%s: deviation = %v, want %v (tolerance: %v)",
							tt.description, deviation, tt.expectedDevMin, tolerance)
					}
				}
			}
		})
	}
}

func TestFormatVolumeDeviationMessage(t *testing.T) {
	tests := []struct {
		name             string
		entityType       string
		entityName       string
		entityId         string
		isSpike          bool
		deviationPercent float64
		currentVolume    float64
		previousVolume   float64
		currentDate      time.Time
		previousDate     time.Time
		expectedContains []string
		description      string
	}{
		{
			name:             "spike message",
			entityType:       "Agent",
			entityName:       "test-agent",
			entityId:         "123",
			isSpike:          true,
			deviationPercent: 60.0,
			currentVolume:    160.0,
			previousVolume:   100.0,
			currentDate:      time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			previousDate:     time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC),
			expectedContains: []string{"Agent", "test-agent", "123", "60.0", "increase", "2024-01-15", "2024-01-14"},
			description:      "Spike message should contain all relevant information",
		},
		{
			name:             "drop message",
			entityType:       "Agent",
			entityName:       "test-agent",
			entityId:         "123",
			isSpike:          false,
			deviationPercent: 60.0,
			currentVolume:    40.0,
			previousVolume:   100.0,
			currentDate:      time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			previousDate:     time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC),
			expectedContains: []string{"Agent", "test-agent", "123", "60.0", "decrease", "2024-01-15", "2024-01-14"},
			description:      "Drop message should contain all relevant information",
		},
		// Scenario 12.2.1: Very large deviation percentages (>1000%)
		{
			name:             "very large deviation",
			entityType:       "Agent",
			entityName:       "test-agent",
			entityId:         "123",
			isSpike:          true,
			deviationPercent: 1500.0,
			currentVolume:    16000.0,
			previousVolume:   1000.0,
			currentDate:      time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			previousDate:     time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC),
			expectedContains: []string{"1500.0", "increase"},
			description:      "Very large deviation should format correctly",
		},
		// Scenario 12.2.2: Very small deviation percentages (<1%)
		{
			name:             "very small deviation",
			entityType:       "Agent",
			entityName:       "test-agent",
			entityId:         "123",
			isSpike:          true,
			deviationPercent: 0.5,
			currentVolume:    100.5,
			previousVolume:   100.0,
			currentDate:      time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			previousDate:     time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC),
			expectedContains: []string{"0.5", "increase"},
			description:      "Very small deviation should format correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := FormatVolumeDeviationMessage(
				tt.entityType,
				tt.entityName,
				tt.entityId,
				tt.isSpike,
				tt.deviationPercent,
				tt.currentVolume,
				tt.previousVolume,
				tt.currentDate,
				tt.previousDate,
			)

			for _, expected := range tt.expectedContains {
				if !strings.Contains(message, expected) {
					t.Errorf("%s: message should contain '%s', got: %s", tt.description, expected, message)
				}
			}
		})
	}
}
