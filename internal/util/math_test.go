package util

import (
	"math"
	"testing"
)

// TestCalculateMean tests the mean calculation function
func TestCalculateMean(t *testing.T) {
	tests := []struct {
		name     string
		numbers  []float64
		expected float64
	}{
		{
			name:     "simple average",
			numbers:  []float64{1, 2, 3, 4, 5},
			expected: 3.0,
		},
		{
			name:     "all same values",
			numbers:  []float64{5, 5, 5, 5, 5},
			expected: 5.0,
		},
		{
			name:     "with decimals",
			numbers:  []float64{1.5, 2.5, 3.5, 4.5},
			expected: 3.0,
		},
		{
			name:     "negative values",
			numbers:  []float64{-10, -5, 0, 5, 10},
			expected: 0.0,
		},
		{
			name:     "single value",
			numbers:  []float64{42},
			expected: 42.0,
		},
		{
			name:     "large numbers",
			numbers:  []float64{1000000, 2000000, 3000000},
			expected: 2000000.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateMean(tt.numbers)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("CalculateMean() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestCalculateStandardDeviation tests the standard deviation calculation
func TestCalculateStandardDeviation(t *testing.T) {
	tests := []struct {
		name     string
		numbers  []float64
		mean     float64
		expected float64
	}{
		{
			name:     "simple dataset",
			numbers:  []float64{2, 4, 4, 4, 5, 5, 7, 9},
			mean:     5.0,
			expected: 2.0,
		},
		{
			name:     "no variance",
			numbers:  []float64{5, 5, 5, 5, 5},
			mean:     5.0,
			expected: 0.0,
		},
		{
			name:     "high variance",
			numbers:  []float64{1, 5, 10, 15, 20},
			mean:     10.2,
			expected: 6.7941151, // Population standard deviation
		},
		{
			name:     "negative values",
			numbers:  []float64{-10, -5, 0, 5, 10},
			mean:     0.0,
			expected: 7.0710678,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateStandardDeviation(tt.numbers, tt.mean)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("CalculateStandardDeviation() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestCalculateZScore tests the z-score calculation
func TestCalculateZScore(t *testing.T) {
	tests := []struct {
		name      string
		newNumber float64
		mean      float64
		stdDev    float64
		expected  float64
	}{
		{
			name:      "exactly at mean",
			newNumber: 10.0,
			mean:      10.0,
			stdDev:    2.0,
			expected:  0.0,
		},
		{
			name:      "one standard deviation above",
			newNumber: 12.0,
			mean:      10.0,
			stdDev:    2.0,
			expected:  1.0,
		},
		{
			name:      "one standard deviation below",
			newNumber: 8.0,
			mean:      10.0,
			stdDev:    2.0,
			expected:  -1.0,
		},
		{
			name:      "three standard deviations above",
			newNumber: 16.0,
			mean:      10.0,
			stdDev:    2.0,
			expected:  3.0,
		},
		{
			name:      "five standard deviations above",
			newNumber: 20.0,
			mean:      10.0,
			stdDev:    2.0,
			expected:  5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateZScore(tt.newNumber, tt.mean, tt.stdDev)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("CalculateZScore() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestAnomalyDetectionWithThreshold15 tests anomaly detection with 1.5 threshold (for large samples)
func TestAnomalyDetectionWithThreshold15(t *testing.T) {
	threshold := 1.5

	tests := []struct {
		name           string
		historicalData []float64
		newValue       float64
		shouldAlert    bool
		description    string
	}{
		{
			name:           "normal value within range",
			historicalData: []float64{10, 11, 10, 12, 10, 11, 10, 12, 11, 10, 12, 11, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12},
			newValue:       11.0,
			shouldAlert:    false,
			description:    "Value within 1.5 std dev should not alert",
		},
		{
			name:           "moderate spike",
			historicalData: []float64{10, 11, 10, 12, 10, 11, 10, 12, 11, 10, 12, 11, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12},
			newValue:       13.5,
			shouldAlert:    true,
			description:    "Value above 1.5 std dev should alert",
		},
		{
			name:           "significant spike",
			historicalData: []float64{10, 11, 10, 12, 10, 11, 10, 12, 11, 10, 12, 11, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12},
			newValue:       20.0,
			shouldAlert:    true,
			description:    "Large spike well above threshold should alert",
		},
		{
			name:           "slight decrease",
			historicalData: []float64{10, 11, 10, 12, 10, 11, 10, 12, 11, 10, 12, 11, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12},
			newValue:       9.5,
			shouldAlert:    false,
			description:    "Small decrease should not alert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean := CalculateMean(tt.historicalData)
			stdDev := CalculateStandardDeviation(tt.historicalData, mean)
			zScore := CalculateZScore(tt.newValue, mean, stdDev)

			isAnomaly := zScore > threshold

			t.Logf("Mean: %.2f, StdDev: %.2f, NewValue: %.2f, Z-Score: %.2f, Threshold: %.2f",
				mean, stdDev, tt.newValue, zScore, threshold)

			if isAnomaly != tt.shouldAlert {
				t.Errorf("%s: isAnomaly = %v, want %v (z-score: %.2f, threshold: %.2f)",
					tt.description, isAnomaly, tt.shouldAlert, zScore, threshold)
			}
		})
	}
}

// TestAnomalyDetectionWithThreshold30 tests anomaly detection with 3.0 threshold (for medium samples)
func TestAnomalyDetectionWithThreshold30(t *testing.T) {
	threshold := 3.0

	tests := []struct {
		name           string
		historicalData []float64
		newValue       float64
		shouldAlert    bool
		description    string
	}{
		{
			name:           "normal value",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11, 10, 12, 11, 10, 12, 11, 10},
			newValue:       11.5,
			shouldAlert:    false,
			description:    "Normal value should not alert",
		},
		{
			name:           "moderate spike above threshold",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11, 10, 12, 11, 10, 12, 11, 10},
			newValue:       15.0,
			shouldAlert:    true,
			description:    "Spike above 3.0 std dev should alert (z-score ~5.1)",
		},
		{
			name:           "significant spike above threshold",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11, 10, 12, 11, 10, 12, 11, 10},
			newValue:       25.0,
			shouldAlert:    true,
			description:    "Significant spike above 3.0 std dev should alert",
		},
		{
			name:           "extreme spike",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11, 10, 12, 11, 10, 12, 11, 10},
			newValue:       50.0,
			shouldAlert:    true,
			description:    "Extreme spike should definitely alert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean := CalculateMean(tt.historicalData)
			stdDev := CalculateStandardDeviation(tt.historicalData, mean)
			zScore := CalculateZScore(tt.newValue, mean, stdDev)

			isAnomaly := zScore > threshold

			t.Logf("Mean: %.2f, StdDev: %.2f, NewValue: %.2f, Z-Score: %.2f, Threshold: %.2f",
				mean, stdDev, tt.newValue, zScore, threshold)

			if isAnomaly != tt.shouldAlert {
				t.Errorf("%s: isAnomaly = %v, want %v (z-score: %.2f, threshold: %.2f)",
					tt.description, isAnomaly, tt.shouldAlert, zScore, threshold)
			}
		})
	}
}

// TestAnomalyDetectionWithThreshold50 tests anomaly detection with 5.0 threshold (for small samples)
func TestAnomalyDetectionWithThreshold50(t *testing.T) {
	threshold := 5.0

	tests := []struct {
		name           string
		historicalData []float64
		newValue       float64
		shouldAlert    bool
		description    string
	}{
		{
			name:           "normal value",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11},
			newValue:       11.0,
			shouldAlert:    false,
			description:    "Normal value should not alert",
		},
		{
			name:           "moderate spike above threshold",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11},
			newValue:       20.0,
			shouldAlert:    true,
			description:    "Spike above 5.0 std dev should alert (z-score ~11.9)",
		},
		{
			name:           "large spike",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11},
			newValue:       40.0,
			shouldAlert:    true,
			description:    "Large spike above 5.0 std dev should alert",
		},
		{
			name:           "extreme spike",
			historicalData: []float64{10, 11, 12, 10, 11, 12, 11},
			newValue:       100.0,
			shouldAlert:    true,
			description:    "Extreme spike should definitely alert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean := CalculateMean(tt.historicalData)
			stdDev := CalculateStandardDeviation(tt.historicalData, mean)
			zScore := CalculateZScore(tt.newValue, mean, stdDev)

			isAnomaly := zScore > threshold

			t.Logf("Mean: %.2f, StdDev: %.2f, NewValue: %.2f, Z-Score: %.2f, Threshold: %.2f",
				mean, stdDev, tt.newValue, zScore, threshold)

			if isAnomaly != tt.shouldAlert {
				t.Errorf("%s: isAnomaly = %v, want %v (z-score: %.2f, threshold: %.2f)",
					tt.description, isAnomaly, tt.shouldAlert, zScore, threshold)
			}
		})
	}
}

// TestBehavioralSpikeDetectionScenarios tests realistic behavioral spike detection scenarios
func TestBehavioralSpikeDetectionScenarios(t *testing.T) {
	tests := []struct {
		name           string
		historicalData []float64
		newValue       float64
		threshold      float64
		shouldAlert    bool
		description    string
	}{
		{
			name: "stable ingestion with sudden spike",
			historicalData: []float64{
				10_000_000, 11_000_000, 10_500_000, 10_800_000,
				10_200_000, 10_900_000, 10_600_000, 10_400_000,
				10_700_000, 10_300_000, 10_500_000, 10_800_000,
				10_600_000,
			},
			newValue:    50_000_000, // 5x spike!
			threshold:   1.5,
			shouldAlert: true,
			description: "Stable volume with sudden 5x spike should alert",
		},
		{
			name: "gradual increase - moderate alert",
			historicalData: []float64{
				20_000_000, 21_000_000, 20_500_000, 22_000_000,
				21_500_000, 22_500_000, 21_800_000, 22_200_000,
				21_900_000, 22_800_000, 22_500_000, 23_000_000,
				22_700_000,
			},
			newValue:    24_000_000, // Moderate growth (z-score ~2.4)
			threshold:   1.5,
			shouldAlert: true,
			description: "Moderate increase beyond 1.5 std dev should alert",
		},
		{
			name: "seasonal pattern - no alert",
			historicalData: []float64{
				100, 110, 105, 108, 102, 109, 106,
				104, 107, 103, 105, 108, 106, 105,
			},
			newValue:    110,
			threshold:   3.0,
			shouldAlert: false,
			description: "Value within seasonal variance should not alert",
		},
		{
			name: "double the normal volume",
			historicalData: []float64{
				5_000_000, 5_100_000, 5_200_000, 5_000_000,
				5_300_000, 5_100_000, 5_000_000, 5_200_000,
			},
			newValue:    10_000_000, // 2x spike
			threshold:   3.0,
			shouldAlert: true,
			description: "Doubling volume should alert with 3.0 threshold",
		},
		{
			name: "small sample conservative threshold",
			historicalData: []float64{
				1_000_000, 1_100_000, 1_050_000, 1_080_000,
				1_020_000, 1_090_000,
			},
			newValue:    5_000_000, // ~5x spike
			threshold:   5.0,       // High threshold for small sample
			shouldAlert: true,
			description: "Even with high threshold, 5x spike should alert",
		},
		{
			name: "zero standard deviation edge case",
			historicalData: []float64{
				100, 100, 100, 100, 100, 100, 100,
			},
			newValue:    100,
			threshold:   3.0,
			shouldAlert: false,
			description: "No variance, same value should not alert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean := CalculateMean(tt.historicalData)
			stdDev := CalculateStandardDeviation(tt.historicalData, mean)

			// Handle zero standard deviation case
			if stdDev == 0 || math.IsNaN(stdDev) {
				t.Logf("Zero or NaN standard deviation detected, skipping anomaly check")
				if tt.shouldAlert {
					t.Errorf("Expected alert but got zero/NaN stdDev")
				}
				return
			}

			zScore := CalculateZScore(tt.newValue, mean, stdDev)
			isAnomaly := zScore > tt.threshold

			percentageChange := ((tt.newValue - mean) / mean) * 100

			t.Logf("Historical Mean: %.2f, StdDev: %.2f", mean, stdDev)
			t.Logf("New Value: %.2f, Z-Score: %.2f, Threshold: %.2f", tt.newValue, zScore, tt.threshold)
			t.Logf("Percentage Change: %.1f%%", percentageChange)

			if isAnomaly != tt.shouldAlert {
				t.Errorf("%s: isAnomaly = %v, want %v (z-score: %.2f, threshold: %.2f, change: %.1f%%)",
					tt.description, isAnomaly, tt.shouldAlert, zScore, tt.threshold, percentageChange)
			}
		})
	}
}

// TestDynamicThresholdBehavior tests how different thresholds affect anomaly detection
func TestDynamicThresholdBehavior(t *testing.T) {
	historicalData := []float64{
		10_000_000, 11_000_000, 10_500_000, 10_800_000,
		10_200_000, 10_900_000, 10_600_000, 10_400_000,
		10_700_000, 10_300_000,
	}

	mean := CalculateMean(historicalData)
	stdDev := CalculateStandardDeviation(historicalData, mean)

	testValues := []struct {
		value       float64
		description string
	}{
		{11_000_000, "normal value"},
		{15_000_000, "moderate spike (+50%)"},
		{20_000_000, "large spike (+100%)"},
		{30_000_000, "extreme spike (+200%)"},
		{50_000_000, "massive spike (+400%)"},
	}

	thresholds := []float64{1.5, 3.0, 5.0}

	t.Logf("Historical Mean: %.2f, StdDev: %.2f", mean, stdDev)
	t.Logf("\n%-20s %-15s %-15s %-15s %-15s", "Value", "Z-Score", "1.5 Threshold", "3.0 Threshold", "5.0 Threshold")
	separator := "--------------------------------------------------------------------------------"
	t.Log(separator)

	for _, tv := range testValues {
		zScore := CalculateZScore(tv.value, mean, stdDev)
		results := make([]string, len(thresholds))

		for i, threshold := range thresholds {
			if zScore > threshold {
				results[i] = "ALERT"
			} else {
				results[i] = "ok"
			}
		}

		t.Logf("%-20s %-15.2f %-15s %-15s %-15s",
			tv.description, zScore, results[0], results[1], results[2])
	}
}

// TestGetEnvInt64FromString tests the environment variable parsing function
func TestGetEnvInt64FromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "valid positive integer",
			input:    "123",
			expected: 123,
		},
		{
			name:     "valid negative integer",
			input:    "-456",
			expected: -456,
		},
		{
			name:     "zero",
			input:    "0",
			expected: 0,
		},
		{
			name:     "invalid string",
			input:    "abc",
			expected: 0,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "decimal number",
			input:    "12.34",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetEnvInt64FromString(tt.input)
			if result != tt.expected {
				t.Errorf("GetEnvInt64FromString(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
