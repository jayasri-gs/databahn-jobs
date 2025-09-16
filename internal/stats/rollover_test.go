package stats

import (
	"testing"
	"time"
)

func TestSplitByTimeRanges(t *testing.T) {
	tests := []struct {
		name        string
		minEpoch    int64
		maxEpoch    int64
		duration    time.Duration
		expected    []timeRange
		description string
	}{
		{
			name:     "1 hour duration - align to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 4, 23, 0, 0, time.UTC).UnixMilli(), // 4:23 AM
			maxEpoch: time.Date(2024, 1, 15, 7, 45, 0, 0, time.UTC).UnixMilli(), // 7:45 AM
			duration: time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 4, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 5, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 5, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 4:23 to 4:00 and create hourly ranges",
		},
		{
			name:     "2 hour duration - align to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 3, 15, 0, 0, time.UTC).UnixMilli(), // 3:15 AM
			maxEpoch: time.Date(2024, 1, 15, 9, 30, 0, 0, time.UTC).UnixMilli(), // 9:30 AM
			duration: 2 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 5, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 5, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 3:15 to 3:00 and create 2-hour ranges",
		},
		{
			name:     "4 hour duration - align to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 2, 45, 0, 0, time.UTC).UnixMilli(),  // 2:45 AM
			maxEpoch: time.Date(2024, 1, 15, 14, 20, 0, 0, time.UTC).UnixMilli(), // 2:20 PM
			duration: 4 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 18, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 2:45 to 2:00 and create 4-hour ranges",
		},
		{
			name:     "6 hour duration - align to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 1, 30, 0, 0, time.UTC).UnixMilli(),  // 1:30 AM
			maxEpoch: time.Date(2024, 1, 15, 19, 45, 0, 0, time.UTC).UnixMilli(), // 7:45 PM
			duration: 6 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 1, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 19, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 15, 19, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 1, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 1:30 to 1:00 and create 6-hour ranges",
		},
		{
			name:     "24 hour duration - align to day boundary",
			minEpoch: time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC).UnixMilli(), // 2:30 PM
			maxEpoch: time.Date(2024, 1, 17, 8, 15, 0, 0, time.UTC).UnixMilli(),  // 8:15 AM (2 days later)
			duration: 24 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 18, 0, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 14:30 to 00:00 and create daily ranges",
		},
		{
			name:     "48 hour duration - align to day boundary",
			minEpoch: time.Date(2024, 1, 15, 16, 45, 0, 0, time.UTC).UnixMilli(), // 4:45 PM
			maxEpoch: time.Date(2024, 1, 20, 10, 30, 0, 0, time.UTC).UnixMilli(), // 10:30 AM (5 days later)
			duration: 48 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 19, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 19, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 21, 0, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 16:45 to 00:00 and create 48-hour ranges",
		},
		{
			name:     "Duration exceeds time span - creates aligned range",
			minEpoch: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(),
			maxEpoch: time.Date(2024, 1, 15, 11, 30, 0, 0, time.UTC).UnixMilli(),
			duration: 3 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should create aligned range even when duration exceeds time span",
		},
		{
			name:     "Exact duration match",
			minEpoch: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(),
			maxEpoch: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC).UnixMilli(),
			duration: time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should return single range when duration exactly matches time span",
		},
		{
			name:     "Short time span with 1 hour duration - aligns to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 10, 15, 0, 0, time.UTC).UnixMilli(), // 10:15 AM
			maxEpoch: time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC).UnixMilli(), // 10:45 AM
			duration: time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 10:15 to 10:00 and create full hour range even for short time span",
		},
		{
			name:     "Short time span with 2 hour duration - aligns to hour boundary",
			minEpoch: time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC).UnixMilli(), // 2:30 PM
			maxEpoch: time.Date(2024, 1, 15, 15, 15, 0, 0, time.UTC).UnixMilli(), // 3:15 PM
			duration: 2 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 15, 16, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 14:30 to 14:00 and create full 2-hour range even for short time span",
		},
		{
			name:     "Short time span with 24 hour duration - aligns to day boundary",
			minEpoch: time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC).UnixMilli(), // 2:30 PM
			maxEpoch: time.Date(2024, 1, 15, 18, 45, 0, 0, time.UTC).UnixMilli(), // 6:45 PM
			duration: 24 * time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should align 14:30 to 00:00 and create full day range even for short time span",
		},
		{
			name:     "Cross midnight with 1 hour duration",
			minEpoch: time.Date(2024, 1, 15, 23, 15, 0, 0, time.UTC).UnixMilli(), // 11:15 PM
			maxEpoch: time.Date(2024, 1, 16, 2, 30, 0, 0, time.UTC).UnixMilli(),  // 2:30 AM next day
			duration: time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 15, 23, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 1, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 16, 1, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 2, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 1, 16, 2, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 16, 3, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should handle cross-midnight scenario with hour alignment",
		},
		{
			name:     "Cross month boundary with 1 hour duration",
			minEpoch: time.Date(2024, 1, 31, 23, 45, 0, 0, time.UTC).UnixMilli(), // 11:45 PM Jan 31
			maxEpoch: time.Date(2024, 2, 1, 1, 15, 0, 0, time.UTC).UnixMilli(),   // 1:15 AM Feb 1
			duration: time.Hour,
			expected: []timeRange{
				{start: time.Date(2024, 1, 31, 23, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 2, 1, 1, 0, 0, 0, time.UTC).UnixMilli()},
				{start: time.Date(2024, 2, 1, 1, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 2, 1, 2, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should handle cross-month boundary with hour alignment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitByTimeRanges(tt.minEpoch, tt.maxEpoch, tt.duration)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d ranges, got %d", len(tt.expected), len(result))
				return
			}

			for i, expectedRange := range tt.expected {
				if result[i].start != expectedRange.start {
					t.Errorf("Range %d start: expected %d, got %d", i, expectedRange.start, result[i].start)
				}
				if result[i].end != expectedRange.end {
					t.Errorf("Range %d end: expected %d, got %d", i, expectedRange.end, result[i].end)
				}
			}

			// Verify ranges are contiguous and non-overlapping
			for i := 1; i < len(result); i++ {
				if result[i].start != result[i-1].end {
					t.Errorf("Ranges %d and %d are not contiguous: %d != %d", i-1, i, result[i-1].end, result[i].start)
				}
			}
		})
	}
}
