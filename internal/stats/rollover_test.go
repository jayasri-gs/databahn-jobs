package stats

import (
	"encoding/json"
	"strings"
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
		{
			name:     "Time difference less than duration - creates single aligned range",
			minEpoch: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC).UnixMilli(), // 1:00 PM Jan 1
			maxEpoch: time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC).UnixMilli(), // 2:00 AM Jan 1
			duration: time.Hour * 24,
			expected: []timeRange{
				{start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli() + 1},
			},
			description: "Should handle time difference less than duration by creating single aligned range",
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

func TestIsWeekDifferenceMoreThan(t *testing.T) {
	tests := []struct {
		name     string
		x        int
		yearThis int
		weekThis int
		yearThat int
		weekThat int
		expected bool
	}{
		{
			name:     "Same week - difference is 0, threshold 1",
			x:        1,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "Same week - difference is 0, threshold 0",
			x:        0,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "This is 1 week newer than that, threshold 0",
			x:        0,
			yearThis: 2024,
			weekThis: 11,
			yearThat: 2024,
			weekThat: 10,
			expected: true,
		},
		{
			name:     "This is 1 week newer than that, threshold 1",
			x:        1,
			yearThis: 2024,
			weekThis: 11,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "This is 2 weeks newer than that, threshold 1",
			x:        1,
			yearThis: 2024,
			weekThis: 12,
			yearThat: 2024,
			weekThat: 10,
			expected: true,
		},
		{
			name:     "This is 5 weeks newer than that, threshold 5",
			x:        5,
			yearThis: 2024,
			weekThis: 15,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "This is 5 weeks newer than that, threshold 4",
			x:        4,
			yearThis: 2024,
			weekThis: 15,
			yearThat: 2024,
			weekThat: 10,
			expected: true,
		},
		{
			name:     "That is newer than this - returns false",
			x:        4,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2024,
			weekThat: 15,
			expected: false,
		},
		{
			name:     "This crosses year boundary - that is older",
			x:        2,
			yearThis: 2024,
			weekThis: 2,
			yearThat: 2023,
			weekThat: 52,
			expected: false,
		},
		{
			name:     "This crosses year boundary - that is much older",
			x:        2,
			yearThis: 2024,
			weekThis: 2,
			yearThat: 2023,
			weekThat: 51,
			expected: true,
		},
		{
			name:     "This is one year newer - 52 weeks, threshold 51",
			x:        51,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2023,
			weekThat: 10,
			expected: true,
		},
		{
			name:     "This is one year newer - 52 weeks, threshold 52",
			x:        52,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2023,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "This is 3 years newer - 156 weeks, threshold 155",
			x:        155,
			yearThis: 2023,
			weekThis: 1,
			yearThat: 2020,
			weekThat: 1,
			expected: true,
		},
		{
			name:     "This is 3 years newer - 156 weeks, threshold 156",
			x:        156,
			yearThis: 2023,
			weekThis: 1,
			yearThat: 2020,
			weekThat: 1,
			expected: false,
		},
		{
			name:     "Large threshold, small difference",
			x:        100,
			yearThis: 2024,
			weekThis: 15,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "Exactly at boundary - 10 weeks difference, threshold 10",
			x:        10,
			yearThis: 2024,
			weekThis: 15,
			yearThat: 2024,
			weekThat: 5,
			expected: false,
		},
		{
			name:     "Just above boundary - 11 weeks difference, threshold 10",
			x:        10,
			yearThis: 2024,
			weekThis: 16,
			yearThat: 2024,
			weekThat: 5,
			expected: true,
		},
		{
			name:     "This at week 52, that at week 1 same year - that is newer",
			x:        50,
			yearThis: 2024,
			weekThis: 52,
			yearThat: 2024,
			weekThat: 1,
			expected: true,
		},
		{
			name:     "Zero threshold with 1 week difference",
			x:        0,
			yearThis: 2024,
			weekThis: 21,
			yearThat: 2024,
			weekThat: 20,
			expected: true,
		},
		{
			name:     "This is recent, that is older from previous year",
			x:        2,
			yearThis: 2024,
			weekThis: 5,
			yearThat: 2023,
			weekThat: 50,
			expected: true,
		},
		{
			name:     "That is newer across year boundary",
			x:        0,
			yearThis: 2023,
			weekThis: 52,
			yearThat: 2024,
			weekThat: 1,
			expected: false,
		},
		{
			name:     "Mid-year span - that is older",
			x:        20,
			yearThis: 2024,
			weekThis: 26,
			yearThat: 2024,
			weekThat: 1,
			expected: true,
		},
		{
			name:     "This is 2 years newer",
			x:        100,
			yearThis: 2024,
			weekThis: 1,
			yearThat: 2022,
			weekThat: 1,
			expected: true,
		},
		{
			name:     "Sequential weeks - this is 1 week newer",
			x:        0,
			yearThis: 2024,
			weekThis: 16,
			yearThat: 2024,
			weekThat: 15,
			expected: true,
		},
		{
			name:     "10 weeks difference, threshold 9",
			x:        9,
			yearThis: 2024,
			weekThis: 20,
			yearThat: 2024,
			weekThat: 10,
			expected: true,
		},
		{
			name:     "10 weeks difference, threshold 10",
			x:        10,
			yearThis: 2024,
			weekThis: 20,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
		{
			name:     "Large year gap - this is 10 years newer",
			x:        500,
			yearThis: 2025,
			weekThis: 1,
			yearThat: 2015,
			weekThat: 1,
			expected: true,
		},
		{
			name:     "That is older by 1 week, threshold 0",
			x:        0,
			yearThis: 2024,
			weekThis: 15,
			yearThat: 2024,
			weekThat: 14,
			expected: true,
		},
		{
			name:     "That is newer - negative difference",
			x:        5,
			yearThis: 2024,
			weekThis: 10,
			yearThat: 2024,
			weekThat: 20,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isWeekDifferenceMoreThan(tt.x, tt.yearThis, tt.weekThis, tt.yearThat, tt.weekThat)

			if result != tt.expected {
				t.Errorf("isWeekDifferenceMoreThan(%d, yearThis=%d, weekThis=%d, yearThat=%d, weekThat=%d) = %v; expected %v",
					tt.x, tt.yearThis, tt.weekThis, tt.yearThat, tt.weekThat, result, tt.expected)
			}
		})
	}
}

func TestIsWeekDifferenceMoreThanEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		x           int
		yearThis    int
		weekThis    int
		yearThat    int
		weekThat    int
		description string
	}{
		{
			name:        "Negative threshold handling",
			x:           -1,
			yearThis:    2024,
			weekThis:    10,
			yearThat:    2024,
			weekThat:    10,
			description: "Same week with negative threshold should return false (0 > -1 is true)",
		},
		{
			name:        "Very large year difference - this is newer",
			x:           100,
			yearThis:    2024,
			weekThis:    1,
			yearThat:    2020,
			weekThat:    1,
			description: "4 years = 208 weeks, should be > 100",
		},
		{
			name:        "Week 52 vs Week 1 - this is newer",
			x:           51,
			yearThis:    2024,
			weekThis:    52,
			yearThat:    2024,
			weekThat:    1,
			description: "51 weeks difference within same year, this is newer",
		},
		{
			name:        "Same year, large week span - this is newer",
			x:           30,
			yearThis:    2024,
			weekThis:    40,
			yearThat:    2024,
			weekThat:    1,
			description: "39 weeks difference, threshold 30, this is newer",
		},
		{
			name:        "Zero weeks difference with high threshold",
			x:           1000,
			yearThis:    2024,
			weekThis:    25,
			yearThat:    2024,
			weekThat:    25,
			description: "Same week with very high threshold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify it doesn't panic and returns a boolean
			result := isWeekDifferenceMoreThan(tt.x, tt.yearThis, tt.weekThis, tt.yearThat, tt.weekThat)
			t.Logf("%s: result = %v", tt.description, result)
		})
	}
}

func TestIsWeekDifferenceMoreThanBoundaryConditions(t *testing.T) {
	// Test the exact boundary between true and false
	// This is at week 20, That is at week 10 -> difference is 10 weeks
	yearThis, weekThis := 2024, 20
	yearThat, weekThat := 2024, 10

	// At threshold = 9, should return true (10 > 9)
	if !isWeekDifferenceMoreThan(9, yearThis, weekThis, yearThat, weekThat) {
		t.Errorf("Expected true when difference (10) > threshold (9)")
	}

	// At threshold = 10, should return false (10 > 10 is false)
	if isWeekDifferenceMoreThan(10, yearThis, weekThis, yearThat, weekThat) {
		t.Errorf("Expected false when difference (10) == threshold (10)")
	}

	// At threshold = 11, should return false (10 > 11 is false)
	if isWeekDifferenceMoreThan(11, yearThis, weekThis, yearThat, weekThat) {
		t.Errorf("Expected false when difference (10) < threshold (11)")
	}

	// Test reverse: That is newer than This -> negative difference
	// This is at week 10, That is at week 20 -> difference is -10 weeks
	if isWeekDifferenceMoreThan(0, yearThat, weekThat, yearThis, weekThis) {
		t.Errorf("Expected false when that is newer than this")
	}
}

func TestIsWeekDifferenceMoreThanYearTransitions(t *testing.T) {
	tests := []struct {
		name     string
		x        int
		yearThis int
		weekThis int
		yearThat int
		weekThat int
		expected bool
	}{
		{
			name:     "This at beginning of 2024, That at end of 2023 - that is older",
			x:        0,
			yearThis: 2024,
			weekThis: 1,
			yearThat: 2023,
			weekThat: 52,
			expected: true,
		},
		{
			name:     "This at beginning of 2024, That at end of 2023 - threshold 1",
			x:        1,
			yearThis: 2024,
			weekThis: 1,
			yearThat: 2023,
			weekThat: 52,
			expected: false,
		},
		{
			name:     "This at mid-2024, That at mid-2023",
			x:        52,
			yearThis: 2024,
			weekThis: 26,
			yearThat: 2023,
			weekThat: 26,
			expected: false,
		},
		{
			name:     "This at mid-2024, That at mid-2023, threshold 51",
			x:        51,
			yearThis: 2024,
			weekThis: 26,
			yearThat: 2023,
			weekThat: 26,
			expected: true,
		},
		{
			name:     "Exactly 52 weeks apart - this is newer",
			x:        52,
			yearThis: 2023,
			weekThis: 15,
			yearThat: 2022,
			weekThat: 15,
			expected: false,
		},
		{
			name:     "53 weeks apart - this is newer",
			x:        52,
			yearThis: 2023,
			weekThis: 16,
			yearThat: 2022,
			weekThat: 15,
			expected: true,
		},
		{
			name:     "That is newer - returns false",
			x:        10,
			yearThis: 2023,
			weekThis: 10,
			yearThat: 2024,
			weekThat: 10,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isWeekDifferenceMoreThan(tt.x, tt.yearThis, tt.weekThis, tt.yearThat, tt.weekThat)
			if result != tt.expected {
				t.Errorf("isWeekDifferenceMoreThan(%d, yearThis=%d, weekThis=%d, yearThat=%d, weekThat=%d) = %v; expected %v",
					tt.x, tt.yearThis, tt.weekThis, tt.yearThat, tt.weekThat, result, tt.expected)
			}
		})
	}
}

func TestIsDayDifferenceMoreThan(t *testing.T) {
	tests := []struct {
		name     string
		x        int
		yearThis int
		dayThis  int
		yearThat int
		dayThat  int
		expected bool
	}{
		{
			name:     "Same day - difference is 0, threshold 1",
			x:        1,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Same day - difference is 0, threshold 0",
			x:        0,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "This is 1 day newer than that, threshold 0",
			x:        0,
			yearThis: 2024,
			dayThis:  101,
			yearThat: 2024,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "This is 1 day newer than that, threshold 1",
			x:        1,
			yearThis: 2024,
			dayThis:  101,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "This is 2 days newer than that, threshold 1",
			x:        1,
			yearThis: 2024,
			dayThis:  102,
			yearThat: 2024,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "This is 10 days newer than that, threshold 10",
			x:        10,
			yearThis: 2024,
			dayThis:  110,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "This is 10 days newer than that, threshold 9",
			x:        9,
			yearThis: 2024,
			dayThis:  110,
			yearThat: 2024,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "That is newer than this - returns false",
			x:        5,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2024,
			dayThat:  110,
			expected: false,
		},
		{
			name:     "This crosses year boundary - that is older",
			x:        5,
			yearThis: 2024,
			dayThis:  10,
			yearThat: 2023,
			dayThat:  360,
			expected: true,
		},
		{
			name:     "This is one year newer - 365 days",
			x:        364,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2023,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "This is one year newer - 365 days, threshold 365",
			x:        365,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2023,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "This is 2 years newer - 730 days",
			x:        729,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2022,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "This is 2 years newer - 730 days, threshold 730",
			x:        730,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2022,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Large threshold, small difference",
			x:        100,
			yearThis: 2024,
			dayThis:  110,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Exactly at boundary - 30 days difference, threshold 30",
			x:        30,
			yearThis: 2024,
			dayThis:  130,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Just above boundary - 31 days difference, threshold 30",
			x:        30,
			yearThis: 2024,
			dayThis:  131,
			yearThat: 2024,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "Day 365 vs day 1 same year - that is newer",
			x:        300,
			yearThis: 2024,
			dayThis:  365,
			yearThat: 2024,
			dayThat:  1,
			expected: true,
		},
		{
			name:     "Zero threshold with 1 day difference",
			x:        0,
			yearThis: 2024,
			dayThis:  200,
			yearThat: 2024,
			dayThat:  199,
			expected: true,
		},
		{
			name:     "This is recent, that is older from previous year",
			x:        50,
			yearThis: 2024,
			dayThis:  30,
			yearThat: 2023,
			dayThat:  300,
			expected: true,
		},
		{
			name:     "That is newer across year boundary",
			x:        0,
			yearThis: 2023,
			dayThis:  365,
			yearThat: 2024,
			dayThat:  1,
			expected: false,
		},
		{
			name:     "Mid-year span - that is older",
			x:        100,
			yearThis: 2024,
			dayThis:  200,
			yearThat: 2024,
			dayThat:  50,
			expected: true,
		},
		{
			name:     "Sequential days - this is 1 day newer",
			x:        0,
			yearThis: 2024,
			dayThis:  151,
			yearThat: 2024,
			dayThat:  150,
			expected: true,
		},
		{
			name:     "50 days difference, threshold 49",
			x:        49,
			yearThis: 2024,
			dayThis:  150,
			yearThat: 2024,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "50 days difference, threshold 50",
			x:        50,
			yearThis: 2024,
			dayThis:  150,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Large year gap - this is 5 years newer",
			x:        1000,
			yearThis: 2025,
			dayThis:  1,
			yearThat: 2020,
			dayThat:  1,
			expected: true,
		},
		{
			name:     "That is newer - negative difference",
			x:        10,
			yearThis: 2024,
			dayThis:  100,
			yearThat: 2024,
			dayThat:  200,
			expected: false,
		},
		{
			name:     "End of year to beginning of next year",
			x:        5,
			yearThis: 2024,
			dayThis:  5,
			yearThat: 2023,
			dayThat:  360,
			expected: true,
		},
		{
			name:     "Start of year - 89 days difference, threshold 88",
			x:        88,
			yearThis: 2024,
			dayThis:  90,
			yearThat: 2024,
			dayThat:  1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDayDifferenceMoreThan(tt.x, tt.yearThis, tt.dayThis, tt.yearThat, tt.dayThat)

			if result != tt.expected {
				t.Errorf("isDayDifferenceMoreThan(%d, yearThis=%d, dayThis=%d, yearThat=%d, dayThat=%d) = %v; expected %v",
					tt.x, tt.yearThis, tt.dayThis, tt.yearThat, tt.dayThat, result, tt.expected)
			}
		})
	}
}

func TestIsDayDifferenceMoreThanEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		x           int
		yearThis    int
		dayThis     int
		yearThat    int
		dayThat     int
		description string
	}{
		{
			name:        "Negative threshold handling",
			x:           -1,
			yearThis:    2024,
			dayThis:     100,
			yearThat:    2024,
			dayThat:     100,
			description: "Same day with negative threshold should return false (0 > -1 is true)",
		},
		{
			name:        "Very large year difference - this is newer",
			x:           1000,
			yearThis:    2024,
			dayThis:     1,
			yearThat:    2020,
			dayThat:     1,
			description: "4 years = 1460 days, should be > 1000",
		},
		{
			name:        "Day 365 vs Day 1 - this is newer",
			x:           300,
			yearThis:    2024,
			dayThis:     365,
			yearThat:    2024,
			dayThat:     1,
			description: "364 days difference within same year, this is newer",
		},
		{
			name:        "Same year, large day span - this is newer",
			x:           200,
			yearThis:    2024,
			dayThis:     300,
			yearThat:    2024,
			dayThat:     1,
			description: "299 days difference, threshold 200, this is newer",
		},
		{
			name:        "Zero days difference with high threshold",
			x:           10000,
			yearThis:    2024,
			dayThis:     180,
			yearThat:    2024,
			dayThat:     180,
			description: "Same day with very high threshold",
		},
		{
			name:        "Large negative difference",
			x:           100,
			yearThis:    2023,
			dayThis:     1,
			yearThat:    2024,
			dayThat:     365,
			description: "That is much newer, should return false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify it doesn't panic and returns a boolean
			result := isDayDifferenceMoreThan(tt.x, tt.yearThis, tt.dayThis, tt.yearThat, tt.dayThat)
			t.Logf("%s: result = %v", tt.description, result)
		})
	}
}

func TestIsDayDifferenceMoreThanBoundaryConditions(t *testing.T) {
	// Test the exact boundary between true and false
	// This is at day 150, That is at day 100 -> difference is 50 days
	yearThis, dayThis := 2024, 150
	yearThat, dayThat := 2024, 100

	// At threshold = 49, should return true (50 > 49)
	if !isDayDifferenceMoreThan(49, yearThis, dayThis, yearThat, dayThat) {
		t.Errorf("Expected true when difference (50) > threshold (49)")
	}

	// At threshold = 50, should return false (50 > 50 is false)
	if isDayDifferenceMoreThan(50, yearThis, dayThis, yearThat, dayThat) {
		t.Errorf("Expected false when difference (50) == threshold (50)")
	}

	// At threshold = 51, should return false (50 > 51 is false)
	if isDayDifferenceMoreThan(51, yearThis, dayThis, yearThat, dayThat) {
		t.Errorf("Expected false when difference (50) < threshold (51)")
	}

	// Test reverse: That is newer than This -> negative difference
	// This is at day 100, That is at day 150 -> difference is -50 days
	if isDayDifferenceMoreThan(0, yearThat, dayThat, yearThis, dayThis) {
		t.Errorf("Expected false when that is newer than this")
	}
}

func TestIsDayDifferenceMoreThanYearTransitions(t *testing.T) {
	tests := []struct {
		name     string
		x        int
		yearThis int
		dayThis  int
		yearThat int
		dayThat  int
		expected bool
	}{
		{
			name:     "This at beginning of 2024, That at end of 2023 - that is older",
			x:        5,
			yearThis: 2024,
			dayThis:  10,
			yearThat: 2023,
			dayThat:  360,
			expected: true,
		},
		{
			name:     "This at beginning of 2024, That at end of 2023 - threshold 15",
			x:        15,
			yearThis: 2024,
			dayThis:  10,
			yearThat: 2023,
			dayThat:  360,
			expected: false,
		},
		{
			name:     "This at mid-2024, That at mid-2023",
			x:        365,
			yearThis: 2024,
			dayThis:  180,
			yearThat: 2023,
			dayThat:  180,
			expected: false,
		},
		{
			name:     "This at mid-2024, That at mid-2023, threshold 364",
			x:        364,
			yearThis: 2024,
			dayThis:  180,
			yearThat: 2023,
			dayThat:  180,
			expected: true,
		},
		{
			name:     "Exactly 365 days apart - this is newer",
			x:        365,
			yearThis: 2023,
			dayThis:  100,
			yearThat: 2022,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "366 days apart - this is newer",
			x:        365,
			yearThis: 2023,
			dayThis:  101,
			yearThat: 2022,
			dayThat:  100,
			expected: true,
		},
		{
			name:     "That is newer - returns false",
			x:        100,
			yearThis: 2023,
			dayThis:  100,
			yearThat: 2024,
			dayThat:  100,
			expected: false,
		},
		{
			name:     "Cross year by 1 day",
			x:        0,
			yearThis: 2024,
			dayThis:  1,
			yearThat: 2023,
			dayThat:  365,
			expected: true,
		},
		{
			name:     "Multiple years - 3 years difference",
			x:        1000,
			yearThis: 2023,
			dayThis:  1,
			yearThat: 2020,
			dayThat:  1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDayDifferenceMoreThan(tt.x, tt.yearThis, tt.dayThis, tt.yearThat, tt.dayThat)
			if result != tt.expected {
				t.Errorf("isDayDifferenceMoreThan(%d, yearThis=%d, dayThis=%d, yearThat=%d, dayThat=%d) = %v; expected %v",
					tt.x, tt.yearThis, tt.dayThis, tt.yearThat, tt.dayThat, result, tt.expected)
			}
		})
	}
}

func TestNewIntegrationRolloverConfig_RejectsNonPositiveBucket(t *testing.T) {
	if _, err := NewIntegrationRolloverConfig(0); err == nil {
		t.Fatal("expected error for zero bucket")
	}
	if _, err := NewIntegrationRolloverConfig(-time.Hour); err == nil {
		t.Fatal("expected error for negative bucket")
	}
	cfg, err := NewIntegrationRolloverConfig(time.Hour)
	if err != nil {
		t.Fatalf("unexpected error for valid bucket: %v", err)
	}
	if cfg.aggWindow != time.Hour {
		t.Fatalf("aggWindow = %s, want 1h", cfg.aggWindow)
	}
}

func TestValidateRolloverDurations_RejectsNonPositive(t *testing.T) {
	cfg := &RolloverConfig{
		aggQueryRange:   0,
		validationRange: time.Hour,
		aggWindow:       time.Hour,
	}
	if err := validateRolloverDurations(cfg); err == nil {
		t.Fatal("expected error for non-positive aggQueryRange")
	}
}

func TestBuildRolloverAggRequest_IncludesOperatorIdCompositeSource(t *testing.T) {
	req := buildRolloverAggRequest(1_700_000_000_000, 1_700_003_600_000, nil, 100, time.Hour)
	if len(req.Aggs.CompositeBuckets.Composite.Sources) != 8 {
		t.Fatalf("expected 8 composite sources, got %d", len(req.Aggs.CompositeBuckets.Composite.Sources))
	}

	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal rollover request: %v", err)
	}
	body := string(payload)
	if !strings.Contains(body, `"operator_id"`) {
		t.Fatalf("expected operator_id composite source in request body: %s", body)
	}
	if !strings.Contains(body, fieldOperatorId) {
		t.Fatalf("expected %q field in request body: %s", fieldOperatorId, body)
	}
}

func TestKey_newDocKey_DifferentOperatorIdsProduceDifferentHashes(t *testing.T) {
	base := Key{
		Name:                 "total_events_delivered",
		Namespace:            "freeform-pipeline-executor",
		SourceId:             "source-1",
		DestinationId:        "N/A",
		RuleId:               "N/A",
		FleetNodeId:          "N/A",
		TimeHistogramBuckets: 1_700_000_000_000,
	}
	opA := base
	opA.OperatorId = "parse-abc"
	opB := base
	opB.OperatorId = "transform-xyz"

	hashA := opA.newDocKey("db_statistics_v2_p1_tenant_y2024_d100")
	hashB := opB.newDocKey("db_statistics_v2_p1_tenant_y2024_d100")
	if hashA == hashB {
		t.Fatalf("expected different doc keys for different operator ids, got %q", hashA)
	}
}

func TestAggKeyString_NormalizesMissingOperatorId(t *testing.T) {
	tests := []struct {
		name     string
		key      map[string]any
		expected string
	}{
		{
			name:     "missing key",
			key:      map[string]any{},
			expected: missingOperatorTagValue,
		},
		{
			name:     "empty string",
			key:      map[string]any{fieldOperatorId: ""},
			expected: missingOperatorTagValue,
		},
		{
			name:     "present operator",
			key:      map[string]any{fieldOperatorId: "parse-abc"},
			expected: "parse-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := aggKeyString(tt.key, fieldOperatorId)
			if got != tt.expected {
				t.Fatalf("aggKeyString() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNewRequestSourceTermsAgg_UsesMissingValueForAbsentTag(t *testing.T) {
	agg := newRequestSourceTermsAgg(fieldOperatorId)
	script := agg.Terms.Script.Source
	if !strings.Contains(script, fieldOperatorId) {
		t.Fatalf("expected painless script to reference %q, got %q", fieldOperatorId, script)
	}
	if !strings.Contains(script, `'N/A'`) {
		t.Fatalf("expected painless script to default missing values to N/A, got %q", script)
	}
}
