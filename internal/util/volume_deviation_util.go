package util

import (
	"fmt"
	"time"
)

// VolumeDeviationDateRange represents date ranges for volume deviation comparison
type VolumeDeviationDateRange struct {
	// Current day (the day we're checking) - used for both comparisons
	CurrentDayStart time.Time
	CurrentDayEnd   time.Time

	// Yesterday (24 hours before current day) - for Condition 1: current day vs yesterday
	YesterdayStart time.Time
	YesterdayEnd   time.Time

	// Same day from previous week - for Condition 2: current day vs same day previous week
	PreviousWeekSameDayStart time.Time
	PreviousWeekSameDayEnd   time.Time
}

// CalculateVolumeDeviationDateRange calculates all date ranges needed for volume deviation comparison
// This function is reusable for both agent and log source level alerts
// It calculates:
// 1. Current day: Full day of yesterday (00:00:00 to 23:59:59) - for 4AM runs analyzing previous day
// 2. Yesterday: Full day before current day (00:00:00 to 23:59:59)
// 3. Previous week same day: Same full day from 7 days before current day
func CalculateVolumeDeviationDateRange() *VolumeDeviationDateRange {
	now := time.Now().UTC()

	// Use yesterday as "current day" (for 4AM runs analyzing previous day)
	targetDay := now.AddDate(0, 0, -1)

	// Current day: full day of yesterday (00:00:00 to 23:59:59)
	currentDayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	currentDayEnd := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 23, 59, 59, 999999999, time.UTC)

	// Yesterday: day before target day (full day)
	yesterdayDay := targetDay.AddDate(0, 0, -1)
	yesterdayStart := time.Date(yesterdayDay.Year(), yesterdayDay.Month(), yesterdayDay.Day(), 0, 0, 0, 0, time.UTC)
	yesterdayEnd := time.Date(yesterdayDay.Year(), yesterdayDay.Month(), yesterdayDay.Day(), 23, 59, 59, 999999999, time.UTC)

	// Previous week same day: same full day from 7 days before target day
	previousWeekSameDay := targetDay.AddDate(0, 0, -7)
	previousWeekSameDayStart := time.Date(previousWeekSameDay.Year(), previousWeekSameDay.Month(), previousWeekSameDay.Day(), 0, 0, 0, 0, time.UTC)
	previousWeekSameDayEnd := time.Date(previousWeekSameDay.Year(), previousWeekSameDay.Month(), previousWeekSameDay.Day(), 23, 59, 59, 999999999, time.UTC)

	return &VolumeDeviationDateRange{
		CurrentDayStart:          currentDayStart,
		CurrentDayEnd:            currentDayEnd,
		YesterdayStart:           yesterdayStart,
		YesterdayEnd:             yesterdayEnd,
		PreviousWeekSameDayStart: previousWeekSameDayStart,
		PreviousWeekSameDayEnd:   previousWeekSameDayEnd,
	}
}

// CalculateVolumeDeviationPercentage calculates the percentage deviation between two volumes
// Returns positive value for increase, negative for decrease
// When oldVolume is 0, returns 0 (no meaningful percentage comparison can be made)
func CalculateVolumeDeviationPercentage(oldVolume, newVolume float64) float64 {
	if oldVolume == 0 {
		// No meaningful percentage comparison when baseline is zero
		// Alerts for spike from zero are handled separately in the alert logic
		return 0
	}
	return ((newVolume - oldVolume) / oldVolume) * 100.0
}

// CheckSpikeOrDrop checks if volume deviation exceeds thresholds for spike or drop
// Returns: (isSpike, isDrop, deviationPercentage)
func CheckSpikeOrDrop(currentVolume, previousVolume, spikeThreshold, dropThreshold float64) (bool, bool, float64) {
	deviation := CalculateVolumeDeviationPercentage(previousVolume, currentVolume)

	isSpike := deviation > spikeThreshold
	isDrop := deviation < -dropThreshold // Negative deviation means drop

	return isSpike, isDrop, deviation
}

// FormatVolumeDeviationMessage formats a message for volume deviation alerts
func FormatVolumeDeviationMessage(entityType, entityName, entityId string, isSpike bool, deviationPercent, currentVolume, previousVolume float64, currentDate, previousDate time.Time) string {
	direction := "decrease"
	if isSpike {
		direction = "increase"
	}

	currentDateStr := currentDate.Format("2006-01-02")
	previousDateStr := previousDate.Format("2006-01-02")

	return fmt.Sprintf(
		"%s '%s' (ID: %s) shows a %.1f%% %s in volume. "+
			"Current period (%s): %s, "+
			"Previous period (%s): %s",
		entityType,
		entityName,
		entityId,
		deviationPercent,
		direction,
		currentDateStr,
		HumanReadableBytes(int64(currentVolume)),
		previousDateStr,
		HumanReadableBytes(int64(previousVolume)),
	)
}
