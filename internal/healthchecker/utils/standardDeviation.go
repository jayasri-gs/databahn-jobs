package utils

import (
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"math"
)

func CalculateMean(data []statistics.HistogramBucket) float64 {
	sum := 0.0
	for _, value := range data {
		sum += value.Value
	}
	mean := sum / float64(len(data))
	return mean
}
func CalculateStdDev(data []statistics.HistogramBucket) float64 {
	// Calculate mean
	sum := 0.0
	for _, value := range data {
		sum += value.Value
	}
	mean := sum / float64(len(data))

	// Calculate sum of squared differences from the mean
	sumSquaredDiff := 0.0
	for _, value := range data {
		diff := value.Value - mean
		sumSquaredDiff += diff * diff
	}

	// Calculate standard deviation
	stdDev := math.Sqrt(sumSquaredDiff / float64(len(data)))

	return stdDev
}
func CalculateZScore(data []statistics.HistogramBucket) []float64 {
	// Calculate mean
	sum := 0.0
	for _, value := range data {
		sum += value.Value
	}
	mean := sum / float64(len(data))

	// Calculate sum of squared differences from the mean
	sumSquaredDiff := 0.0
	for _, value := range data {
		diff := value.Value - mean
		sumSquaredDiff += diff * diff
	}

	// Calculate standard deviation
	stdDev := math.Sqrt(sumSquaredDiff / float64(len(data)))

	// Calculate z-score
	var zScores []float64
	for _, value := range data {
		zScore := (value.Value - mean) / stdDev
		zScores = append(zScores, zScore)
	}

	return zScores
}

func CreateThresholds(data []statistics.HistogramBucket) (float64, float64) {
	// Calculate standard deviation dynamically
	stdDev := CalculateStdDev(data)

	// Adjust thresholds based on standard deviation
	silentThreshold := stdDev * 0.5
	noisyThreshold := stdDev * 1.5

	return silentThreshold, noisyThreshold
}

func ClassifySources(std, stdThreshold float64) int {
	if std < stdThreshold {
		return common.WHISPERING
	} else {
		return common.NOISY
	}
}
