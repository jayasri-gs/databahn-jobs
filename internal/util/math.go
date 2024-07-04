package util

import (
	"math"
	"strconv"
)

func CalculateMean(numbers []float64) float64 {
	sum := 0.0
	for _, num := range numbers {
		sum += num
	}
	return sum / float64(len(numbers))
}

func CalculateStandardDeviation(numbers []float64, mean float64) float64 {
	var sumSquares float64
	for _, num := range numbers {
		diff := num - mean
		sumSquares += diff * diff
	}
	variance := sumSquares / float64(len(numbers))
	return math.Sqrt(variance)
}

func CalculateZScore(newNumber, mean, stdDev float64) float64 {
	return (newNumber - mean) / stdDev
}

func GetEnvInt64FromString(env string) int {
	d, err := strconv.Atoi(env)
	if err != nil {
		return 0
	}
	return d
}
