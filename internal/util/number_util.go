package util

import "fmt"

func DataVolumeToBytes(vol int64, unit string) int64 {
	switch unit {
	case "B":
		return vol
	case "KB":
		return vol * 1024
	case "MB":
		return vol * 1024 * 1024
	case "GB":
		return vol * 1024 * 1024 * 1024
	case "TB":
		return vol * 1024 * 1024 * 1024 * 1024
	default:
		return vol // Default to bytes if unit is unrecognized
	}
}

func HumanReadableBytes(bytesSize int64) string {
	if bytesSize < 1024 {
		return fmt.Sprintf("%d B", bytesSize)
	}

	const unit = 1024
	div, exp := int64(unit), 0
	for n := bytesSize / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %cB", float64(bytesSize)/float64(div), "KMGTPE"[exp])
}

func HumanReadableNumber(number int64) string {
	if number < 1000 {
		return fmt.Sprintf("%d", number)
	}

	const unit = 1000
	div, exp := int64(unit), 0
	// Iterate to find the appropriate unit (K, M, etc.)
	for n := number / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	// Define suffixes for thousands, millions, billions, etc.
	suffixes := "KMBTQ"
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}

	value := float64(number) / float64(div)
	return fmt.Sprintf("%.2f %c", value, suffixes[exp])
}
