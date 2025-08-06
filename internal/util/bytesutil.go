package util

import "fmt"

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
