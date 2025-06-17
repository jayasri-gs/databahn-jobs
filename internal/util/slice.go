package util

func Contains(l []string, key string) bool {
	for _, item := range l {
		if item == key {
			return true
		}
	}
	return false
}

func PartitionSlice[T any](slice []T, batchSize int) [][]T {
	if len(slice) == 0 || batchSize <= 0 {
		return [][]T{}
	}
	var batches [][]T
	for batchSize < len(slice) {
		slice, batches = slice[batchSize:], append(batches, slice[0:batchSize:batchSize])
	}
	batches = append(batches, slice)
	return batches
}
