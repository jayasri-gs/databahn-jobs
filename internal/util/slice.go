package util

func Contains(l []string, key string) bool {
	for _, item := range l {
		if item == key {
			return true
		}
	}
	return false
}
