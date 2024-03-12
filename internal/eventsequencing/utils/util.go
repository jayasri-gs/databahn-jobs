package utils

import "strings"

func ReplaceChars(s string) string {
	if len(s) > 4 {
		return "******" + s[len(s)-3:]
	}
	return strings.Repeat("*", len(s))
}
