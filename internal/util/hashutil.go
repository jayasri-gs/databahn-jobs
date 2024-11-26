package util

import (
	"fmt"
	"github.com/twmb/murmur3"
)

func Hash(input string) string {
	h := murmur3.New128()
	h.Write([]byte(input))
	hs := h.Sum(nil)
	return fmt.Sprintf("%x", hs)
}
