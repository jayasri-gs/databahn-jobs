package utils

import (
	"crypto/rand"
	"fmt"
	"github.com/go-playground/validator/v10"
	"math/big"
)

func IsValid(object interface{}) error {
	validate := validator.New()
	err := validate.Struct(object)
	if err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			return fmt.Errorf("validation failed for %s. Field is %s", err.Field(), err.Tag())
		}
	}
	return nil
}

func GenerateRandomString(n int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return ""
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret)
}
