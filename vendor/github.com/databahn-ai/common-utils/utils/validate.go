package utils

import (
	"fmt"

	"github.com/go-playground/validator/v10"
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
