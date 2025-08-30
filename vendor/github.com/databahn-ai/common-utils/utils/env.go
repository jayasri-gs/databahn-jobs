package utils

import (
	"os"
	"strconv"
	"strings"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func GetEnvInt(key string, def int) int {
	env := os.Getenv(key)
	var intVal int
	if env != "" {
		val, err := strconv.Atoi(env)
		if err != nil {
			logger.GetLogger().Error("Failed to parse Env Variable", zap.String("key", key), zap.String("val", env), zap.Error(err))
			intVal = def
		} else {
			intVal = val
		}
	} else {
		intVal = def
	}
	return intVal
}

func GetEnvOrDefault(key string, def string) string {
	env := os.Getenv(key)
	if env == "" {
		return def
	}
	return env
}

func GetValueOrDefault(value interface{}, defaultValue string) string {
	if value != nil && value.(string) != "" {
		return value.(string)
	}
	return defaultValue
}

func UseOrAddProtocol(url string) string {
	if url != "" {
		if url[:4] != "http" {
			url = "https://" + url
		}
	}
	return url
}

func GetMaskedString(token string, length int) string {
	if len(token) <= length {
		return token // If the token is 4 characters or less, return it as is
	}
	masked := strings.Repeat("*", len(token)-length) + token[len(token)-length:]
	return masked
}

// GetEnvFloat gets a float value from environment variable with a default value
func GetEnvFloat(key string, defaultValue float64) float64 {
	envValue := os.Getenv(key)
	if envValue == "" {
		return defaultValue
	}

	floatValue, err := strconv.ParseFloat(envValue, 64)
	if err != nil {
		logger.GetLogger().Error("Failed to parse float environment variable, using default",
			zap.String("key", key), zap.String("value", envValue), zap.Float64("default", defaultValue), zap.Error(err))
		return defaultValue
	}

	return floatValue
}

// GetEnvBool gets a boolean value from environment variable with a default value
func GetEnvBool(key string, defaultValue bool) bool {
	envValue := os.Getenv(key)
	if envValue == "" {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(envValue)
	if err != nil {
		logger.GetLogger().Error("Failed to parse boolean environment variable, using default",
			zap.String("key", key), zap.String("value", envValue), zap.Bool("default", defaultValue), zap.Error(err))
		return defaultValue
	}

	return boolValue
}
