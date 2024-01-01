package utils

import (
	"os"
	"strconv"

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
