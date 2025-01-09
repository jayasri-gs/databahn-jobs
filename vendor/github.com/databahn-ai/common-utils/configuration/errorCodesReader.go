package configuration

import (
	"context"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strings"
)

const prefix = "alerts_mapping."

func GetErrorMessage(ct context.Context, alertConfigReader ConfigReader, errorCode string, params string) string {

	key := prefix + errorCode
	message := alertConfigReader.GetString(key)
	if message == "" {
		logger.GetLoggerWithContext(ct).Error("Error code not found in configuration", zap.String("error_code", errorCode))
		return params
	}
	if params == "" {
		message = strings.ReplaceAll(message, "{0}", "")
	} else {
		message = strings.ReplaceAll(message, "{0}", params)
	}
	return message
}
