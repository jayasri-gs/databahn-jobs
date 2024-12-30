package configuration

import (
	"context"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strings"
)

func GetErrorMessage(ct context.Context, alertConfigReader ConfigReader, errorCode string, params string) string {

	message := alertConfigReader.GetString(errorCode)
	if message == "" {
		logger.GetLoggerWithContext(ct).Error("Error code not found in configuration", zap.String("error_code", errorCode))
		return ""
	}
	if params == "" {
		return message
	}
	message = strings.ReplaceAll(message, "{0}", params)
	return message
}
