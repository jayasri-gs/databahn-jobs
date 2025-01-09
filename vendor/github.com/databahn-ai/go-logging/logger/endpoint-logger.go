package logger

import (
	"os"

	"github.com/databahn-ai/go-logging/logger/constants"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func newEndpointLogger(logLocation string, maxSize int, maxBackups int, maxAge int) *zap.Logger {
	loggerOnce.Do(func() {
		w := zapcore.AddSync(&lumberjack.Logger{
			Filename:   logLocation,
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
			MaxAge:     maxAge,
		})
		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(NewDbEncoderConfig()),
			w,
			zap.InfoLevel,
		)

		logger = zap.New(core, zap.AddCaller())
		if serviceVersion := os.Getenv(constants.ServiceVersion); serviceVersion != "" {
			logger = logger.With(zap.String("service_version", serviceVersion))
		}

		if serviceName := os.Getenv(constants.ServiceName); serviceName != "" {
			logger = logger.With(zap.String("service_name", serviceName))
		}
	})
	return logger
}

func GetEndpointLogger(logLocation string, maxSize int, maxBackups int, maxAge int) *zap.Logger {
	return newEndpointLogger(logLocation, maxSize, maxBackups, maxAge)
}
