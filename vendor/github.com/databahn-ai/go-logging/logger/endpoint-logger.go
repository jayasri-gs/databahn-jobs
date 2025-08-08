package logger

import (
	"os"

	"github.com/databahn-ai/go-logging/logger/constants"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// newEndpointLogger initializes a new logger for the endpoint.
// It uses lumberjack for log rotation and zap for structured logging.
// The logger is configured to write JSON logs and includes service version and name if available.
// The log location, max size, max backups, and max age are configurable.
// The logger is created only once using sync.Once to ensure thread safety.
// The logger is returned as a pointer to zap.Logger.
// The function takes the log location, max size, max backups, and max age as parameters.
// The log location is the file path where the logs will be written.
// The max size is the maximum size of the log file in megabytes before it is rotated.
// The max backups is the maximum number of old log files to keep.
// The max age is the maximum number of days to keep old log files.
func newEndpointLogger(logLocation string, maxSize int, maxBackups int, maxAge int, fields ...zap.Field) *zap.Logger {
	loggerOnceEndpointLogger.Do(func() {
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
		if len(fields) > 0 {
			logger = logger.With(fields...)
		}
	})
	return logger
}

func GetEndpointLoggerWithFields(logLocation string, maxSize int, maxBackups int, maxAge int, fields ...zap.Field) *zap.Logger {
	logger := newEndpointLogger(logLocation, maxSize, maxBackups, maxAge, fields...)
	return logger
}

func GetEndpointLogger(logLocation string, maxSize int, maxBackups int, maxAge int) *zap.Logger {
	return GetEndpointLoggerWithFields(logLocation, maxSize, maxBackups, maxAge)
}
