package logger

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/apex/gateway"
	"github.com/databahn-ai/go-logging/logger/constants"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger      *zap.Logger
	loggerOnce  sync.Once
	loggerReady = false
)

// GetLogger - gets logging instance
func GetLogger() *zap.Logger {
	logger, _ = initLogger()
	return logger
}

// GetLoggerWithContext - gets logging instance
// ! NOTE this function panics!
func GetLoggerWithContext(ctx context.Context) (newLogger *zap.Logger) {
	if ctx == nil {
		panic("GetLoggerWithContext called with nil context.")
	}
	newLogger, _ = initLogger()

	if userUUID := ctx.Value(constants.UserUuid); userUUID != nil {
		newLogger = newLogger.With(zap.Any("user_uuid", userUUID))
	}
	if tenantUUID := ctx.Value(constants.TenantUuid); tenantUUID != nil {
		newLogger = newLogger.With(zap.Any("tenant_uuid", tenantUUID))
	}

	if fleetId := ctx.Value(constants.FleetId); fleetId != nil {
		newLogger = newLogger.With(zap.Any("fleet_id", fleetId))
	}

	if activationKey := ctx.Value(constants.ActivationCode); activationKey != nil {
		newLogger = newLogger.With(zap.Any("activation_code", activationKey))
	}

	return newLogger
}

// IsLoggerReady returns true if logging initialization was successful, false otherwise.
func IsLoggerReady() bool {
	return loggerReady
}

// getLogger - actual singleton for logging infrastructure
func initLogger() (*zap.Logger, error) {
	var err error
	loggerOnce.Do(func() {
		cfg := NewDbConfig()
		logger, err = cfg.Build()
		loggerReady = err == nil
		if serviceVersion := os.Getenv(constants.ServiceVersion); serviceVersion != "" {
			logger = logger.With(zap.String("service_version", serviceVersion))
		}

		if serviceName := os.Getenv(constants.ServiceName); serviceName != "" {
			logger = logger.With(zap.String("service_name", serviceName))
		}
	})
	return logger, err
}

func NewDbConfig() zap.Config {
	level := getLogLevel()
	encoding := getLogEncoding()
	config := zap.Config{
		Level:            level,
		Development:      false,
		Encoding:         encoding,
		EncoderConfig:    NewDbEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	if isSamplingEnabled() {
		initial, thereafter := getSamplingConfig()
		config.Sampling = &zap.SamplingConfig{
			Initial:    initial,
			Thereafter: thereafter,
		}
	}
	return config
}

func getLogLevel() zap.AtomicLevel {
	var level zap.AtomicLevel
	switch os.Getenv(constants.LoggingMode) {
	case constants.LogModeDebug:
		level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case constants.LogModeError:
		level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	case constants.LogModeWarn:
		level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case constants.LogModeInfo:
		level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	default:
		level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
	return level
}

func getLogEncoding() string {
	var encode string
	switch os.Getenv(constants.LoggingEncoding) {
	case constants.LoggingConsoleEncoding:
		encode = "console"
	case constants.LoggingJsonEncoding:
		encode = "json"
	default:
		encode = "json"
	}
	return encode
}

func NewDbEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// Init - ensures we're able to invoke logging before getting further into code.
// Returns error if something's amiss.
func init() {
	_, err := initLogger()
	if err != nil {
		fmt.Printf("issue setting up logger: %s", err.Error())
	}
}

// SetLoggingContexts sets up several commonly used logging contexts (userid,
// requestid, tenantid) for later logging.
func SetLoggingContexts(ctx context.Context) context.Context {
	requestContext, ok := gateway.RequestContext(ctx)
	if ok {
		ctx = context.WithValue(ctx, constants.LctxLambdaRequestId, requestContext.RequestID)
	}

	// I'm not calling util.GetTenantIDFromContext() etc here so that we don't have an import loop.
	var tenantUUID string
	var userUUID string
	id := ctx.Value(constants.TenantUuid)
	if id != nil {
		tenantUUID = id.(string)
	}
	id = ctx.Value(constants.UserUuid)
	if id != nil {
		userUUID = id.(string)
	}
	ctx = context.WithValue(ctx, constants.TenantUuid, tenantUUID)
	ctx = context.WithValue(ctx, constants.UserUuid, userUUID)
	return ctx
}

func isSamplingEnabled() bool {
	env := os.Getenv(constants.EnableLogSampling)
	return env != "false"
}

func getSamplingConfig() (int, int) {
	var initial, thereafter int
	maxEnv := os.Getenv(constants.LogSamplingMaxInitial)
	if maxEnv == "" {
		initial = constants.DefaultLogSamplingMaxInitial
	} else {
		parsed, err := strconv.ParseInt(maxEnv, 10, 64)
		if err != nil {
			initial = constants.DefaultLogSamplingMaxInitial
		} else {
			initial = int(parsed)
		}
	}
	rateEnv := os.Getenv(constants.LogSamplingRateThereafter)
	if rateEnv == "" {
		thereafter = constants.DefaultLogSamplingRateThereafter
	} else {
		parsed, err := strconv.ParseInt(rateEnv, 10, 64)
		if err != nil {
			thereafter = constants.DefaultLogSamplingRateThereafter
		} else {
			thereafter = int(parsed)
		}
	}
	return initial, thereafter
}
