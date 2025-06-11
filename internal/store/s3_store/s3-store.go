package s3_store

import (
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
	appConfigs "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var client *aws.Client

func Connect() bool {
	s3Mode := appConfigs.GetAppConfiguration().GetString(configuration.S3AccessKeyId)
	if s3Mode == "" {
		logger.GetLogger().Info("s3 artifacts mode not set, selecting default mode")
		s3Mode = "AWS_MODE"
	} else {
		s3Mode = "KEY_BASED_AUTH"
	}
	if client == nil {
		c := aws.Client{
			AuthType: s3Mode,
		}
		endpoint := appConfigs.GetAppConfiguration().GetString(configuration.S3Endpoint)
		if endpoint != "" {
			c.URL = endpoint
			c.InsecureSkipVerify = true
		}
		if appConfigs.GetAppConfiguration().GetBool(configuration.S3ForcePathStyle) {
			c.S3ForcePathStyle = true
		} else {
			c.S3ForcePathStyle = false
		}
		c.Region = appConfigs.GetAppConfiguration().GetString(configuration.Region)
		if appConfigs.GetAppConfiguration().GetString(configuration.S3AccessKeyId) != "" {
			c.AccessKeyID = appConfigs.GetAppConfiguration().GetString(configuration.S3AccessKeyId)
		}
		if appConfigs.GetAppConfiguration().GetString(configuration.S3SecretKey) != "" {
			c.SecretAccessKey = appConfigs.GetAppConfiguration().GetString(configuration.S3SecretKey)
		}
		eerr := c.Connect()
		if eerr != nil {
			logger.GetLogger().Error("error while creating s3 connection", zap.Error(eerr))
			return false
		}

		client = &c

	}
	return true
}

func GetClient() *aws.Client {
	if client != nil {
		return client
	}
	if !Connect() {
		return nil
	}
	return client
}
