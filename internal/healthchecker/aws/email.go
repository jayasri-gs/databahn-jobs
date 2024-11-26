package awsemail

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type Recipient struct {
	To  []string
	CC  []string
	BCC []string
}
type EmailNotification struct {
	Recipients *Recipient
	Body       *string
	Subject    *string
}

var client *ses.Client

func SendEmail(ctx context.Context, emailNotification EmailNotification) error {
	client := getSESClient(config.GetAppConfiguration().GetString(SesEmailRegionsEnvKey))
	if client == nil {
		logging.GetLoggerWithContext(ctx).Debug("error while creating ses client")
		return errors.New("error while creating ses client")
	}
	emailTemplate, err := emailNotification.formEmail(config.GetAppConfiguration().GetString(SesEmailFrom))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while forming email", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Debug("attempting to send email", zap.Any("recipient", emailNotification.Recipients))
	emailOutput, err := client.SendEmail(ctx, emailTemplate)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("send email failed.", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Debug("Email notification sent. ", zap.Stringp("Message ID", emailOutput.MessageId))
	return nil
}

func (emailNotification *EmailNotification) formEmail(senderEmail string) (*ses.SendEmailInput, error) {
	if emailNotification.Body == nil || *emailNotification.Body == "" {
		return nil, errors.New("email body is empty")
	}
	return &ses.SendEmailInput{
		Destination: &types.Destination{
			CcAddresses:  emailNotification.Recipients.CC,
			ToAddresses:  emailNotification.Recipients.To,
			BccAddresses: emailNotification.Recipients.BCC,
		},
		Message: &types.Message{
			Body: &types.Body{
				Html: &types.Content{
					Charset: aws.String(config.GetAppConfiguration().GetString("UTF-8-encoded")),
					Data:    emailNotification.Body,
				},
			},
			Subject: &types.Content{
				//	Charset: aws.String(config.GetAppConfiguration().GetString(constants.EmailDefaultCharset)),
				Data: emailNotification.Subject,
			},
		},
		ReplyToAddresses: []string{DefaultReplyTo},
		Source:           aws.String(senderEmail),
	}, nil
}
func getSESClient(region string) *ses.Client {
	if client == nil {
		cfg, err := awsConfig.LoadDefaultConfig(context.TODO(), awsConfig.WithRegion(region))
		if err != nil {
			logging.GetLogger().Warn("issue getting s3 configuration", zap.Error(err))
			return client
		}
		client = ses.NewFromConfig(cfg)
	}
	return client
}
