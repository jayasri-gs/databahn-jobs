package aws

import (
	"context"
	"regexp"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"go.uber.org/zap"
)

type CloudWatchClient struct {
	Region string
	Client *cloudwatchlogs.CloudWatchLogs
}

// GetRecentCWErrorLogs finds the most recent logs stream in logGroup, then searches the most recent
func GetRecentCWErrorLogs(ctx context.Context, logGroup string, region string, logsToSearch int64) (errorLogs []string, err error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	cw := cloudwatchlogs.New(sess)
	logGroupPrefix := "/aws/lambda/" + logGroup
	// Ignoring error here - it's logged downstream. If we don't get logs, just alert without logs.
	lsName, _ := getLatestLogStreamName(ctx, sess, logGroupPrefix)

	logs, err := cw.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &logGroupPrefix,
		LogStreamName: &lsName,
		Limit:         &logsToSearch,
	})
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error getting logs", zap.Error(err))
		return errorLogs, err
	}
	resultRegex := "(error|panic)"
	regex, err := regexp.Compile(resultRegex)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error compiling compliance target regex", zap.Error(err), zap.String("regex", resultRegex))
		return errorLogs, err
	}
	for i := range logs.Events {
		matchTarget := []byte(*logs.Events[i].Message)
		if regex.Match(matchTarget) {
			maybePrettyString, err := utils.MakeJSONStringpretty(*logs.Events[i].Message)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Info("error making json eventlog pretty. Make them look upon the ugly instead!", zap.Error(err),
					zap.String("Message", *logs.Events[i].Message))
				maybePrettyString = *logs.Events[i].Message
			}
			errorLogs = append(errorLogs, maybePrettyString)
		}
	}
	return errorLogs, nil
}

func NewCloudWatchClient(region string) *CloudWatchClient {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return &CloudWatchClient{
		Region: region,
		Client: cloudwatchlogs.New(sess),
	}
}

func (c *CloudWatchClient) Fetch(ctx context.Context, input *cloudwatchlogs.GetLogEventsInput) (*cloudwatchlogs.GetLogEventsOutput, error) {
	return c.Client.GetLogEventsWithContext(ctx, input)
}

func (c *CloudWatchClient) FetchWithFilter(ctx context.Context, logGroupName, logStreamName string, startTime *int64) (*cloudwatchlogs.GetLogEventsOutput, error) {
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
		StartTime:     startTime,
		Limit:         aws.Int64(50),
	}
	return c.Client.GetLogEventsWithContext(ctx, input)
}

func (c *CloudWatchClient) FetchWithToken(ctx context.Context, logGroupName, logStreamName, nextToken string) (*cloudwatchlogs.GetLogEventsOutput, error) {
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
		NextToken:     aws.String(nextToken),
		Limit:         aws.Int64(50),
	}
	return c.Client.GetLogEventsWithContext(ctx, input)
}

func getLatestLogStreamName(ctx context.Context, sess *session.Session, logGroupPrefix string) (string, error) {
	cw := cloudwatchlogs.New(sess)
	descendingSort := true
	dls := cloudwatchlogs.DescribeLogStreamsInput{
		Descending: &descendingSort,
		// LogStreamNamePrefix: ,
		LogGroupName: &logGroupPrefix,
	}
	dlo, err := cw.DescribeLogStreams(&dls)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error fetching log streams",
			zap.String("log stream prefix", logGroupPrefix), zap.Error(err))
		return "", err
	}
	if dlo.LogStreams == nil || len(dlo.LogStreams) == 0 {
		logger.GetLoggerWithContext(ctx).Error("No cloudwatch log stream names found for log stream prefix",
			zap.String("log stream prefix", logGroupPrefix))
		return "", err
	}
	return *dlo.LogStreams[0].LogStreamName, nil
}
