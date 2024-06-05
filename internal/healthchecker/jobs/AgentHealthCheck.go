package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

var CheckpointData = make(map[string]string)

func UploadCheckpointFile(bucket string, ctx context.Context) error {

	data, err := json.Marshal(CheckpointData)
	if err != nil {
		return err
	}

	if err != nil && err != io.EOF {
		return err
	}

	s3PutObject := s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("checkpoint-agent/checkpoint.json"),
		Body:   bytes.NewReader(data),
	}
	_, err = aws.UploadFileToS3(ctx, &s3PutObject)
	if err != nil {
		return err
	}
	return nil

}

func DownloadCheckpointFile(bucket string) error {

	fromS3, err := aws.DownloadFileFromS3("checkpoint-agent/checkpoint.json", bucket)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			// Handle NoSuchKwey error
			logging.GetLogger().Info("Error contains NoSuchKey, Meaning its new file")
			return nil
		} else {
			// Handle other errors
			logging.GetLogger().Error("Error:", zap.Error(err))
		}
	}

	err = json.Unmarshal(fromS3, &CheckpointData)
	if err != nil {
		return err
	}
	return nil
}

func AgentHealthChecker(ctx context.Context) error {

	bucket := config.GetAppConfiguration().GetString("s3.artifacts.bucket")
	err := DownloadCheckpointFile(bucket)
	if err != nil {
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("checking agent errors in logs for alerting")
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("agent/windows/logs"),
	}
	// Use the input object in the aws.ListObjects function
	objects, err := aws.ListObjects(ctx, input)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error", zap.Error(err))
	}
	sort.Slice(objects.Contents, func(i, j int) bool {
		return objects.Contents[i].LastModified.After(*objects.Contents[j].LastModified)
	})

	currentHour := time.Now().UTC().Hour() - 1
	for _, object := range objects.Contents {

		objectDay := object.LastModified.UTC().Day()
		currentDay := time.Now().UTC().Day()
		if objectDay == currentDay {
			objectHour := object.LastModified.UTC().Hour()
			if objectHour == currentHour {

				key := *object.Key
				bucketName := *objects.Name
				parts := strings.Split(key, "/")
				if len(parts) < 5 {
					logging.GetLoggerWithContext(ctx).Error("Returning due to Invalid key:", zap.String("key", key), zap.String("bucketName", bucketName))
				}
				tenantId := parts[3]
				agentId := parts[4]
				fileName := parts[5]
				if CheckpointData[agentId] != fileName {
					processLogFiles(ctx, key, bucketName, tenantId, agentId, fileName)
				}

			}
		}
	}

	return nil
}

func AgentAlertForFleetNode(ctx context.Context) error {
	defer logging.GetLogger().Sync()

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for appliances whose health reported is less than 1 hour file, Marking log source as active if it is reporting stats")

	err := AgentHealthChecker(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts for unhealthy fleet nodes", zap.Error(err))
		return err
	}

	return nil
}

func processLogFiles(ctx context.Context, prefix string, bucket string, tenantId string, agentId string, fileName string) {

	fromS3, err := aws.DownloadFileFromS3(prefix, bucket)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error while downloading file from s3", zap.Error(err))
		return
	}

	logging.GetLoggerWithContext(ctx).Info("Processing for Keys:", zap.String("prefix", prefix), zap.String("bucket", bucket), zap.String("tenantId", tenantId), zap.String("agentId", agentId), zap.String("fileName", fileName), zap.String("tenantId", tenantId), zap.String("agentid", agentId), zap.String("fileName", fileName))

	logging.GetLoggerWithContext(ctx).Info("Processing log file", zap.String("tenantId", tenantId), zap.String("agentId", agentId), zap.String("fileName", fileName))
	agentObject := agent.GetFromCache(ctx, utils.UUIDFromStringOrNil(agentId), utils.UUIDFromStringOrNil(tenantId))
	unescapedString, err := strconv.Unquote(string(fromS3))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error(err.Error())
	}
	var agentEntityArray []alerts_common.Alert

	un := strings.Split(unescapedString, "\n")
	for _, u := range un {
		if u == "" {
			continue
		}
		var output map[string]interface{}
		err = json.Unmarshal([]byte(u), &output)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error(err.Error())
			continue
		}
		if output == nil {
			continue
		}
		if output["level"] == "error" || output["level"] == "warn" || output["msg"] == "fluent-bit started" {
			logging.GetLoggerWithContext(ctx).Info(" Data:  ", zap.String("AgentName", agentObject.Name), zap.String("AgentName", agentObject.Name), zap.String("tenantId", tenantId), zap.String("agentId", agentId), zap.Reflect("output", output))
			logMessage := fmt.Sprintf(" AgentName: %s, TenantId: %s, AgentId: %s, Output: %v", agentObject.Name, tenantId, agentId, output)
			logging.GetLoggerWithContext(ctx).Info(logMessage)
			var temp alerts_common.Alert
			temp.ID = uuid.New()
			temp.Criticality = alerts_common.CriticalAlert
			temp.Title = "Agent Node Health Data Reporting : " + agentObject.Name
			temp.Message = logMessage
			temp.CreatedAt = time.Now()
			temp.UpdatedAt = time.Now()
			temp.FirstObservedAt = time.Now()
			temp.LastObservedAt = time.Now()
			temp.TenantUUID = utils.UUIDFromStringOrNil(tenantId)
			temp.Functionality = "Agent Node"
			temp.FunctionalityEntityId = agentObject.ID.String()
			temp.FunctionalityEntityName = agentObject.Name
			temp.FunctionalityType = "Agent"
			temp.Status = 1
			temp.UpdatedBy = "system"
			temp.Dismissed = false

			agentEntityArray = append(agentEntityArray, temp)
		}
	}
	// raise alert and save it to opensearch
	err = helper.SendToNotificationTopic(ctx, agentEntityArray)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert Agent Node inactivity", zap.Error(err))
	}
	CheckpointData[agentId] = fileName
	err = UploadCheckpointFile(bucket, ctx)
	if err != nil {
		return
	}

}
