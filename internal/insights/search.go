package insights

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type AwsSearchSecret struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Bucket          string `json:"bucket"`
}

var s3Client *s3.Client
var awsSecret *AwsSearchSecret

func _loadS3ClientAndSecret(ctx context.Context) error {
	secretName := appConfig.GetAppConfiguration().GetString("search.secret_name")
	region := appConfig.GetAppConfiguration().GetString("region")
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return err
	}
	awsSecret = &AwsSearchSecret{}
	err = json.Unmarshal([]byte(*data.SecretString), &awsSecret)
	if err != nil {
		return err
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsSecret.AccessKeyID, awsSecret.SecretAccessKey, "")),
	)
	if err != nil {
		return err
	}
	s3Client = s3.NewFromConfig(cfg)
	return nil
}

func _getSearchS3Client(ctx context.Context) (*s3.Client, error) {
	if s3Client == nil {
		err := _loadS3ClientAndSecret(ctx)
		if err != nil {
			return nil, err
		}
	}
	return s3Client, nil
}

func uploadFileToS3ForSearch(ctx context.Context, index *IndexMetadata, fileName string) error {
	fileBaseName := filepath.Base(fileName)
	objectKey := fmt.Sprintf("tenant_id=%s/insight_rule_id=%s/year=%04d/month=%02d/date=%02d/%s", index.TenantId, index.Type, index.Year, index.Month, index.Day, fileBaseName)
	return _uploadFileToS3(ctx, objectKey, fileName)
}

func writeToSearchFile(file *os.File, docs []Doc, attMap map[string]string, sourceIdToNameMap map[string]string) error {
	for _, doc := range docs {
		searchMap := doc.SearchMap(attMap, sourceIdToNameMap)
		j, err := json.Marshal(searchMap)
		if err != nil {
			return err
		}
		if _, err := file.Write(j); err != nil {
			return err
		}
		if _, err := file.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func getAttributes(index IndexMetadata) (map[string]string, error) {
	if index.Type == APP_TYPE_SOURCEHOSTNAME {
		return map[string]string{
			"key1": "sourcehostname",
		}, nil
	}
	rule := make(map[string]interface{})
	tx := appConfig.GetDB().Raw("select * from insights_rule where tenant_id = ? AND id = ?", index.TenantId, index.Type).First(&rule)
	if tx.Error != nil {
		return nil, tx.Error
	}
	attColumn, ok := rule["attributes"].(string)
	if !ok {
		return nil, errors.New("attributes column type mismatch")
	}
	attColumnJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(attColumn), &attColumnJson)
	if err != nil {
		return nil, err
	}
	attributeList, ok := attColumnJson["attributes"].([]any)
	if !ok {
		return nil, errors.New("attributes column value mismatch")
	}
	attributes := make(map[string]string)
	for i := 0; i < len(attributeList); i++ {
		attributes["key"+strconv.Itoa(i+1)] = fmt.Sprintf("%s", attributeList[i])
	}
	return attributes, nil
}

func getS3FileName(index IndexMetadata) string {
	return os.TempDir() + "/" + index.String() + ".txt"
}

func _uploadFileToS3(ctx context.Context, objectKey, filePath string) error {
	client, err := _getSearchS3Client(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	buffer, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	s3PutObject := s3.PutObjectInput{
		Bucket: aws.String(awsSecret.Bucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(buffer),
	}
	_, err = client.PutObject(ctx, &s3PutObject)
	if err != nil {
		return err
	}
	return nil
}
