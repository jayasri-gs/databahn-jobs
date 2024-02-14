package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/cmd"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"strings"
)

func main() {
	ctx := context.Background()
	//job := flag.String("job", "", "job name")
	job := common.LOG_SOURCE_ACTIVITY_CHECKER
	//input := ReadInputData()
	//flag.Parse()
	//logger.GetLoggerWithContext(ctx).Debug("starting job with parameters", zap.Reflect("input", input))
	config.GetAppConfiguration()
	config.GetDB()
	var sampleMessage model.Message
	cmd.RunJob(ctx, job, sampleMessage)
}

func ReadInputData() model.Message {

	var sampleMessage model.Message
	var fileName string

	flag.StringVar(&sampleMessage.RequestId, "reqId", "05101994", "request id ")
	flag.StringVar(&sampleMessage.Destination, "destination", "out-topic", "destination-topic")
	flag.StringVar(&sampleMessage.BucketName, "bucketName", "db-replay", "bucket name")
	flag.StringVar(&sampleMessage.BucketPrefix, "bucketPrefix", "test/vfs/72767276/2023/12/08/", "bucket Prefix")
	flag.StringVar(&sampleMessage.AccessKeyID, "accessId", "AKIA3FRFSAVQ7REF25F3", "bucket name")
	flag.StringVar(&sampleMessage.SecretAccessKey, "secret", "JgboMDEhSwki1TQlVbt9IDjbnGSb71+AaNx+I1tM", "secret ")
	flag.StringVar(&sampleMessage.Region, "region", "us-east-1", "aws region ")
	flag.StringVar(&fileName, "fileName", "", "files ")
	flag.Parse()
	sampleMessage.FileName = strings.Split(fileName, ",")

	return sampleMessage
}
