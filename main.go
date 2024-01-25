package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/cmd"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strings"
)

func main() {
	ctx := context.Background()
	job := flag.String("job", "", "job name")
	input := ReadInputData()
	//flag.Parse()
	logger.GetLoggerWithContext(ctx).Debug("starting job with parameters", zap.Reflect("input", input))

	cmd.RunJob(ctx, *job, input)
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
