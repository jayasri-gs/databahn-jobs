package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/jobs"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/google/uuid"
	"strings"
)

func main() {
	ctx := context.Background()
	//job := flag.String("job", "", "job name")
	//input := ReadInputData()
	//flag.Parse()
	//logger.GetLoggerWithContext(ctx).Debug("starting job with parameters", zap.Reflect("input", input))
	config.GetAppConfiguration()
	//cmd.RunJob(ctx, *job, input)
	jobs.AlertForLogSourceInactivity(ctx)
}

func ReadInputData() model.Message {

	var sampleMessage model.Message
	var fileName string

	flag.StringVar(&sampleMessage.RequestId, "reqId", "05101994", "request id ")
	flag.StringVar(&sampleMessage.NewSource, "destination", "out-topic", "destination-topic")
	flag.StringVar(&sampleMessage.BucketName, "bucketName", "db-replay", "bucket name")
	flag.StringVar(&sampleMessage.BucketPrefix, "bucketPrefix", "test/vfs/72767276/2023/12/08/", "bucket Prefix")
	flag.StringVar(&sampleMessage.AccessKeyID, "accessId", "AKIA3FRFSAVQ7REF25F3", "bucket name")
	flag.StringVar(&sampleMessage.SecretAccessKey, "secret", "JgboMDEhSwki1TQlVbt9IDjbnGSb71+AaNx+I1tM", "secret ")
	flag.StringVar(&sampleMessage.Region, "region", "us-east-1", "aws region ")
	flag.StringVar(&fileName, "fileName", "", "files ")

	flag.StringVar(&sampleMessage.Source, "source", "", "Source id ")
	flag.StringVar(&sampleMessage.TenantId, "tenantId", "", "TenantId")
	flag.StringVar(&sampleMessage.DeviceType, "deviceType", "", "deviceType Id")
	flag.StringVar(&sampleMessage.DeviceVendor, "deviceVendor", "", "deviceVendor Id")
	flag.StringVar(&sampleMessage.LogType, "logType", "", "Log type name")
	flag.StringVar(&sampleMessage.FleetId, "fleetId", uuid.Nil.String(), "Fleet ID ")
	flag.StringVar(&sampleMessage.ConnectId, "connectId", uuid.Nil.String(), "aws region ")

	flag.Parse()
	sampleMessage.FileName = strings.Split(fileName, ",")

	return sampleMessage
}
