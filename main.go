package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/cmd"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"net/http"
	"strings"
)

func main() {
	job := flag.String("job", "", "job name")
	input := ReadInputData()
	//flag.Parse()

	ctx := context.Background()
	go cmd.RunJob(ctx, *job, input)

	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		render.Status(r, http.StatusOK)
		render.PlainText(w, r, "healthy")
		return
	})
	err := http.ListenAndServe(":8080", r)
	if err != nil {
		panic(err)
	}
	//config.GetAppConfiguration()
	//config.GetDB()
	//jobs.AlertForLogSourceInactivity(context.Background())
}

func ReadInputData() model.Message {

	var sampleMessage model.Message

	flag.StringVar(&sampleMessage.RequestId, "reqId", "05101994", "request id ")
	flag.StringVar(&sampleMessage.Destination, "destination", "out-topic", "destination-topic")
	flag.StringVar(&sampleMessage.BucketName, "bucketName", "db-replay", "bucket name")
	flag.StringVar(&sampleMessage.BucketPrefix, "bucketPrefix", "test/vfs/72767276/2023/12/08/", "bucket Prefix")
	flag.StringVar(&sampleMessage.AccessKeyID, "accessId", "AKIA3FRFSAVQ7REF25F3", "bucket name")
	flag.StringVar(&sampleMessage.SecretAccessKey, "secret", "JgboMDEhSwki1TQlVbt9IDjbnGSb71+AaNx+I1tM", "secret ")
	flag.StringVar(&sampleMessage.Region, "region", "us-east-1", "aws region ")
	fileName := *flag.String("fileName", "test1.log.gz,test30.log.gz", "files ")
	flag.Parse()
	sampleMessage.FileName = strings.Split(fileName, ",")

	return sampleMessage
}
