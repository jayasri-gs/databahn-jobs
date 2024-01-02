package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/cmd"
)

func main() {
	job := flag.String("job", "", "job name")
	flag.Parse()

	ctx := context.Background()
	cmd.RunJob(ctx, *job)
}
