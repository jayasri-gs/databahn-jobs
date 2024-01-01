package main

import (
	"context"
	"github.com/databahn-ai/databhn-jobs/cmd"
	"github.com/databahn-ai/databhn-jobs/internal/common"
)

func main() {
	ctx := context.Background()
	cmd.RunJob(ctx, common.INSIGHTS_AGGREGATION)
}
