package main

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/cmd"
	"github.com/databahn-ai/databahn-jobs/internal/common"
)

func main() {
	ctx := context.Background()
	cmd.RunJob(ctx, common.INSIGHTS_AGGREGATION)
}
