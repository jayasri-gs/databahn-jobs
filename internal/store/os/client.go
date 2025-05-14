package os

import (
	"fmt"
	osUtils "github.com/databahn-ai/common-utils/opensearch"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

var client *opensearch.Client
var StatsIndex = "db_statistics_"

func GetClient() *opensearch.Client {
	if client == nil {
		c, err := osUtils.Connect(appConfig.GetAppConfiguration())
		if err != nil {
			logger.GetLogger().Error("error while creating opensearch connection", zap.Error(err))
			return nil
		}
		client = c
	}
	return client
}

func StatisticsIndexAlias(tenantId string) string {
	return fmt.Sprintf("%s_alias_%s", StatsIndex, tenantId)
}
