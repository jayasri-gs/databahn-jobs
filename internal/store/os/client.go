package os

import (
	"crypto/tls"
	"fmt"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"net/http"
)

var client *opensearch.Client
var StatsIndex = "db_statistics_"

func GetClient() *opensearch.Client {
	if client == nil {
		newClient, err := opensearch.NewClient(opensearch.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Addresses: []string{"https://localhost:9200"},
			Username:  "admin",
			Password:  "admin",
		})
		if err != nil {
			logger.GetLogger().Panic("error while creating opensearch connection", zap.Error(err))
			return nil
		}
		return newClient
		//c, err := osUtils.Connect(appConfig.GetAppConfiguration())
		//if err != nil {
		//	logger.GetLogger().Error("error while creating opensearch connection", zap.Error(err))
		//	return nil
		//}
		//client = c
	}
	return client
}

func StatisticsIndexAlias(tenantId string) string {
	return fmt.Sprintf("%salias_%s", StatsIndex, tenantId)
}
