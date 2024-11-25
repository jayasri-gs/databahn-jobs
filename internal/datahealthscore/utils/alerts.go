package utils

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"strconv"
	"time"
)

func GetAllAlertsFromOpenSearch(ctx context.Context) ([]statistics.AlertDocument, error) {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}

	checkTime := time.Now().Add(-24 * time.Hour)

	q := `updatedAt >= ` + strconv.FormatInt(checkTime.UnixMilli(), 10)

	res, err := os.Search(ctx, client, common.AlertsIndex, q)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", common.AlertsIndex))
		return nil, err
	}

	var alerts []statistics.AlertDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	err = decoder.Decode(res)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
		return alerts, err
	}
	return alerts, nil
}
