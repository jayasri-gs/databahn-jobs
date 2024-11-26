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
	"strings"
	"time"
)

func GetAllAlertsFromOpenSearch(ctx context.Context, functionalitiesToConsider []string) ([]statistics.AlertDocument, error) {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}

	checkTime := time.Now().Add(-24 * time.Hour)
	q := `updatedAt:>` + strconv.FormatInt(checkTime.UnixMilli(), 10) + ` AND functionalityType:` + "(" + strings.Join(functionalitiesToConsider, " OR ") + ")"

	var allAlerts []statistics.AlertDocument
	var searchAfter []any

	for {
		res, newSearchAfter, err := os.SearchPaginated(ctx, client, common.AlertsIndex, q, 100, searchAfter, []os.Sort{{Field: "updatedAt", Order: "asc"}})
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", common.AlertsIndex))
			return nil, err
		}

		var alerts []statistics.AlertDocument
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
		err = decoder.Decode(res)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
			return allAlerts, err
		}

		allAlerts = append(allAlerts, alerts...)

		if len(res) == 0 || newSearchAfter == nil {
			break
		}

		searchAfter = newSearchAfter
	}
	return allAlerts, nil
}
