package insights

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type IndexMetadata struct {
	Version  string
	Year     int
	Month    int
	Day      int
	Hour     int
	TenantId string
	Type     string
}

func (m IndexMetadata) String() string {
	return fmt.Sprintf("%s_%s_%04d_%02d_%02d_%02d_%s", m.Version, m.Type, m.Year, m.Month, m.Day, m.Hour, m.TenantId)
}

func (m IndexMetadata) IsBefore(t time.Time) bool {
	if m.Year < t.Year() {
		return true
	} else if m.Year > t.Year() {
		return false
	} else {
		if m.Month < int(t.Month()) {
			return true
		} else if m.Month > int(t.Month()) {
			return false
		} else {
			if m.Day < t.Day() {
				return true
			} else if m.Day > t.Day() {
				return false
			} else {
				if m.Hour < t.Hour() {
					return true
				} else if m.Hour > t.Hour() {
					return false
				} else {
					return false
				}
			}
		}
	}
}

func parseIndices(indexNames []string) []IndexMetadata {
	var indices []IndexMetadata
	for _, name := range indexNames {
		data := strings.ReplaceAll(name, INSIGHTS_STAGING_INDEX_PREFIX, "")
		splitBy := strings.Split(data, "_")
		if len(splitBy) > 0 {
			version := splitBy[0]
			switch version {
			case "v1":
				if len(splitBy) == 7 {
					tp := splitBy[1]
					year, err := strconv.ParseInt(splitBy[2], 10, 64)
					if err != nil {
						logger.GetLogger().Error("failed to parse index name year", zap.String("indexName", name))
						continue
					}
					month, err := strconv.ParseInt(splitBy[3], 10, 64)
					if err != nil {
						logger.GetLogger().Error("failed to parse index name month", zap.String("indexName", name))
						continue
					}
					day, err := strconv.ParseInt(splitBy[4], 10, 64)
					if err != nil {
						logger.GetLogger().Error("failed to parse index name day", zap.String("indexName", name))
						continue
					}
					hour, err := strconv.ParseInt(splitBy[5], 10, 64)
					if err != nil {
						logger.GetLogger().Error("failed to parse index name hour", zap.String("indexName", name))
						continue
					}
					tenant := splitBy[6]
					m := IndexMetadata{
						Version:  version,
						Year:     int(year),
						Month:    int(month),
						Day:      int(day),
						Hour:     int(hour),
						TenantId: tenant,
						Type:     tp,
					}
					indices = append(indices, m)
				} else {
					logger.GetLogger().Error("failed to parse index name of v1 version", zap.String("indexName", name))
				}
			default:
				logger.GetLogger().Error("failed to parse index name version not supported", zap.String("indexName", name))
			}
		} else {
			logger.GetLogger().Error("failed to parse index name not able to split", zap.String("indexName", name))
		}
	}
	return indices
}
