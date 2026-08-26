package stats

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type OlderRolloverConfig struct {
	weeksOlderThan int
	specificIndex  string
	skipIndices    []string
}

type RolloverConfig struct {
	olderRolloverConfig           *OlderRolloverConfig
	parallelism                   int
	limit                         int
	aggBatchSize                  int
	aggQueryRange                 time.Duration
	validationRange               time.Duration
	aggWindow                     time.Duration
	s3BackupEnabled               bool
	deleteExistingRolledOverIndex bool
	skipValidation                bool
	skipTenantIdValidation        bool
	specificTenants               string
}

type IndexSchema string

const Schema_V1 = IndexSchema("v1")
const Schema_V2 = IndexSchema("v2")
const Schema_V3_temp = IndexSchema("v3")

type IndexLifeCyclePhase string

const Phase_P0 = IndexLifeCyclePhase("p0")
const Phase_P1 = IndexLifeCyclePhase("p1")
const Phase_P2 = IndexLifeCyclePhase("p2")
const Phase_P3 = IndexLifeCyclePhase("p3")

func (p IndexLifeCyclePhase) next() (IndexLifeCyclePhase, error) {
	if p == Phase_P1 {
		return Phase_P2, nil
	} else if p == Phase_P2 {
		return Phase_P3, nil
	}
	return "", errors.New("invalid phase")
}

type IndexLifeCycleMigration string

const Migrate_P1_P2 = IndexLifeCycleMigration("p1_p2")
const Migrate_P2_P3 = IndexLifeCycleMigration("p2_p3")
const Migrate_P3_P4 = IndexLifeCycleMigration("p3_p4")

type Index struct {
	Index  string
	Tenant string
	Year   int
	Week   int
	Day    int
	Schema IndexSchema
	Phase  IndexLifeCyclePhase
}

type timeRange struct {
	start int64
	end   int64
}

func (in Index) aliasName() string {
	return fmt.Sprintf("db_statistics_alias_%s", in.Tenant)
}

func (in Index) newIndexNameForRollover(duration time.Duration) (string, error) {
	strDur := util.FormatDuration(duration)
	if in.Schema == Schema_V1 {
		name := fmt.Sprintf("rolled_over_%s_db_statistics", strDur)
		return strings.ReplaceAll(in.Index, "db_statistics", name), nil
	} else if in.Schema == Schema_V2 || in.Schema == Schema_V3_temp {
		nextPhase, err := in.Phase.next()
		if err != nil {
			return "", err
		}
		name := fmt.Sprintf("rolled_over_%s_db_statistics", strDur)
		oldPhaseStr := fmt.Sprintf("_%s_%s_", in.Schema, in.Phase)
		newPhaseStr := fmt.Sprintf("_%s_%s_", Schema_V2, nextPhase)
		//_v2_p1 -> _v2_p2_, _v3_p1 -> _v2_p2_
		newIndexName := strings.ReplaceAll(in.Index, oldPhaseStr, newPhaseStr)
		return strings.ReplaceAll(newIndexName, "db_statistics", name), nil
	} else {
		return "", errors.New("invalid schema while building new index name")
	}
}

func (in Index) s3FileName(suffix string) string {
	return os.TempDir() + "/" + in.Index + "_" + suffix + ".txt.gz"
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false

}

func parseIndexName(index string) (*Index, bool) {
	split := strings.Split(index, "_")
	if len(split) == 5 {
		year, err := strconv.Atoi(split[3])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[3]))
			return nil, false
		}
		week, err := strconv.Atoi(split[4])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, false
		}
		tenantId := split[2]
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week, Schema: Schema_V1, Phase: Phase_P1}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, false
		}
	} else if len(split) == 6 {
		year, err := strconv.Atoi(split[4])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
			return nil, false
		}
		week, err := strconv.Atoi(split[5])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, false
		}
		tenantId := split[2]
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week, Schema: Schema_V1, Phase: Phase_P1}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, false
		}
	} else if len(split) == 7 {
		schema := split[3]
		indexSchema := IndexSchema(schema)
		if Schema_V2 == indexSchema || Schema_V3_temp == indexSchema {
			phase := split[4]
			tenantId := split[2]
			yYear := split[5]
			if !strings.HasPrefix(yYear, "y") {
				logger.GetLogger().Error("stats index name parsing: unexpected year format", zap.String("year", yYear), zap.String("index", index))
				return nil, false
			}
			year, err := strconv.Atoi(yYear[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
				return nil, false
			}
			dDay := split[6]
			if !strings.HasPrefix(dDay, "d") {
				logger.GetLogger().Error("stats index name parsing: unexpected day format", zap.String("day", dDay), zap.String("index", index))
				return nil, false
			}
			day, err := strconv.Atoi(dDay[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting day", zap.Error(err), zap.String("index", index), zap.String("day", split[6]))
				return nil, false
			}
			indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Day: day, Schema: indexSchema, Phase: IndexLifeCyclePhase(phase)}
			if indexToRollover.Year != 1970 {
				return &indexToRollover, true
			} else {
				logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
				return nil, false
			}
		} else {
			logger.GetLogger().Error("stats index name parsing: unexpected schema", zap.String("schema", schema), zap.String("index", index))
			return nil, false
		}
	} else if len(split) == 8 {
		schema := split[3]
		indexSchema := IndexSchema(schema)
		if Schema_V2 == indexSchema {
			phase := split[4]
			tenantId := split[2]
			yYear := split[6]
			if !strings.HasPrefix(yYear, "y") {
				logger.GetLogger().Error("stats index name parsing: unexpected year format", zap.String("year", yYear), zap.String("index", index))
				return nil, false
			}
			year, err := strconv.Atoi(yYear[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
				return nil, false
			}
			dDay := split[7]
			if !strings.HasPrefix(dDay, "d") {
				logger.GetLogger().Error("stats index name parsing: unexpected day format", zap.String("day", dDay), zap.String("index", index))
				return nil, false
			}
			day, err := strconv.Atoi(dDay[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting day", zap.Error(err), zap.String("index", index), zap.String("day", split[6]))
				return nil, false
			}
			indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Day: day, Schema: indexSchema, Phase: IndexLifeCyclePhase(phase)}
			if indexToRollover.Year != 1970 {
				return &indexToRollover, true
			} else {
				logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
				return nil, false
			}
		} else {
			logger.GetLogger().Error("stats index name parsing: unexpected schema", zap.String("schema", schema), zap.String("index", index))
			return nil, false
		}
	} else {
		logger.GetLogger().Error("stats index name parsing: unexpected index name format", zap.String("index", index))
		return nil, false
	}
}

func parseRolledOverP3OrOlderIndexName(index string) (*Index, bool) {
	split := strings.Split(index, "_")
	if !strings.HasPrefix(index, "rolled_over") {
		return nil, false
	}
	if strings.Contains(index, "_p4_") {
		return nil, false
	}
	if len(split) == 8 { //rolled_over_1m_db_statistics_085f5671-8243-470c-af44-fb0125ffd092_2025_14
		tenantId := split[5]
		year, err := strconv.Atoi(split[6])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
			return nil, false
		}
		week, err := strconv.Atoi(split[7])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, false
		}
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week, Schema: Schema_V1, Phase: Phase_P0}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, false
		}
	} else if len(split) == 9 { //rolled_over_1m_db_statistics_085f5671-8243-470c-af44-fb0125ffd092_v3_2025_16
		tenantId := split[5]
		year, err := strconv.Atoi(split[7])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
			return nil, false
		}
		week, err := strconv.Atoi(split[8])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, false
		}
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week, Schema: Schema_V1, Phase: Phase_P0}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, false
		}
	} else if len(split) == 10 { //rolled_over_1d_db_statistics_v2_p3_15ec2c52-c786-400f-b312-23f7c5d22f09_y2025_w24
		schema := split[5] //v2
		phase := split[6]  //p3
		tenantId := split[7]
		indexSchema := IndexSchema(schema)
		indexPhase := IndexLifeCyclePhase(phase)
		if indexSchema == Schema_V2 && indexPhase == Phase_P3 {
			yYear := split[8] //y2025
			if !strings.HasPrefix(yYear, "y") {
				logger.GetLogger().Error("stats index name parsing: unexpected year format", zap.String("year", yYear), zap.String("index", index))
				return nil, false
			}
			year, err := strconv.Atoi(yYear[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", yYear))
				return nil, false
			}
			dWeek := split[9] //w28
			if !strings.HasPrefix(dWeek, "w") {
				logger.GetLogger().Error("stats index name parsing: unexpected week format", zap.String("week", dWeek), zap.String("index", index))
				return nil, false
			}
			week, err := strconv.Atoi(dWeek[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", dWeek))
				return nil, false
			}
			indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week, Schema: indexSchema, Phase: IndexLifeCyclePhase(phase)}
			if indexToRollover.Year != 1970 {
				return &indexToRollover, true
			} else {
				logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
				return nil, false
			}
		}
	}
	logger.GetLogger().Debug("v2 or older stats index name parsing: unexpected index name format", zap.String("index", index))
	return nil, false
}

func parseRolledOverV2P2IndexName(index string) (*Index, bool) {
	split := strings.Split(index, "_")
	if !strings.HasPrefix(index, "rolled_over") {
		return nil, false
	}
	if len(split) == 10 {
		schema := split[6]
		phase := split[7]
		indexSchema := IndexSchema(schema)
		if Schema_V2 == indexSchema && Phase_P2 == IndexLifeCyclePhase(phase) {
			tenantId := split[5]
			yYear := split[8]
			if !strings.HasPrefix(yYear, "y") {
				logger.GetLogger().Error("stats index name parsing: unexpected year format", zap.String("year", yYear), zap.String("index", index))
				return nil, false
			}
			year, err := strconv.Atoi(yYear[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", yYear))
				return nil, false
			}
			dDay := split[9]
			if !strings.HasPrefix(dDay, "d") {
				logger.GetLogger().Error("stats index name parsing: unexpected day format", zap.String("day", dDay), zap.String("index", index))
				return nil, false
			}
			day, err := strconv.Atoi(dDay[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting day", zap.Error(err), zap.String("index", index), zap.String("day", dDay))
				return nil, false
			}
			indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Day: day, Schema: indexSchema, Phase: IndexLifeCyclePhase(phase)}
			if indexToRollover.Year != 1970 {
				return &indexToRollover, true
			} else {
				logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
				return nil, false
			}
		} else {
			return nil, false
		}
	} else if len(split) == 11 {
		schema := split[6]
		phase := split[7]
		indexSchema := IndexSchema(schema)
		if Schema_V2 == indexSchema && Phase_P2 == IndexLifeCyclePhase(phase) {
			tenantId := split[5]
			yYear := split[9]
			if !strings.HasPrefix(yYear, "y") {
				logger.GetLogger().Error("stats index name parsing: unexpected year format", zap.String("year", yYear), zap.String("index", index))
				return nil, false
			}
			year, err := strconv.Atoi(yYear[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
				return nil, false
			}
			dDay := split[10]
			if !strings.HasPrefix(dDay, "d") {
				logger.GetLogger().Error("stats index name parsing: unexpected day format", zap.String("day", dDay), zap.String("index", index))
				return nil, false
			}
			day, err := strconv.Atoi(dDay[1:])
			if err != nil {
				logger.GetLogger().Error("stats index name parsing: error while converting day", zap.Error(err), zap.String("index", index), zap.String("day", split[6]))
				return nil, false
			}
			indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Day: day, Schema: indexSchema, Phase: IndexLifeCyclePhase(phase)}
			if indexToRollover.Year != 1970 {
				return &indexToRollover, true
			} else {
				return nil, false
			}
		} else {
			return nil, false
		}
	} else {
		return nil, false
	}
}

func getWeekOfYear() (int, int) {
	year, week := time.Now().ISOWeek()
	return year, week
}

func getYearAndDay() (int, int) {
	now := time.Now()
	year := now.Year()
	day := now.YearDay()
	return year, day
}

func yearWeekNumber(year, week int) int {
	return year*100 + week
}

func yearDayNumber(year, day int) int {
	return year*1000 + day
}

func splitYearWeekNumber(yearWeek int) (int, int) {
	year := yearWeek / 100
	week := yearWeek % 100
	return year, week
}

func yearWeekFromYearDay(year, dayOfYear int) (int, int) {
	startOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	date := startOfYear.AddDate(0, 0, dayOfYear-1)
	year, week := date.ISOWeek()
	return year, week
}

type After struct {
	Name                 string `json:"name"`
	Namespace            string `json:"namespace"`
	SourceId             string `json:"source_id"`
	DestinationId        string `json:"destination_id"`
	RuleId               string `json:"rule_id"`
	FleetNodeId          string `json:"fleet_node_id"`
	OperatorId           string `json:"operator_id"`
	TargetNodeId         string `json:"target_node_id"`
	RouteKey             string `json:"route_key"`
	TimeHistogramBuckets int64  `json:"time_histogram_buckets"`
}

type RequestSourceTermsAgg struct {
	Terms struct {
		Script ScripTerms `json:"script"`
	} `json:"terms"`
}

func newRequestSourceTermsAgg(field string) RequestSourceTermsAgg {
	script := fmt.Sprintf("if ((!doc.containsKey('%s')) || doc['%s'].size() == 0) { return 'N/A'; } else { return doc['%s'].value; }", field, field, field)
	terms := ScripTerms{
		Source: script,
		Lang:   "painless",
	}
	agg := RequestSourceTermsAgg{}
	agg.Terms.Script = terms
	return agg

}

type ScripTerms struct {
	Source string `json:"source"`
	Lang   string `json:"lang"`
}

type RequestSourceTimeHistogramBuckets struct {
	DateHistogram struct {
		Field         string `json:"field"`
		FixedInterval string `json:"fixed_interval"`
	} `json:"date_histogram"`
}

type RequestSource struct {
	Name                 *RequestSourceTermsAgg             `json:"name,omitempty"`
	Namespace            *RequestSourceTermsAgg             `json:"namespace,omitempty"`
	SourceId             *RequestSourceTermsAgg             `json:"source_id,omitempty"`
	DestinationId        *RequestSourceTermsAgg             `json:"destination_id,omitempty"`
	RuleId               *RequestSourceTermsAgg             `json:"rule_id,omitempty"`
	FleetNodeId          *RequestSourceTermsAgg             `json:"fleet_node_id,omitempty"`
	OperatorId           *RequestSourceTermsAgg             `json:"operator_id,omitempty"`
	TargetNodeId         *RequestSourceTermsAgg             `json:"target_node_id,omitempty"`
	RouteKey             *RequestSourceTermsAgg             `json:"route_key,omitempty"`
	TimeHistogramBuckets *RequestSourceTimeHistogramBuckets `json:"time_histogram_buckets,omitempty"`
}

type TopHits struct {
	Source string `json:"_source"`
	Size   int    `json:"size"`
}

type Sum struct {
	Field string `json:"field"`
}

type RolloverAggRequest struct {
	Size  int `json:"size"`
	Query struct {
		Range struct {
			TagsDbTsWin struct {
				Gte int64 `json:"gte"`
				Lt  int64 `json:"lt"`
			} `json:"tags.db_ts_win"`
		} `json:"range"`
	} `json:"query"`
	Aggs struct {
		CompositeBuckets struct {
			Composite struct {
				After   *After          `json:"after,omitempty"`
				Size    int             `json:"size"`
				Sources []RequestSource `json:"sources"`
			} `json:"composite"`
			Aggregations struct {
				AllFields struct {
					TopHits TopHits `json:"top_hits"`
				} `json:"all_fields"`
				TotalCount struct {
					Sum Sum `json:"sum"`
				} `json:"total_count"`
			} `json:"aggregations"`
		} `json:"composite_buckets"`
	} `json:"aggs"`
}

type Error struct {
	RootCause []struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"root_cause"`
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type EsDoc struct {
	Index  string   `json:"_index"`
	Id     string   `json:"_id"`
	Score  float64  `json:"_score"`
	Source EsSource `json:"_source"`
}

type EsSource struct {
	AggKey    string `json:"agg_key"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	WindowKey string `json:"window_key"`
	Counter   struct {
		Value any `json:"value"`
	} `json:"counter"`
	Id        string         `json:"id"`
	Tags      map[string]any `json:"tags"`
	Timestamp any            `json:"timestamp"`
}

type Key struct {
	Name                 string `json:"name"`
	Namespace            string `json:"namespace"`
	SourceId             string `json:"source_id"`
	DestinationId        string `json:"destination_id"`
	RuleId               string `json:"rule_id"`
	FleetNodeId          string `json:"fleet_node_id"`
	OperatorId           string `json:"operator_id"`
	TargetNodeId         string `json:"target_node_id"`
	RouteKey             string `json:"route_key"`
	TimeHistogramBuckets int64  `json:"time_histogram_buckets"`
}

func (k Key) newDocKey(srcIndexName string) string {
	val := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%d", k.Name, k.Namespace, k.SourceId, k.DestinationId, k.RuleId, k.FleetNodeId, k.OperatorId, k.TargetNodeId, k.RouteKey, srcIndexName, k.TimeHistogramBuckets)
	return util.Hash(val)
}

type RolloverAggResponse struct {
	Error    *Error `json:"error"`
	Status   int    `json:"status"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore interface{}   `json:"max_score"`
		Hits     []interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		CompositeBuckets struct {
			AfterKey *After `json:"after_key"`
			Buckets  []struct {
				Key       Key `json:"key"`
				DocCount  int `json:"doc_count"`
				AllFields struct {
					Hits struct {
						Total struct {
							Value    int    `json:"value"`
							Relation string `json:"relation"`
						} `json:"total"`
						MaxScore float64 `json:"max_score"`
						Hits     []EsDoc `json:"hits"`
					} `json:"hits"`
				} `json:"all_fields"`
				TotalCount struct {
					Value any `json:"value"`
				} `json:"total_count"`
			} `json:"buckets"`
		} `json:"composite_buckets"`
	} `json:"aggregations"`
}

type MinMaxRequest struct {
	Size int `json:"size"`
	Aggs struct {
		MinVal struct {
			Min struct {
				Field string `json:"field"`
			} `json:"min"`
		} `json:"min_val"`
		MaxVal struct {
			Max struct {
				Field string `json:"field"`
			} `json:"max"`
		} `json:"max_val"`
	} `json:"aggs"`
}

type MinMaxResponse struct {
	Error        *Error `json:"error"`
	Status       int    `json:"status"`
	Aggregations struct {
		MaxVal struct {
			Value float64 `json:"value"`
		} `json:"max_val"`
		MinVal struct {
			Value float64 `json:"value"`
		} `json:"min_val"`
	} `json:"aggregations"`
}
