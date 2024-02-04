package insights

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	opensearch2 "github.com/databahn-ai/databahn-jobs/internal/store/opensearch"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
	"strings"
	"time"
)

const sightsScript = `
{
  "script": {
    "source": "
      if (ctx._source.min_time == null || params.min_time < ctx._source.min_time) {
       ctx._source.min_time = params.min_time;
      }
      if (ctx._source.max_time == null || params.max_time > ctx._source.max_time) {
       ctx._source.max_time = params.max_time;
      }
      ctx._source.source_id = params.source_id; 
      ctx._source.timestamp = params.timestamp;
    ",
    "lang": "painless",
    "params": {
      "key1": "{{.Key1}}",
      "key2": "{{.Key2}}",
      "source_id": "{{.SourceId}}",
      "tenant_id": "{{.TenantId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "timestamp": {{.Timestamp}}
    }
  },
  "upsert": {
      "id": "{{.Id}}",
      "key1": "{{.Key1}}",
      "key2": "{{.Key2}}",
      "tenant_id": "{{.TenantId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "source_id": "{{.SourceId}}",
      "timestamp": {{.Timestamp}},
      "reputation": "` + REPUTATION_NORMAL + `"
    }
}
`

func aggregateInsights(ctx context.Context, cli *opensearch.Client, index IndexMetadata) error {
	indexName := INSIGHTS_STAGING_INDEX_PREFIX + index.String()
	page := 0
	count := 0
	var after *After = nil
	for {
		sourceKey1 := Source{}
		key1Terms := Terms{}
		key1Terms.Terms.Field = "key1"
		sourceKey1.Key1 = &key1Terms
		sourceKey2 := Source{}
		key2Terms := Terms{}
		key2Terms.Terms.Field = "key2"
		sourceKey2.Key2 = &key2Terms
		sourceSourceId := Source{}
		sourceTerms := Terms{}
		sourceTerms.Terms.Field = "source_id"
		sourceSourceId.SourceId = &sourceTerms
		request := Request{}
		request.Aggs.GroupBy.Composite.Size = INSIGHTS_READ_BATCH
		request.Aggs.GroupBy.Composite.After = after
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey1)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey2)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceSourceId)
		request.Aggs.GroupBy.Aggs.PageCnt.Sum.Field = "count"
		request.Aggs.GroupBy.Aggs.PageMnTime.Min.Field = "min_time"
		request.Aggs.GroupBy.Aggs.PageMxTime.Max.Field = "max_time"

		resp, err := opensearch2.MakeSearchCall(ctx, indexName, request, cli)
		if err != nil {
			return err
		}

		bodyContent, _ := io.ReadAll(resp.Body)
		response := &Response{}
		err = json.Unmarshal(bodyContent, response)
		if err != nil {
			return err
		}

		if response.Error.Reason != "" {
			return errors.New(response.Error.Reason)
		}

		count += len(response.Aggregations.GroupBy.Buckets)
		page++
		after = response.Aggregations.GroupBy.AfterKey

		if resp != nil && resp.Body != nil {
			e := resp.Body.Close()
			if e != nil {
				logger.GetLogger().Error("failed to close response body", zap.Error(e))
			}
		}

		if len(response.Aggregations.GroupBy.Buckets) == 0 {
			logger.GetLogger().Debug("no more documents to process", zap.String("index", indexName))
			break
		}

		var docs []Doc
		for _, bucket := range response.Aggregations.GroupBy.Buckets {
			doc := Doc{}
			doc.Key1 = bucket.Key.Key1
			doc.Key2 = bucket.Key.Key2
			doc.Id = InsightId(bucket.Key.Key1, bucket.Key.Key2, bucket.Key.SourceId)
			doc.SourceId = bucket.Key.SourceId
			doc.TenantId = index.TenantId
			doc.Type = index.Type
			doc.MinTime = int64(bucket.PageMnTime.Value)
			doc.MaxTime = int64(bucket.PageMxTime.Value)
			doc.Count = bucket.PageCnt.Value
			doc.Timestamp = time.Now().UnixMilli()
			docs = append(docs, doc)
		}

		err = upsertSightsDocs(ctx, cli, index.TenantId, index.Type, docs)
		if err != nil {
			return err
		}

		err = upsertFrequencyDocs(ctx, cli, &index, docs)
		if err != nil {
			return err
		}

		logger.GetLogger().Debug("processed documents", zap.String("index", indexName), zap.Int("page", page), zap.Int("count", len(docs)))
	}

	logger.GetLogger().Info("processed all documents", zap.String("index", indexName), zap.Int("total_count", count))

	return nil

}

func upsertSightsDocs(ctx context.Context, cli *opensearch.Client, tenantId, app string, documents []Doc) error {
	if len(documents) == 0 {
		return nil
	}
	index := SightIndexNameByApp(app, tenantId)
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": \"%s\"}}\n", doc.Id)
		if err != nil {
			return err
		}
		s := doc.Sight()
		j, err := getUpdateRequestBody(&s)
		if err != nil {
			return err
		}
		buff.Write(j)
		buff.Write([]byte("\n"))
	}

	request := opensearchapi.BulkRequest{
		Index: index,
		Body:  buff,
	}
	err := performBulkRequest(ctx, cli, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("updated documents to es sights", zap.Int("count", len(documents)))
	return nil
}

func upsertFrequencyDocs(ctx context.Context, cli *opensearch.Client, index *IndexMetadata, documents []Doc) error {
	if len(documents) == 0 {
		return nil
	}
	indexName := FrequencyIndexNameByApp(index.Type, index.TenantId)
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		f := doc.Frequency()
		id := buildFrequencyDocId(index, &f)
		_, err := fmt.Fprintf(buff, "{\"index\": {\"_id\": \"%s\"}}\n", id)
		if err != nil {
			return err
		}
		j, err := json.Marshal(f)
		if err != nil {
			return err
		}
		buff.Write(j)
		buff.Write([]byte("\n"))
	}
	request := opensearchapi.BulkRequest{
		Index: indexName,
		Body:  buff,
	}
	err := performBulkRequest(ctx, cli, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("indexed documents to es frequency", zap.Int("count", len(documents)))
	return nil
}

func buildFrequencyDocId(index *IndexMetadata, doc *Frequency) string {
	key := index.String() + doc.Key1 + doc.Key2 + doc.SourceId
	h := sha256.New()
	h.Write([]byte(key))
	id := fmt.Sprintf("%x", h.Sum(nil))
	return id
}

func getUpdateRequestBody(doc *Sight) ([]byte, error) {
	b := strings.ReplaceAll(sightsScript, "\n", " ")
	return utils.ParseTemplate([]byte(b), doc)
}

func performBulkRequest(ctx context.Context, cli *opensearch.Client, request *opensearchapi.BulkRequest) error {
	resp, err := request.Do(ctx, cli)
	if err != nil {
		return err
	} else {
		if resp.IsError() {
			return errors.New(resp.String())
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			bodyJ := EsResp{}
			err = json.Unmarshal(bodyBytes, &bodyJ)
			if err != nil {
				return err
			}
			if bodyJ.Errors {
				return errors.New(string(bodyBytes))
			}
		}
	}
	return nil
}

type EsResp struct {
	Errors bool `json:"errors"`
}

type Terms struct {
	Terms struct {
		Field string `json:"field"`
	} `json:"terms"`
}

type Source struct {
	Key1     *Terms `json:"key1,omitempty"`
	Key2     *Terms `json:"key2,omitempty"`
	SourceId *Terms `json:"source_id,omitempty"`
}

type After struct {
	Key1     string `json:"key1"`
	Key2     string `json:"key2"`
	SourceId string `json:"source_id"`
}

type Request struct {
	Size int `json:"size"`
	Aggs struct {
		GroupBy struct {
			Composite struct {
				Size    int      `json:"size"`
				After   *After   `json:"after,omitempty"`
				Sources []Source `json:"sources"`
			} `json:"composite"`
			Aggs struct {
				PageCnt struct {
					Sum struct {
						Field string `json:"field"`
					} `json:"sum"`
				} `json:"page_cnt"`
				PageMnTime struct {
					Min struct {
						Field string `json:"field"`
					} `json:"min"`
				} `json:"page_mn_time"`
				PageMxTime struct {
					Max struct {
						Field string `json:"field"`
					} `json:"max"`
				} `json:"page_mx_time"`
			} `json:"aggs"`
		} `json:"group_by"`
	} `json:"aggs"`
}

type Response struct {
	opensearch2.ErrorResponse
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
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
		GroupBy struct {
			AfterKey *After `json:"after_key"`
			Buckets  []struct {
				Key struct {
					Key1     string `json:"key1"`
					Key2     string `json:"key2"`
					SourceId string `json:"source_id"`
				} `json:"key"`
				DocCount   int `json:"doc_count"`
				PageMnTime struct {
					Value float64 `json:"value"`
				} `json:"page_mn_time"`
				PageCnt struct {
					Value float64 `json:"value"`
				} `json:"page_cnt"`
				PageMxTime struct {
					Value float64 `json:"value"`
				} `json:"page_mx_time"`
			} `json:"buckets"`
		} `json:"group_by"`
	} `json:"aggregations"`
}

type Doc struct {
	Id        string  `json:"id"`
	Key1      string  `json:"key1"`
	Key2      string  `json:"key2,omitempty"`
	Type      string  `json:"type"`
	InsightId string  `json:"insight_id"`
	SourceId  string  `json:"source_id"`
	TenantId  string  `json:"tenant_id"`
	MinTime   int64   `json:"min_time"`
	MaxTime   int64   `json:"max_time"`
	Count     float64 `json:"count"`
	Timestamp int64   `json:"timestamp"`
}

func (d Doc) Sight() Sight {
	return Sight{
		Id:        d.Id,
		Key1:      d.Key1,
		Key2:      d.Key2,
		Type:      d.Type,
		SourceId:  d.SourceId,
		TenantId:  d.TenantId,
		MinTime:   d.MinTime,
		MaxTime:   d.MaxTime,
		Timestamp: d.Timestamp,
	}
}

func (d Doc) Frequency() Frequency {
	eod := util.GetDayEndTimestamp(d.MaxTime)
	return Frequency{
		Id:              d.Id,
		Key1:            d.Key1,
		Key2:            d.Key2,
		Type:            d.Type,
		SourceId:        d.SourceId,
		TenantId:        d.TenantId,
		Count:           d.Count,
		Timestamp:       d.MaxTime,
		DayEndTimestamp: eod,
	}
}

type Sight struct {
	Id                  string `json:"id"`
	Key1                string `json:"key1"`
	Key2                string `json:"key2,omitempty"`
	Type                string `json:"type"`
	SourceId            string `json:"source_id"`
	TenantId            string `json:"tenant_id"`
	MinTime             int64  `json:"min_time"`
	MaxTime             int64  `json:"max_time"`
	Reputation          string `json:"reputation"`
	Timestamp           int64  `json:"timestamp"`
	ReputationUpdatedAt int64  `json:"reputation_updated_at"`
}

type ReputationUpdateRequest struct {
	Id             string `json:"id"`
	Reputation     string `json:"reputation"`
	SkipReputation string `json:"skip_reputation"`
	UpdatedAt      int64  `json:"updated_at"`
}

func (s Sight) History(time int64, reputation string) SilentDeviceHistory {
	id := fmt.Sprintf("%s:%s:%s:%d", s.Key1, s.Key2, s.SourceId, time)
	return SilentDeviceHistory{
		Id:              id,
		Key1:            s.Key1,
		Key2:            s.Key2,
		Type:            s.Type,
		SourceId:        s.SourceId,
		TenantId:        s.TenantId,
		Reputation:      reputation,
		DayEndTimestamp: time,
	}
}

type SilentDeviceHistory struct {
	Id              string `json:"id"`
	Key1            string `json:"key1"`
	Key2            string `json:"key2,omitempty"`
	Type            string `json:"type"`
	SourceId        string `json:"source_id"`
	TenantId        string `json:"tenant_id"`
	DayEndTimestamp int64  `json:"day_end_timestamp"`
	Reputation      string `json:"reputation"`
}

type Frequency struct {
	Id              string  `json:"id"`
	Key1            string  `json:"key1"`
	Key2            string  `json:"key2"`
	Type            string  `json:"type"`
	SourceId        string  `json:"source_id"`
	TenantId        string  `json:"tenant_id"`
	Count           float64 `json:"count"`
	Timestamp       int64   `json:"timestamp"`
	DayEndTimestamp int64   `json:"day_end_timestamp"`
}
