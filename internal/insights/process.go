package insights

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	opensearch2 "github.com/databahn-ai/databahn-jobs/internal/store/opensearch"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
	"strings"
	"time"
)

const script = `
{
  "script": {
    "source": "
      if (ctx._source.min_time == null || params.min_time < ctx._source.min_time) {
       ctx._source.min_time = params.min_time;
      }
      if (ctx._source.max_time == null || params.max_time > ctx._source.max_time) {
       ctx._source.max_time = params.max_time;
      }
      if (ctx._source.count == null)  {
       ctx._source.count = params.count;
      } else {
       ctx._source.count = ctx._source.count + params.count;
      }
      ctx._source.source_id = params.source_id; 
      ctx._source.timestamp = params.timestamp;
    ",
    "lang": "painless",
    "params": {
      "key": "{{.Key}}",
      "source_id": "{{.SourceId}}",
      "tenant_id": "{{.TenantId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "count": {{.Count}},
      "timestamp": {{.Timestamp}}
    }
  },
  "upsert": {
      "key": "{{.Key}}",
      "min_source_id": "{{.SourceId}}",
      "max_source_id": "{{.SourceId}}",
      "tenant_id": "{{.TenantId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "count": {{.Count}},
      "source_id": "{{.SourceId}}",
      "timestamp": {{.Timestamp}}
    }
}
`

func aggregateInsights(ctx context.Context, cli *opensearch.Client, index IndexMetadata) error {
	indexName := common.INSIGHTS_STAGING_INDEX_PREFIX + index.String()
	page := 0
	count := 0
	var after *After = nil

	for {
		sourceKey := Source{}
		keyTerms := Terms{}
		keyTerms.Terms.Field = "key"
		sourceKey.Key = &keyTerms
		sourceSourceId := Source{}
		sourceTerms := Terms{}
		sourceTerms.Terms.Field = "source_id"
		sourceSourceId.SourceId = &sourceTerms
		request := Request{}
		request.Aggs.GroupBy.Composite.Size = common.INSIGHTS_READ_BATCH
		request.Aggs.GroupBy.Composite.After = after
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey)
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
			logger.GetLogger().Info("no more documents to process", zap.String("index", indexName))
			break
		}

		var docs []Doc
		for _, bucket := range response.Aggregations.GroupBy.Buckets {
			doc := Doc{}
			doc.Id = bucket.Key.Key + bucket.Key.SourceId
			doc.Key = bucket.Key.Key
			doc.SourceId = bucket.Key.SourceId
			doc.TenantId = index.TenantId
			doc.MinTime = int64(bucket.PageMnTime.Value)
			doc.MaxTime = int64(bucket.PageMxTime.Value)
			doc.Count = bucket.PageCnt.Value
			doc.Timestamp = time.Now().UnixMilli()
			docs = append(docs, doc)
		}

		err = updateDocs(ctx, cli, index.TenantId, docs)
		if err != nil {
			return err
		}

		logger.GetLogger().Info("processed documents", zap.String("index", indexName), zap.Int("page", page), zap.Int("count", len(docs)))
	}

	logger.GetLogger().Info("processed all documents", zap.String("index", indexName), zap.Int("total_count", count))

	return nil

}

func updateDocs(ctx context.Context, cli *opensearch.Client, tenantId string, documents []Doc) error {
	if len(documents) == 0 {
		return nil
	}
	index := common.INSIGHTS_STORE_INDEX_PREFIX + tenantId

	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": \"%s\"}}\n", doc.Id)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		j, err := getUpdateRequestBody(&doc)
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
			logger.GetLogger().Debug("updated documents to es", zap.Int("count", len(documents)))
		}
	}
	return nil
}

func getUpdateRequestBody(doc *Doc) ([]byte, error) {
	b := strings.ReplaceAll(script, "\n", " ")
	return utils.ParseTemplate([]byte(b), doc)
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
	Key      *Terms `json:"key,omitempty"`
	SourceId *Terms `json:"source_id,omitempty"`
}

type After struct {
	Key      string `json:"key"`
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

type ErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

type Response struct {
	ErrorResponse
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
					Key      string `json:"key"`
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
	Key       string  `json:"key"`
	SourceId  string  `json:"source_id"`
	TenantId  string  `json:"tenant_id"`
	MinTime   int64   `json:"min_time"`
	MaxTime   int64   `json:"max_time"`
	Count     float64 `json:"count"`
	Timestamp int64   `json:"timestamp"`
}
