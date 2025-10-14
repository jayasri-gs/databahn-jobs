package insights

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	osstore "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

// Default cardinality threshold for tracking unique event keys
const DefaultCardinalityThreshold = 360000

// checkIndexCardinalityAndAlert checks cardinality for a single index and generates alerts if needed
func checkIndexCardinalityAndAlert(ctx context.Context, indexMetadata IndexMetadata, dataPlaneId string, uniqueKeyCount, cardinalityThreshold int) error {
	var insightRuleName string
	var err error

	// Handle logging differently for sourcehostname vs insight rules
	if indexMetadata.Type == APP_TYPE_SOURCEHOSTNAME {
		logger.GetLogger().Info("sourcehostname cardinality check",
			zap.String("type", indexMetadata.Type),
			zap.String("tenant_id", indexMetadata.TenantId),
			zap.String("data_plane_id", dataPlaneId),
			zap.Int("unique_count", uniqueKeyCount),
			zap.Int("threshold", cardinalityThreshold))
		insightRuleName = "" // Not applicable for sourcehostname
	} else {
		// Fetch insight rule name once for both logging and alert generation
		insightRuleName, err = getInsightRuleName(indexMetadata)
		if err != nil {
			// Log with rule ID if we can't fetch the name
			logger.GetLogger().Info("index cardinality check",
				zap.String("insight_rule_id", indexMetadata.Type),
				zap.String("tenant_id", indexMetadata.TenantId),
				zap.String("data_plane_id", dataPlaneId),
				zap.Int("unique_count", uniqueKeyCount),
				zap.Int("threshold", cardinalityThreshold))
			insightRuleName = "" // Set to empty if we couldn't fetch it
		} else {
			logger.GetLogger().Info("insight rule cardinality check",
				zap.String("insight_rule_name", insightRuleName),
				zap.String("insight_rule_id", indexMetadata.Type),
				zap.String("tenant_id", indexMetadata.TenantId),
				zap.String("data_plane_id", dataPlaneId),
				zap.Int("unique_count", uniqueKeyCount),
				zap.Int("threshold", cardinalityThreshold))
		}
	}

	if uniqueKeyCount > cardinalityThreshold {
		return generateIndexCardinalityAlert(ctx, indexMetadata, dataPlaneId, uniqueKeyCount, cardinalityThreshold, insightRuleName)
	}
	return nil
}

// getInsightRuleName fetches the insight rule name from database using index metadata
func getInsightRuleName(indexMetadata IndexMetadata) (string, error) {
	rule := make(map[string]interface{})
	tx := config.GetDB().Raw("select name from insights_rule where tenant_id = ? AND id = ?", indexMetadata.TenantId, indexMetadata.Type).First(&rule)
	if tx.Error != nil {
		return "", fmt.Errorf("failed to fetch insight rule: %w", tx.Error)
	}

	ruleName, ok := rule["name"].(string)
	if !ok {
		return "", fmt.Errorf("rule name not found or invalid type for insight rule ID: %s", indexMetadata.Type)
	}

	return ruleName, nil
}

// todo: Do we need to update acc to key3, key4, key5 ?
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
      ctx._source.updated_at = params.updated_at;
    ",
    "lang": "painless",
    "params": {
      "key1": {{.Key1 | printf "%q"}},
      "key2": {{.Key2 | printf "%q"}},
      "source_id": "{{.SourceId}}",
      "tenant_id": "{{.TenantId}}",
      "data_plane_id": "{{.DataPlaneId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "timestamp": {{.Timestamp}},
      "updated_at": {{.UpdatedAt}}
    }
  },
  "upsert": {
      "id": {{.Id | printf "%q"}},
      "key1": {{.Key1 | printf "%q"}},
      "key2": {{.Key2 | printf "%q"}},
      "tenant_id": "{{.TenantId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "source_id": "{{.SourceId}}",
      "data_plane_id": "{{.DataPlaneId}}",
      "timestamp": {{.Timestamp}},
	  "updated_at": {{.UpdatedAt}},
      "reputation": "` + REPUTATION_NORMAL + `"
    }
}
`

func createSource(key string) Source {
	terms := Terms{}
	terms.Terms.Field = key
	source := Source{}
	switch key {
	case "key1":
		source.Key1 = &terms
	case "key2":
		source.Key2 = &terms
	case "key3":
		source.Key3 = &terms
	case "key4":
		source.Key4 = &terms
	case "key5":
		source.Key5 = &terms
	case "source_id":
		source.SourceId = &terms
	case "data_plane_id":
		source.DataPlaneId = &terms
	}
	return source
}

func aggregateInsights(ctx context.Context, cli *opensearch.Client, index IndexMetadata, sourceIdToNameMap map[string]string) error {
	// Load cardinality threshold from environment variable
	cardinalityThreshold := utils.GetEnvInt("INSIGHTS_CARDINALITY_THRESHOLD", DefaultCardinalityThreshold)

	logger.GetLogger().Info("Insights cardinality threshold configuration loaded",
		zap.Int("cardinality_threshold", cardinalityThreshold))

	indexName := INSIGHTS_STAGING_INDEX_PREFIX + index.String()
	page := 0
	count := 0
	var after *After = nil
	s3FileName := getS3FileName(index)
	s3File, err := os.OpenFile(s3FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	attMap, err := getAttributes(index)
	if err != nil {
		return err
	}
	hasData := false

	// Track unique event keys for this entire index
	indexUniqueKeys := make(map[string]bool)
	var indexDataPlaneId string
	for {
		sourceKey1 := createSource("key1")
		sourceKey2 := createSource("key2")
		sourceKey3 := createSource("key3")
		sourceKey4 := createSource("key4")
		sourceKey5 := createSource("key5")
		sourceSourceId := createSource("source_id")
		sourceDataPlaneId := createSource("data_plane_id")
		request := Request{}
		request.Aggs.GroupBy.Composite.Size = INSIGHTS_READ_BATCH
		request.Aggs.GroupBy.Composite.After = after
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey1)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey2)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey3)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey4)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceKey5)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceSourceId)
		request.Aggs.GroupBy.Composite.Sources = append(request.Aggs.GroupBy.Composite.Sources, sourceDataPlaneId)
		request.Aggs.GroupBy.Aggs.PageCnt.Sum.Field = "count"
		request.Aggs.GroupBy.Aggs.PageMnTime.Min.Field = "min_time"
		request.Aggs.GroupBy.Aggs.PageMxTime.Max.Field = "max_time"

		resp, err := osstore.MakeSearchCall(ctx, indexName+"*", request, cli)
		if err != nil {
			return err
		}
		bodyContent, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return errors.New("failed to get successful response from opensearch, body:" + string(bodyContent))
		}
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
			doc.Key3 = bucket.Key.Key3
			doc.Key4 = bucket.Key.Key4
			doc.Key5 = bucket.Key.Key5
			doc.Id = InsightId(bucket.Key.Key1, bucket.Key.Key2, bucket.Key.Key3, bucket.Key.Key4, bucket.Key.Key5, bucket.Key.SourceId)
			doc.SourceId = bucket.Key.SourceId
			doc.DataPlaneId = bucket.Key.DataPlaneId
			doc.TenantId = index.TenantId
			doc.Type = index.Type
			doc.MinTime = int64(bucket.PageMnTime.Value)
			doc.MaxTime = int64(bucket.PageMxTime.Value)
			doc.Count = bucket.PageCnt.Value
			doc.Timestamp = time.Now().UnixMilli()
			docs = append(docs, doc)

			// Collect unique event key for cardinality tracking
			indexUniqueKeys[doc.Id] = true

			// Capture data plane ID from first document (should be same for entire index)
			if indexDataPlaneId == "" {
				indexDataPlaneId = doc.DataPlaneId
			}
		}

		if index.Type == APP_TYPE_SOURCEHOSTNAME {
			err = upsertSightsDocs(ctx, cli, index.TenantId, index.Type, docs)
			if err != nil {
				return err
			}
		}

		hasData = true
		err = writeToSearchFile(s3File, docs, attMap, sourceIdToNameMap)
		if err != nil {
			return err
		}

		logger.GetLogger().Debug("processed documents", zap.String("index", indexName), zap.Int("page", page), zap.Int("count", len(docs)))
	}
	err = s3File.Close()
	if err != nil {
		return err
	}
	if hasData {
		err = uploadFileToS3ForSearch(ctx, &index, s3File.Name())
		if err != nil {
			return err
		}
	}

	logger.GetLogger().Info("processed all documents", zap.String("index", indexName), zap.Int("total_count", count))

	// Check cardinality for this index and generate alert if needed
	uniqueKeyCount := len(indexUniqueKeys)
	err = checkIndexCardinalityAndAlert(ctx, index, indexDataPlaneId, uniqueKeyCount, cardinalityThreshold)
	if err != nil {
		logger.GetLogger().Error("failed to check index cardinality", zap.Error(err), zap.String("index", indexName))
		// Don't fail the aggregation process for alert failures
	}

	// Clean up cache after index processing
	indexUniqueKeys = nil

	return nil

}

func isValidJSON(data []byte) bool {
	var js interface{}
	return json.Unmarshal(data, &js) == nil
}

func upsertSightsDocs(ctx context.Context, cli *opensearch.Client, tenantId, app string, documents []Doc) error {
	if len(documents) == 0 {
		return nil
	}
	index := SightIndexNameByApp(app, tenantId)
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": %s}}\n", strconv.Quote(doc.Id))
		if err != nil {
			return err
		}
		s := doc.Sight()
		j, err := getUpdateRequestBody(&s)
		if err != nil {
			return err
		}
		if !isValidJSON(j) {
			logger.GetLogger().Error("invalid json for sight doc", zap.String("docId", doc.Id), zap.String("index", index), zap.String("json", string(j)))
			return errors.New("invalid json for sight doc: " + doc.Id + " index: " + index)
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
	Key1        *Terms `json:"key1,omitempty"`
	Key2        *Terms `json:"key2,omitempty"`
	Key3        *Terms `json:"key3,omitempty"`
	Key4        *Terms `json:"key4,omitempty"`
	Key5        *Terms `json:"key5,omitempty"`
	SourceId    *Terms `json:"source_id,omitempty"`
	DataPlaneId *Terms `json:"data_plane_id,omitempty"`
}

type After struct {
	Key1        string `json:"key1"`
	Key2        string `json:"key2"`
	Key3        string `json:"key3"`
	Key4        string `json:"key4"`
	Key5        string `json:"key5"`
	SourceId    string `json:"source_id"`
	DataPlaneId string `json:"data_plane_id"`
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
	osstore.ErrorResponse
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
					Key1        string `json:"key1"`
					Key2        string `json:"key2"`
					Key3        string `json:"key3"`
					Key4        string `json:"key4"`
					Key5        string `json:"key5"`
					SourceId    string `json:"source_id"`
					DataPlaneId string `json:"data_plane_id"`
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
	Id          string  `json:"id"`
	Key1        string  `json:"key1"`
	Key2        string  `json:"key2,omitempty"`
	Key3        string  `json:"key3,omitempty"`
	Key4        string  `json:"key4,omitempty"`
	Key5        string  `json:"key5,omitempty"`
	Type        string  `json:"type"`
	InsightId   string  `json:"insight_id"`
	SourceId    string  `json:"source_id"`
	TenantId    string  `json:"tenant_id"`
	DataPlaneId string  `json:"data_plane_id"`
	MinTime     int64   `json:"min_time"`
	MaxTime     int64   `json:"max_time"`
	Count       float64 `json:"count"`
	Timestamp   int64   `json:"timestamp"`
}

func (d Doc) Sight() Sight {
	return Sight{
		Id:          d.Id,
		Key1:        d.Key1,
		Key2:        d.Key2,
		Type:        d.Type,
		SourceId:    d.SourceId,
		TenantId:    d.TenantId,
		DataPlaneId: d.DataPlaneId,
		MinTime:     d.MinTime,
		MaxTime:     d.MaxTime,
		Timestamp:   d.Timestamp,
		UpdatedAt:   time.Now().UnixMilli(),
	}
}

func (d Doc) Frequency() Frequency {
	eod := util.GetDayEndTimestamp(d.MaxTime)
	return Frequency{
		Id:              d.Id,
		Key1:            d.Key1,
		Key2:            d.Key2,
		Key3:            d.Key3,
		Key4:            d.Key4,
		Key5:            d.Key5,
		Type:            d.Type,
		SourceId:        d.SourceId,
		TenantId:        d.TenantId,
		DataPlaneId:     d.DataPlaneId,
		Count:           d.Count,
		Timestamp:       d.MaxTime,
		DayEndTimestamp: eod,
	}
}

func (d Doc) SearchMap(attMap map[string]string, sourceIdToNameMap map[string]string) map[string]any {
	result := make(map[string]any)
	for k, v := range attMap {
		switch k {
		case "key1":
			result[v] = d.Key1
		case "key2":
			result[v] = d.Key2
		case "key3":
			result[v] = d.Key3
		case "key4":
			result[v] = d.Key4
		case "key5":
			result[v] = d.Key5
		}
	}
	result["source_id"] = d.SourceId
	if sourceName, ok := sourceIdToNameMap[d.SourceId]; ok {
		result["source_name"] = sourceName
	} else {
		result["source_name"] = "unknown_source_name"
	}
	result["timestamp"] = d.Timestamp
	result["count"] = d.Count
	result["id"] = d.Id
	return result
}

func (d Doc) KeyMap() Frequency {
	eod := util.GetDayEndTimestamp(d.MaxTime)
	return Frequency{
		Id:              d.Id,
		Key1:            d.Key1,
		Key2:            d.Key2,
		Key3:            d.Key3,
		Key4:            d.Key4,
		Key5:            d.Key5,
		Type:            d.Type,
		SourceId:        d.SourceId,
		TenantId:        d.TenantId,
		DataPlaneId:     d.DataPlaneId,
		Count:           d.Count,
		Timestamp:       d.MaxTime,
		DayEndTimestamp: eod,
	}
}

type Sight struct {
	Id          string `json:"id"`
	Key1        string `json:"key1"`
	Key2        string `json:"key2,omitempty"`
	Key3        string `json:"key3,omitempty"`
	Key4        string `json:"key4,omitempty"`
	Key5        string `json:"key5,omitempty"`
	Type        string `json:"type"`
	SourceId    string `json:"source_id"`
	TenantId    string `json:"tenant_id"`
	DataPlaneId string `json:"data_plane_id"`
	MinTime     int64  `json:"min_time"`
	MaxTime     int64  `json:"max_time"`
	Reputation  string `json:"reputation"`
	Timestamp   int64  `json:"timestamp"`
	UpdatedAt   int64  `json:"updated_at"`
}

// generateIndexCardinalityAlert creates and sends an alert when unique event key count exceeds threshold for a single index
func generateIndexCardinalityAlert(ctx context.Context, indexMetadata IndexMetadata, dataPlaneId string, uniqueKeyCount, threshold int, insightRuleName string) error {
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to create alerts manager: %w", err)
	}
	defer alertsManager.Close(ctx)

	// Create alert for high cardinality in single index
	alertTime := time.Now()

	// Determine entity ID, title, and message based on data type
	var entityId, title, message, entityName string
	if indexMetadata.Type == APP_TYPE_SOURCEHOSTNAME {
		// For sourcehostname data, use tenant ID as entity ID
		entityId = indexMetadata.TenantId
		title = "High Unique Device Count Detected"
		message = fmt.Sprintf("%d unique devices, exceeding the threshold of %d. This may indicate excessive device diversity or data quality issues in device tracking.",
			uniqueKeyCount, threshold)
		entityName = "sourcehostname"
	} else {
		// For insight rules, use insight rule ID as entity ID
		entityId = indexMetadata.Type
		if insightRuleName != "" {
			title = fmt.Sprintf("High Unique Event Count Detected - Rule %s", insightRuleName)
			message = fmt.Sprintf("Insight rule '%s' generated %d unique events, exceeding the threshold of %d. This may indicate data quality issues or excessive event diversity in this specific rule.",
				insightRuleName, uniqueKeyCount, threshold)
			entityName = fmt.Sprintf("insight rule %s", insightRuleName)
		} else {
			// Fallback if rule name couldn't be fetched
			title = fmt.Sprintf("High Unique Event Count Detected - Rule ID %s", insightRuleName)
			message = fmt.Sprintf("Insight rule Name '%s' generated %d unique events, exceeding the threshold of %d. This may indicate data quality issues or excessive event diversity in this specific rule.",
				insightRuleName, uniqueKeyCount, threshold)
			entityName = fmt.Sprintf("insight rule ID %s", insightRuleName)
		}
	}

	cardinalityAlert, err := alerts_async.NewAlert(
		alerts_async.InsightsRule,
		alerts_async.WithEntityDetails(
			entityId,
			entityName,
			dataPlaneId, // Use the data plane ID from the index
			indexMetadata.TenantId,
		),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.RateLimitExceeded),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, ""),
		alerts_async.WithAction("Please check hostname mapping for the normalization, or incoming data patterns for device inventory alert. For insight rules, please check attributes selected in configuration, those should not have too many unique combinations."),
	)
	if err != nil {
		return fmt.Errorf("failed to create index cardinality alert: %w", err)
	}

	// Set timestamps
	cardinalityAlert.CreatedAt = alertTime.UnixMilli()
	cardinalityAlert.UpdatedAt = alertTime.UnixMilli()
	cardinalityAlert.FirstObservedAt = alertTime.UnixMilli()
	cardinalityAlert.LastObservedAt = alertTime.UnixMilli()

	// Send alert
	err = alertsManager.SendAlerts([]*alerts_async.Alert{cardinalityAlert})
	if err != nil {
		return fmt.Errorf("failed to send index cardinality alert: %w", err)
	}

	// Log success with appropriate context based on type
	if indexMetadata.Type == APP_TYPE_SOURCEHOSTNAME {
		logger.GetLogger().Info("sourcehostname cardinality alert sent successfully",
			zap.String("type", indexMetadata.Type),
			zap.String("tenant_id", indexMetadata.TenantId),
			zap.String("data_plane_id", dataPlaneId),
			zap.Int("unique_count", uniqueKeyCount),
			zap.Int("threshold", threshold),
			zap.String("alert_id", cardinalityAlert.Id))
	} else {
		if insightRuleName != "" {
			logger.GetLogger().Info("insight rule cardinality alert sent successfully",
				zap.String("insight_rule_name", insightRuleName),
				zap.String("insight_rule_id", indexMetadata.Type),
				zap.String("tenant_id", indexMetadata.TenantId),
				zap.String("data_plane_id", dataPlaneId),
				zap.Int("unique_count", uniqueKeyCount),
				zap.Int("threshold", threshold),
				zap.String("alert_id", cardinalityAlert.Id))
		} else {
			logger.GetLogger().Info("insight rule cardinality alert sent successfully",
				zap.String("insight_rule_id", indexMetadata.Type),
				zap.String("tenant_id", indexMetadata.TenantId),
				zap.String("data_plane_id", dataPlaneId),
				zap.Int("unique_count", uniqueKeyCount),
				zap.Int("threshold", threshold),
				zap.String("alert_id", cardinalityAlert.Id))
		}
	}

	return nil
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
		Key3:            s.Key3,
		Key4:            s.Key4,
		Key5:            s.Key5,
		Type:            s.Type,
		SourceId:        s.SourceId,
		TenantId:        s.TenantId,
		DataPlaneId:     s.DataPlaneId,
		Reputation:      reputation,
		DayEndTimestamp: time,
	}
}

type SilentDeviceHistory struct {
	Id              string `json:"id"`
	Key1            string `json:"key1"`
	Key2            string `json:"key2,omitempty"`
	Key3            string `json:"key3,omitempty"`
	Key4            string `json:"key4,omitempty"`
	Key5            string `json:"key5,omitempty"`
	Type            string `json:"type"`
	SourceId        string `json:"source_id"`
	TenantId        string `json:"tenant_id"`
	DataPlaneId     string `json:"data_plane_id"`
	DayEndTimestamp int64  `json:"day_end_timestamp"`
	Reputation      string `json:"reputation"`
}

type Frequency struct {
	Id              string  `json:"id"`
	Key1            string  `json:"key1"`
	Key2            string  `json:"key2"`
	Key3            string  `json:"key3"`
	Key4            string  `json:"key4"`
	Key5            string  `json:"key5"`
	Type            string  `json:"type"`
	SourceId        string  `json:"source_id"`
	TenantId        string  `json:"tenant_id"`
	DataPlaneId     string  `json:"data_plane_id"`
	Count           float64 `json:"count"`
	Timestamp       int64   `json:"timestamp"`
	DayEndTimestamp int64   `json:"day_end_timestamp"`
}
