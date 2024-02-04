package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
)

const AlertIndex = "db_alerts"

type AlertDoc struct {
	Id                      string `json:"id"`
	Title                   string `json:"title"`
	Message                 string `json:"message"`
	CreatedAt               int64  `json:"createdAt"`
	UpdatedAt               int64  `json:"updatedAt"`
	FirstObservedAt         int64  `json:"firstObservedAt"`
	LastObservedAt          int64  `json:"lastObservedAt"`
	TenantId                string `json:"tenantId"`
	FunctionalityType       string `json:"functionalityType"`
	Functionality           string `json:"functionality"`
	FunctionalityEntityId   string `json:"functionalityEntityId"`
	FunctionalityEntityName string `json:"functionalityEntityName"`
	Dismissed               bool   `json:"dismissed"`
	DismissedAt             int64  `json:"dismissedAt"`
	DismissedBy             string `json:"dismissedBy"`
	Criticality             string `json:"criticality"`
}

const script = `
{
  "script": {
    "source": "
      ctx._source.updatedAt = params.updatedAt;
      ctx._source.lastObservedAt = params.lastObservedAt;
      ctx._source.dismissed = params.dismissed;
    ",
    "lang": "painless",
    "params": {
       "updatedAt": {{.UpdatedAt}},
       "lastObservedAt": {{.LastObservedAt}},
       "dismissed": {{.Dismissed}}
    }
  },
  "upsert": {
    "id":  "{{.Id}}",
    "title": "{{.Title}}",
    "message": "{{.Message}}",
    "createdAt": "{{.CreatedAt}}",
    "updatedAt": "{{.UpdatedAt}}",
    "firstObservedAt": "{{.FirstObservedAt}}",
    "lastObservedAt": "{{.LastObservedAt}}",
    "tenantId": "{{.TenantId}}",
    "functionalityType": "{{.FunctionalityType}}",
    "functionality": "{{.Functionality}}",
    "functionalityEntityId": "{{.FunctionalityEntityId}}",
    "functionalityEntityName": "{{.FunctionalityEntityName}}",
    "dismissed": "{{.Dismissed}}",
    "dismissedBy": "{{.DismissedBy}}",
    "criticality": "{{.Criticality}}"
  }
}
`

func SaveAlertToOpenSearch(ctx context.Context, documents []AlertDoc, index string, client *opensearch.Client) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": \"%s\"}}\n", doc.Id)
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
	resp, err := request.Do(ctx, client)
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
			logger.GetLogger().Debug("indexed documents to es", zap.Int("count", len(documents)))
		}
	}
	return nil
}

type EsResp struct {
	Errors bool `json:"errors"`
}
