package insights

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/util"
)

// getObjectKeyForSearch returns the storage path for aggregated insight data (used for both S3 and Azure Blob).
func getObjectKeyForSearch(index *IndexMetadata, fileName string) string {
	fileBaseName := filepath.Base(fileName)
	return fmt.Sprintf("tenant_id=%s/insight_rule_id=%s/year=%04d/month=%02d/date=%02d/%s", index.TenantId, index.Type, index.Year, index.Month, index.Day, fileBaseName)
}

// uploadAggregatedInsightFile uploads the aggregated insight file to object storage (S3 or Azure Blob) using the shared objstore client.
func uploadAggregatedInsightFile(ctx context.Context, index *IndexMetadata, fileName string) error {
	objectKey := getObjectKeyForSearch(index, fileName)
	return util.UploadFileToObjectStore(ctx, objectKey, fileName)
}

func writeToSearchFile(file *os.File, docs []Doc, attMap map[string]string, sourceIdToNameMap map[string]string) error {
	for _, doc := range docs {
		searchMap := doc.SearchMap(attMap, sourceIdToNameMap)
		j, err := json.Marshal(searchMap)
		if err != nil {
			return err
		}
		if _, err := file.Write(j); err != nil {
			return err
		}
		if _, err := file.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func getAttributes(index IndexMetadata) (map[string]string, error) {
	if index.Type == APP_TYPE_SOURCEHOSTNAME {
		return map[string]string{
			"key1": "sourcehostname",
		}, nil
	}
	rule := make(map[string]interface{})
	tx := appConfig.GetDB().Raw("select * from insights_rule where tenant_id = ? AND id = ?", index.TenantId, index.Type).First(&rule)
	if tx.Error != nil {
		return nil, tx.Error
	}
	attColumn, ok := rule["attributes"].(string)
	if !ok {
		return nil, errors.New("attributes column type mismatch")
	}
	attColumnJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(attColumn), &attColumnJson)
	if err != nil {
		return nil, err
	}
	attributeList, ok := attColumnJson["attributes"].([]any)
	if !ok {
		return nil, errors.New("attributes column value mismatch")
	}
	attributes := make(map[string]string)
	for i := 0; i < len(attributeList); i++ {
		attributes["key"+strconv.Itoa(i+1)] = fmt.Sprintf("%s", attributeList[i])
	}
	return attributes, nil
}

func getSearchFileName(index IndexMetadata, ext string) string {
	return os.TempDir() + "/" + index.String() + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + ext
}
