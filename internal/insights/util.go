package insights

import (
	"fmt"
	"net/url"
)

func InsightId(key1, key2, key3, key4, key5, sourceId string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s", url.QueryEscape(key1), url.QueryEscape(key2), url.QueryEscape(key3), url.QueryEscape(key4), url.QueryEscape(key5), sourceId)
}

func InsightId2(key1, key2, sourceId string) string {
	return fmt.Sprintf("%s:%s:%s", url.QueryEscape(key1), url.QueryEscape(key2), sourceId)
}

func SightIndexName(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, APP_TYPE_SOURCEHOSTNAME, AGG_SIGHTS, tenantId)
}

func FrequencyIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, appName, AGG_FREQUENCY, tenantId)
}

func SightIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, appName, AGG_SIGHTS, tenantId)
}

func SilentDeviceInventoryHistoryIndex(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, APP_TYPE_SOURCEHOSTNAME, REPUTATION_HISTORY, tenantId)
}
