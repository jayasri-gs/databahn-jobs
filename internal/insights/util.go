package insights

import "fmt"

func InsightId(key1, key2, sourceId string) string {
	return fmt.Sprintf("%s:%s:%s", key1, key2, sourceId)
}

func SightIndexName(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, APP_DEVICEINVENTORY, AGG_SIGHTS, tenantId)
}

func FrequencyIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, appName, AGG_FREQUENCY, tenantId)
}

func SightIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, appName, AGG_SIGHTS, tenantId)
}

func SilentDeviceInventoryHistoryIndex(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, APP_DEVICEINVENTORY, REPUTATION_HISTORY, tenantId)
}
