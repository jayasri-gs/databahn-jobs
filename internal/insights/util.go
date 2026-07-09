package insights

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

func InsightId(key1, key2, key3, key4, key5, sourceId string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s", url.QueryEscape(key1), url.QueryEscape(key2), url.QueryEscape(key3), url.QueryEscape(key4), url.QueryEscape(key5), sourceId)
}

func SightIndexName(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, AGG_SIGHTS, APP_TYPE_SOURCEHOSTNAME, tenantId)
}

func FrequencyIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, AGG_FREQUENCY, appName, tenantId)
}

func SightIndexNameByApp(appName, tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, AGG_SIGHTS, appName, tenantId)
}

func SilentDeviceInventoryHistoryIndex(tenantId string) string {
	return fmt.Sprintf("%s%s_%s_%s", INSIGHTS_STORE_INDEX_PREFIX, REPUTATION_HISTORY, APP_TYPE_SOURCEHOSTNAME, tenantId)
}

func DeviceIndexName(tenantId string) string {
	return fmt.Sprintf("%sdevices_%s", INSIGHTS_STORE_INDEX_PREFIX, tenantId)
}

func DeviceId(tenantId, key1 string) string {
	return fmt.Sprintf("%s:%s", tenantId, key1)
}

func listSourceHostnameSightsIndices(indices []string) []string {
	prefix := fmt.Sprintf("%s%s_%s_", INSIGHTS_STORE_INDEX_PREFIX, AGG_SIGHTS, APP_TYPE_SOURCEHOSTNAME)
	var sightsIndices []string
	for _, index := range indices {
		if strings.HasPrefix(index, prefix) {
			sightsIndices = append(sightsIndices, index)
		}
	}
	return sightsIndices
}

func tenantIdFromSightsIndex(index string) (string, error) {
	split := strings.Split(index, "_")
	tenantId := split[len(split)-1]
	if _, err := uuid.Parse(tenantId); err != nil {
		return "", fmt.Errorf("invalid tenant id %q in index %q: %w", tenantId, index, err)
	}
	return tenantId, nil
}
