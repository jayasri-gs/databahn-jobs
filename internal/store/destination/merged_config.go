package destination

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// LoadMergedConfiguration returns inline destination configuration merged with secret overrides.
func LoadMergedConfiguration(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (map[string]string, error) {
	var dest struct {
		Configuration string    `gorm:"column:configuration"`
		TenantID      uuid.UUID `gorm:"column:tenant_id"`
	}
	err := db.WithContext(ctx).Raw(
		"SELECT configuration, tenant_id FROM destination WHERE id = ? AND tenant_id = ? LIMIT 1",
		destID, tenantID,
	).Scan(&dest).Error
	if err != nil {
		return nil, fmt.Errorf("destination not found: %w", err)
	}
	if dest.Configuration == "" {
		return nil, fmt.Errorf("destination configuration is empty for %s", destID)
	}

	var wrapper ConfigWrapper
	if err := json.Unmarshal([]byte(dest.Configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	merged := cloneStringMap(wrapper.Configuration)
	if wrapper.SecretID != "" {
		overrides, err := ResolveCredentialOverrides(ctx, db, wrapper.SecretID, destID, dest.TenantID)
		if err != nil {
			return nil, err
		}
		merged = mergeStringMaps(merged, overrides)
	}
	return merged, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeStringMaps(base, overlay map[string]string) map[string]string {
	out := cloneStringMap(base)
	for k, v := range overlay {
		out[k] = v
	}
	return out
}
