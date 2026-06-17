package destination

import (
	"context"
	"encoding/json"
	"fmt"

	dbaws "github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type ConfigWrapper struct {
	SecretID      string            `json:"secretId"`
	Configuration map[string]string `json:"configuration"`
}

func (w ConfigWrapper) getString(key string) string {
	if w.Configuration == nil {
		return ""
	}
	return w.Configuration[key]
}

// ResolveCredentialOverrides loads credential fields from Secrets Manager when a destination references a secret.
//
//nolint:gosec // CWE-532 false positive: credentials are returned to callers, never logged
func ResolveCredentialOverrides(ctx context.Context, db *gorm.DB, secretRefID string, destID, tenantID uuid.UUID) (map[string]string, error) {
	var backendSecretID string
	err := db.WithContext(ctx).Raw(
		"SELECT backend_secret_id FROM secrets WHERE id = ? LIMIT 1",
		secretRefID,
	).Scan(&backendSecretID).Error
	if err != nil || backendSecretID == "" {
		return nil, fmt.Errorf("failed to look up backend_secret_id: secret reference not found or inaccessible (destination=%s, tenant=%s)", destID, tenantID)
	}

	smOutput, err := dbaws.ReadSecretByName(backendSecretID, appConfig.GetAppConfiguration().GetString("region"))
	if err != nil {
		return nil, fmt.Errorf("failed to read secret from AWS Secrets Manager (destination=%s, tenant=%s)", destID, tenantID)
	}

	if smOutput.SecretString == nil {
		return nil, nil
	}

	var credentialFields map[string]string
	if err := json.Unmarshal([]byte(*smOutput.SecretString), &credentialFields); err != nil {
		return nil, fmt.Errorf("failed to parse secret value: malformed JSON (destination=%s, tenant=%s)", destID, tenantID)
	}
	return credentialFields, nil
}
