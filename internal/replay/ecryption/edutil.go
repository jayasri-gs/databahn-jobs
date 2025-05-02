package ecryption

import (
	"fmt"

	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/db-models/changeflag"
)

func DecryptKeys(input *model.Message) error {
	secretManagerKey := input.AdditionalConfig["secret_id"]
	// keys are not encrypted
	if secretManagerKey == "" {
		return nil
	}

	tenantIdSecretIdMap := map[string][]string{
		input.TenantId: {secretManagerKey},
	}
	secrets, err := changeflag.LoadSecrets(appConfig.GetAppConfiguration(), tenantIdSecretIdMap)
	if err != nil {
		err = fmt.Errorf("error while fetching secrets from aws secret manager: %v", err)
		return err
	}

	for _, secretData := range secrets {
		if len(secretData.Errors) != 0 {
			for _, errMsg := range secretData.Errors {
				err = fmt.Errorf("error while fetching secrets from aws secret manager: %v", errMsg)
				return err
			}
		}
		for _, secretMap := range secretData.Secrets {
			for secretMapKey, secretMapValue := range secretMap {
				input.AdditionalConfig[secretMapKey] = secretMapValue
			}
		}
	}
	return nil
}
