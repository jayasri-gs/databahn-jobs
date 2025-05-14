package vault

import (
	"fmt"
	"github.com/hashicorp/vault/api"
)

func ReadSecrets(vaultAddress string, token string, secretPath string) (map[string]interface{}, error) {
	client, err := api.NewClient(&api.Config{
		Address: vaultAddress,
	})
	if err != nil {
		return nil, err
	}

	client.SetToken(token)

	secret, err := client.Logical().Read(secretPath)
	if err != nil {
		return nil, err
	}

	if secret == nil {
		return nil, fmt.Errorf("no secret found at path: %s", secretPath)
	}

	secretData, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse secret data from Vault")
	}

	return secretData, nil
}
