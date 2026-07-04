package pramaan

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/vault/api"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

/*
VaultPramaan is a utility to start HashiCorp Vault for module testing.
It can start Vault with predefined secrets and policies to create and manage secrets for testing.
You can read, write, and delete secrets with it.
*/
type VaultPramaan struct {
	t           TestLogger
	Container   testcontainers.Container
	ExternalURL string
	NetworkURL  string
	client      *api.Client
	rootToken   string
	unsealKeys  []string
}

const (
	vaultInternalNetworkDns = "pramaanvault"
	vaultPort               = "8200"
)

func NewVaultPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *VaultPramaan {
	// Create Vault container
	vaultContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "hashicorp/vault:1.20.3",
			ExposedPorts: []string{vaultPort + "/tcp"},
			Env: map[string]string{
				"VAULT_DEV_ROOT_TOKEN_ID":  "dev-token",
				"VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:" + vaultPort,
				"VAULT_ADDR":               "http://0.0.0.0:" + vaultPort,
			},
			Networks: []string{dockerNetwork.Name},
			NetworkAliases: map[string][]string{
				dockerNetwork.Name: {vaultInternalNetworkDns},
			},
			WaitingFor: wait.ForLog("Vault server started! Log data will stream in below:"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Could not create vault container %v", err)
	}

	host, err := vaultContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get host: %v", err)
	}

	port, err := vaultContainer.MappedPort(ctx, vaultPort)
	if err != nil {
		log.Fatalf("failed to get port: %v", err)
	}

	externalURL := fmt.Sprintf("http://%s:%d", host, port.Num())
	networkURL := fmt.Sprintf("http://%s:%s", vaultInternalNetworkDns, vaultPort)

	// Create Vault client
	config := api.DefaultConfig()
	config.Address = externalURL
	client, err := api.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create vault client: %v", err)
	}

	// Set the root token for development mode
	client.SetToken("dev-token")

	return &VaultPramaan{
		t:           t,
		Container:   vaultContainer,
		ExternalURL: externalURL,
		NetworkURL:  networkURL,
		client:      client,
		rootToken:   "dev-token",
	}
}

/*
GetClient returns the Vault API client for direct operations.
*/
func (v *VaultPramaan) GetClient() *api.Client {
	return v.client
}

/*
WriteSecret writes a secret to the specified path in Vault.
*/
func (v *VaultPramaan) WriteSecret(ctx context.Context, path string, data map[string]interface{}) error {
	// For KV v2, data needs to be wrapped in a "data" field
	kvData := map[string]interface{}{
		"data": data,
	}
	_, err := v.client.Logical().Write(path, kvData)
	if err != nil {
		return fmt.Errorf("failed to write secret to %s: %v", path, err)
	}
	return nil
}

/*
ReadSecret reads a secret from the specified path in Vault.
*/
func (v *VaultPramaan) ReadSecret(ctx context.Context, path string) (*api.Secret, error) {
	secret, err := v.client.Logical().Read(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read secret from %s: %v", path, err)
	}
	return secret, nil
}

/*
DeleteSecret deletes a secret from the specified path in Vault.
*/
func (v *VaultPramaan) DeleteSecret(ctx context.Context, path string) error {
	_, err := v.client.Logical().Delete(path)
	if err != nil {
		return fmt.Errorf("failed to delete secret from %s: %v", path, err)
	}
	return nil
}

/*
EnableAuthMethod enables an authentication method in Vault.
*/
func (v *VaultPramaan) EnableAuthMethod(ctx context.Context, authType string, options map[string]interface{}) error {
	err := v.client.Sys().EnableAuthWithOptions(authType, &api.EnableAuthOptions{
		Type:        authType,
		Description: options["description"].(string),
	})
	if err != nil {
		return fmt.Errorf("failed to enable auth method %s: %v", authType, err)
	}
	return nil
}

/*
CreatePolicy creates a policy in Vault.
*/
func (v *VaultPramaan) CreatePolicy(ctx context.Context, name, policy string) error {
	err := v.client.Sys().PutPolicy(name, policy)
	if err != nil {
		return fmt.Errorf("failed to create policy %s: %v", name, err)
	}
	return nil
}

/*
CreateToken creates a new token with the specified policies.
*/
func (v *VaultPramaan) CreateToken(ctx context.Context, policies []string) (*api.Secret, error) {
	tokenRequest := &api.TokenCreateRequest{
		Policies: policies,
	}
	secret, err := v.client.Auth().Token().Create(tokenRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to create token: %v", err)
	}
	return secret, nil
}

/*
StorePostgresCredentials stores PostgreSQL credentials in Vault's database secrets engine.
*/
func (v *VaultPramaan) StorePostgresCredentials(ctx context.Context, postgres *PostgresPramaan, secretName string) error {
	// Enable the database secrets engine if not already enabled
	err := v.client.Sys().Mount("database", &api.MountInput{
		Type:        "kv",
		Description: "Database secrets engine for PostgreSQL",
	})
	if err != nil && !isAlreadyMountedError(err) {
		return fmt.Errorf("failed to enable kv secrets engine: %v", err)
	}

	secretData := map[string]interface{}{
		"username": postgres.GetUsername(),
		"password": postgres.GetPassword(),
	}

	err = v.WriteSecret(ctx, secretName, secretData)
	if err != nil {
		return fmt.Errorf("failed to store postgres credentials: %v", err)
	}

	return nil
}

/*
StoreOpenSearchCredentials stores OpenSearch credentials in Vault.
*/
func (v *VaultPramaan) StoreOpenSearchCredentials(ctx context.Context, opensearch *OpenSearchPramaan, secretName string) error {
	err := v.client.Sys().Mount("opensearch", &api.MountInput{
		Type:        "kv",
		Description: "OpenSearch secrets engine",
	})
	if err != nil && !isAlreadyMountedError(err) {
		return fmt.Errorf("failed to enable kv secrets engine: %v", err)
	}
	secretData := map[string]interface{}{
		"open_search.username":            opensearch.GetUsername(),
		"open_search.password":            opensearch.GetPassword(),
		"open_search.statisticsIndexName": "stats",
		"open_search.url":                 opensearch.GetNetworkURL(),
	}

	err = v.WriteSecret(ctx, secretName, secretData)
	if err != nil {
		return fmt.Errorf("failed to store opensearch credentials: %v", err)
	}

	return nil
}

/*
isAlreadyMountedError checks if the error is due to the secrets engine already being mounted.
*/
func isAlreadyMountedError(err error) bool {
	return err != nil && (err.Error() == "path is already in use at database/" ||
		err.Error() == "error enabling database: path is already in use at database/" ||
		err.Error() == "path is already in use at opensearch/" ||
		err.Error() == "error enabling opensearch: path is already in use at opensearch/")
}

/*
GetRootToken returns the root token for the Vault instance.
*/
func (v *VaultPramaan) GetRootToken() string {
	return v.rootToken
}

/*
GetExternalURL returns the external URL for accessing Vault.
*/
func (v *VaultPramaan) GetExternalURL() string {
	return v.ExternalURL
}

/*
GetNetworkURL returns the internal network URL for accessing Vault.
*/
func (v *VaultPramaan) GetNetworkURL() string {
	return v.NetworkURL
}
