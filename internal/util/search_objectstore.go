package util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsecrets "github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	commonutilsaws "github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/objectstore"
	"github.com/databahn-ai/common-utils/vault"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
)

var (
	searchObjectStoreClient     objectstore.ObjectStore
	searchObjectStoreCollection string
)

// AwsSearchSecret is the search secret shape for S3 (flat keys).
type AwsSearchSecret struct {
	AccessKeyID     string `json:"search.access_key_id"`
	SecretAccessKey string `json:"search.secret_access_key"`
	Bucket          string `json:"search.bucket"`
}

// BlobSearchSecret is the blob section of the Azure search secret (object storage for search).
type BlobSearchSecret struct {
	AccountKey  string `json:"account_key"`
	Container   string `json:"container"`
	AccountName string `json:"account_name"`
}

// azureSearchSecretPayload matches the full Azure search secret JSON (search.synapse + search.blob).
type azureSearchSecretPayload struct {
	Search struct {
		Synapse struct {
			SQLUsername string `json:"sql_username"`
			SQLPassword string `json:"sql_password"`
			SASToken    string `json:"sas_token"`
		} `json:"synapse"`
		Blob BlobSearchSecret `json:"blob"`
	} `json:"search"`
}

// readSearchSecretRaw reads the search secret as raw JSON from the configured secret backend (secret.backend).
// Supports AWS Secrets Manager, Vault, and Azure Key Vault.
func readSearchSecretRaw(ctx context.Context, cfg configuration.ConfigReader, secretName string) ([]byte, error) {
	secretBackend := cfg.GetString(configuration.SecretBackend)
	if secretBackend == "" {
		logger.GetLoggerWithContext(ctx).Info("No secret backend configured, using default")
		secretBackend = configuration.SecretBackendAWS
	}

	switch secretBackend {
	case configuration.SecretBackendAWS:
		region := cfg.GetString(configuration.Region)
		if region == "" {
			region = os.Getenv("AWS_REGION")
		}
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
		if region == "" {
			region = "us-east-1"
		}
		data, err := commonutilsaws.ReadSecretByName(secretName, region)
		if err != nil {
			return nil, fmt.Errorf("reading search secret from AWS: %w", err)
		}
		return []byte(*data.SecretString), nil

	case configuration.SecretBackendVault:
		vaultAddress := cfg.GetString(configuration.VaultAddress)
		vaultToken := cfg.GetString(configuration.VaultToken)
		if vaultAddress == "" || vaultToken == "" {
			return nil, fmt.Errorf("vault address or token not set in config")
		}
		path := strings.TrimPrefix(secretName, "vault://")
		secret, err := vault.ReadSecrets(vaultAddress, vaultToken, path)
		if err != nil {
			return nil, fmt.Errorf("reading search secret from Vault: %w", err)
		}
		raw, err := json.Marshal(secret)
		if err != nil {
			return nil, fmt.Errorf("marshaling Vault secret: %w", err)
		}
		return raw, nil

	case configuration.SecretBackendAzure:
		vaultURL := cfg.GetString(configuration.AzureInfraKeyVaultUrl)
		if vaultURL == "" {
			return nil, fmt.Errorf("missing config %s", configuration.AzureInfraKeyVaultUrl)
		}
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("Azure credential: %w", err)
		}
		client, err := azsecrets.NewClient(vaultURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("Azure Key Vault client: %w", err)
		}
		resp, err := client.GetSecret(ctx, secretName, "", nil)
		if err != nil {
			return nil, fmt.Errorf("reading search secret from Azure Key Vault: %w", err)
		}
		if resp.Value == nil {
			return nil, fmt.Errorf("secret value is nil in Azure Key Vault response")
		}
		return []byte(*resp.Value), nil

	default:
		return nil, fmt.Errorf("unsupported secret backend: %s (use %s, %s, or %s)",
			secretBackend, configuration.SecretBackendAWS, configuration.SecretBackendVault, configuration.SecretBackendAzure)
	}
}

// ReadSearchObjectStoreSecret reads the search secret using secret.backend and unmarshals into the concrete type for object.backend.
// objectBackend is "s3" or "blob". For "s3" returns AwsSearchSecret; for "blob" returns the blob part of the Azure secret.
// Exactly one of s3Secret and blobSecret will be non-nil on success.
func ReadSearchObjectStoreSecret(ctx context.Context, cfg configuration.ConfigReader, secretName, objectBackend string) (s3Secret *AwsSearchSecret, blobSecret *BlobSearchSecret, err error) {
	raw, err := readSearchSecretRaw(ctx, cfg, secretName)
	if err != nil {
		return nil, nil, err
	}
	b := strings.ToLower(strings.TrimSpace(objectBackend))
	switch b {
	case "blob":
		var payload azureSearchSecretPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, nil, err
		}
		return nil, &payload.Search.Blob, nil
	case "s3": // s3
		var s AwsSearchSecret
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, nil, err
		}
		return &s, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported object backend: %s (use s3 or blob)", b)
	}
}

// newSearchObjectStoreOverlay builds a config overlay: base app config with object.*
// and object.events.collection overridden from the search secret. The result can be
// passed to objectstore.NewObjectStore.
func newSearchObjectStoreOverlay(ctx context.Context, base configuration.ConfigReader) (configuration.ConfigReader, error) {
	secretName := base.GetString(configuration.SearchSecretNameKey)
	objectBackend := base.GetString(configuration.ObjectBackend)
	s3Sec, blobSec, err := ReadSearchObjectStoreSecret(ctx, base, secretName, objectBackend)
	if err != nil {
		return nil, fmt.Errorf("reading search object store secret: %w", err)
	}

	overrides := make(map[string]interface{})
	backend := strings.ToLower(strings.TrimSpace(objectBackend))

	switch backend {
	case "blob":
		if blobSec == nil {
			return nil, fmt.Errorf("search secret did not contain blob credentials")
		}
		overrides[configuration.ObjectBlobAccountName] = blobSec.AccountName
		overrides[configuration.ObjectBlobAccountKey] = blobSec.AccountKey
	case "s3": // s3
		if s3Sec == nil {
			return nil, fmt.Errorf("search secret did not contain S3 credentials")
		}
		overrides[configuration.ObjectS3AccessKey] = s3Sec.AccessKeyID
		overrides[configuration.ObjectS3SecretKey] = s3Sec.SecretAccessKey
	default:
		return nil, fmt.Errorf("unsupported object backend: %s (use s3 or blob)", backend)
	}

	if s3Sec != nil {
		searchObjectStoreCollection = s3Sec.Bucket
	} else if blobSec != nil {
		searchObjectStoreCollection = blobSec.Container
	} else {
		return nil, fmt.Errorf("search secret did not contain any credentials")
	}

	return &overlayConfigReader{base: base, overrides: overrides}, nil
}

// _loadSearchObjectStore builds an overlay of app config with secret-derived object store
// fields and passes it to objectstore.NewObjectStore.
func _loadSearchObjectStore(ctx context.Context) error {
	base := appConfig.GetAppConfiguration()
	overlay, err := newSearchObjectStoreOverlay(ctx, base)
	if err != nil {
		return err
	}
	store, err := objectstore.NewObjectStore(ctx, overlay)
	if err != nil {
		return fmt.Errorf("creating search object store: %w", err)
	}
	searchObjectStoreClient = store
	return nil
}

func _getSearchObjectStore(ctx context.Context) (objectstore.ObjectStore, string, error) {
	if searchObjectStoreClient == nil {
		if err := _loadSearchObjectStore(ctx); err != nil {
			return nil, "", err
		}
	}
	return searchObjectStoreClient, searchObjectStoreCollection, nil
}

func UploadFileToObjectStore(ctx context.Context, objectKey, filePath string) error {
	client, container, err := _getSearchObjectStore(ctx)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return client.Put(ctx, container, objectKey, data)
}

func UploadGzipFileToObjectStore(ctx context.Context, objectKey, filePath string) error {
	client, container, err := _getSearchObjectStore(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return client.PutStream(ctx, container, objectKey, file, &objectstore.PutOptions{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
	})
}
