package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type blobBackend struct {
	client       *azblob.Client
	accountName  string // stored for user delegation SAS URL generation
	hasSharedKey bool   // true if client was created with shared key (supports service SAS)
}

// NewBlobBackend creates an Azure Blob Storage-backed ObjectStore.
// Supports three auth modes:
//  1. connectionString: full Azure Storage connection string (or AZURE_STORAGE_CONNECTION_STRING env)
//  2. accountName + accountKey: shared key auth
//  3. accountName only (no accountKey): uses native auth (DefaultAzureCredential:
//     managed identity, Azure CLI, service principal env vars, etc.)
//
// When connectionString/accountName not provided, falls back to AZURE_STORAGE_CONNECTION_STRING
// and AZURE_STORAGE_ACCOUNT environment variables.
//
// GetPresignedURL works with all three auth modes:
//   - For connection string and shared key auth, it uses Service SAS.
//   - For AAD auth, it uses User Delegation SAS which requires the identity to have
//     "Storage Blob Data Contributor" or "Storage Blob Delegator" role.
func NewBlobBackend(ctx context.Context, connectionString, accountName, accountKey string) (ObjectStore, error) {
	var client *azblob.Client
	var hasSharedKey bool
	var err error

	// Fallback to standard Azure env vars when not provided
	if connectionString == "" {
		connectionString = os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	}
	if accountName == "" {
		accountName = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}

	if connectionString != "" {
		client, err = azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("failed to create blob client from connection string", zap.Error(err))
			return nil, fmt.Errorf("failed to create blob client from connection string: %w", err)
		}
		hasSharedKey = true
	} else if accountName != "" && accountKey != "" {
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
		sharedCred, credErr := azblob.NewSharedKeyCredential(accountName, accountKey)
		if credErr != nil {
			logging.GetLoggerWithContext(ctx).Error("failed to create shared key credential", zap.Error(credErr))
			return nil, fmt.Errorf("failed to create shared key credential: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, sharedCred, nil)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("failed to create blob client with shared key credential", zap.Error(err))
			return nil, fmt.Errorf("failed to create blob client with shared key credential: %w", err)
		}
		hasSharedKey = true
	} else if accountName != "" {
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("failed to create default azure credential", zap.Error(err))
			return nil, fmt.Errorf("failed to create default azure credential: %w", err)
		}
		client, err = azblob.NewClient(serviceURL, cred, nil)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("failed to create blob client", zap.Error(err))
			return nil, fmt.Errorf("failed to create blob client: %w", err)
		}
	} else {
		return nil, fmt.Errorf("blob backend requires connection_string, or account_name with optional account_key")
	}

	return &blobBackend{
		client:       client,
		accountName:  accountName,
		hasSharedKey: hasSharedKey,
	}, nil
}

// NewBlobBackendWithClient creates an Azure Blob-backed ObjectStore with an existing client.
// accountName is required for User Delegation SAS URL generation (AAD auth).
// hasSharedKey should be true if the client was created with shared key credentials,
// false if created with AAD auth (managed identity, service principal, etc.).
func NewBlobBackendWithClient(client *azblob.Client, accountName string, hasSharedKey bool) ObjectStore {
	return &blobBackend{
		client:       client,
		accountName:  accountName,
		hasSharedKey: hasSharedKey,
	}
}

func (b *blobBackend) Get(ctx context.Context, container, key string) ([]byte, error) {
	downloadResp, err := b.client.DownloadStream(ctx, container, key, nil)
	if err != nil {
		return nil, fmt.Errorf("blob download: %w", err)
	}

	var buf bytes.Buffer
	retryReader := downloadResp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	_, err = buf.ReadFrom(retryReader)
	_ = retryReader.Close()
	if err != nil {
		return nil, fmt.Errorf("blob read body: %w", err)
	}
	return buf.Bytes(), nil
}

func (b *blobBackend) Put(ctx context.Context, container, key string, data []byte) error {
	_, err := b.client.UploadBuffer(ctx, container, key, data, &azblob.UploadBufferOptions{})
	if err != nil {
		return fmt.Errorf("blob upload: %w", err)
	}
	return nil
}

func (b *blobBackend) PutStream(ctx context.Context, container, key string, reader io.Reader) error {
	_, err := b.client.UploadStream(ctx, container, key, reader, &azblob.UploadStreamOptions{})
	if err != nil {
		return fmt.Errorf("blob upload stream: %w", err)
	}
	return nil
}

func (b *blobBackend) Post(ctx context.Context, container, key string, data []byte) error {
	// Azure Blob block blobs don't support native append; Post is equivalent to Put
	return b.Put(ctx, container, key, data)
}

func (b *blobBackend) Delete(ctx context.Context, container, key string) error {
	_, err := b.client.DeleteBlob(ctx, container, key, nil)
	if err != nil {
		return fmt.Errorf("blob delete: %w", err)
	}
	return nil
}

func (b *blobBackend) List(ctx context.Context, container, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	pager := b.client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("blob list objects: %w", err)
		}
		if page.Segment == nil {
			continue
		}
		for _, blob := range page.Segment.BlobItems {
			info := ObjectInfo{}
			if blob.Name != nil {
				info.Key = *blob.Name
			}
			if blob.Properties != nil && blob.Properties.LastModified != nil {
				info.LastModified = *blob.Properties.LastModified
			}
			objects = append(objects, info)
		}
	}
	return objects, nil
}

func (b *blobBackend) GetPresignedURL(ctx context.Context, container, key string, expiry time.Duration) (string, error) {
	// Use service SAS if client has shared key (faster, no extra API call)
	if b.hasSharedKey {
		blobClient := b.client.ServiceClient().NewContainerClient(container).NewBlobClient(key)
		permissions := sas.BlobPermissions{Read: true}
		expiryTime := time.Now().Add(expiry)

		sasURL, err := blobClient.GetSASURL(permissions, expiryTime, nil)
		if err != nil {
			return "", fmt.Errorf("blob get SAS URL: %w", err)
		}
		return sasURL, nil
	}

	// Fall back to User Delegation SAS (requires AAD auth - managed identity, service principal, etc.)
	return b.getUserDelegationSASURL(ctx, container, key, expiry)
}

// getUserDelegationSASURL generates a SAS URL using User Delegation Key (for AAD auth).
// Requires the identity to have "Storage Blob Data Contributor" or "Storage Blob Delegator" role.
func (b *blobBackend) getUserDelegationSASURL(ctx context.Context, container, key string, expiry time.Duration) (string, error) {
	now := time.Now().UTC()
	expiryTime := now.Add(expiry)

	keyInfo := service.KeyInfo{
		Start:  to(now.Format(sas.TimeFormat)),
		Expiry: to(expiryTime.Format(sas.TimeFormat)),
	}

	udc, err := b.client.ServiceClient().GetUserDelegationCredential(ctx, keyInfo, nil)
	if err != nil {
		return "", fmt.Errorf("blob get user delegation credential: %w", err)
	}

	// Create SAS signature values
	permissions := sas.BlobPermissions{Read: true}
	sasValues := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now,
		ExpiryTime:    expiryTime,
		Permissions:   permissions.String(),
		ContainerName: container,
		BlobName:      key,
	}

	// Sign with user delegation credential
	queryParams, err := sasValues.SignWithUserDelegation(udc)
	if err != nil {
		return "", fmt.Errorf("blob sign SAS with user delegation: %w", err)
	}

	// Construct the full URL with proper path encoding
	blobURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s",
		b.accountName, url.PathEscape(container), url.PathEscape(key), queryParams.Encode())

	return blobURL, nil
}

// to is a helper to get a pointer to a value
func to[T any](v T) *T {
	return &v
}
