package upload

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

type AzureUploader struct {
	client      *azblob.Client
	blobCfg     *destination.AzureBlobConfig
	bbClient    *blockblob.Client
	accountName string
	container   string
	blobName    string
	uploadID    string
	blockIDs    []string
	contentType string
}

func NewAzureUploader(client *azblob.Client, cfg *destination.AzureBlobConfig) *AzureUploader {
	accountName := ""
	if cfg != nil {
		accountName = cfg.AccountName
	}
	return &AzureUploader{client: client, blobCfg: cfg, accountName: accountName}
}

func (u *AzureUploader) Init(ctx context.Context, container, blobName, contentType string) error {
	u.container = container
	u.blobName = blobName
	u.contentType = contentType
	u.blockIDs = nil
	u.uploadID = fmt.Sprintf("%s/%s", container, blobName)
	u.bbClient = u.client.ServiceClient().NewContainerClient(container).NewBlockBlobClient(blobName)
	return nil
}

// BlockIDForPart returns the deterministic staged block ID for a multipart part number.
func BlockIDForPart(partNumber int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("part-%06d", partNumber)))
}

func (u *AzureUploader) UploadPart(ctx context.Context, partNumber int, data io.Reader, size int64) (*PartInfo, error) {
	if u.bbClient == nil {
		return nil, fmt.Errorf("azure uploader not initialized")
	}
	body, err := io.ReadAll(data)
	if err != nil {
		return nil, fmt.Errorf("read upload part %d: %w", partNumber, err)
	}
	blockID := BlockIDForPart(partNumber)
	_, err = u.bbClient.StageBlock(ctx, blockID, nopSeekCloser{bytes.NewReader(body)}, nil)
	if err != nil {
		return nil, fmt.Errorf("upload block %d: %w", partNumber, err)
	}
	u.blockIDs = append(u.blockIDs, blockID)
	return &PartInfo{PartNumber: partNumber, ETag: blockID, Size: int64(len(body))}, nil
}

func (u *AzureUploader) Complete(ctx context.Context, parts []PartInfo) error {
	if u.bbClient == nil {
		return fmt.Errorf("azure uploader not initialized")
	}
	commitOpts := &blockblob.CommitBlockListOptions{}
	if u.contentType != "" {
		commitOpts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: &u.contentType}
	}
	_, err := u.bbClient.CommitBlockList(ctx, u.blockIDs, commitOpts)
	if err != nil {
		return fmt.Errorf("commit block list: %w", err)
	}
	return nil
}

func (u *AzureUploader) Abort(ctx context.Context) error {
	if u.client == nil || u.container == "" || u.blobName == "" {
		return nil
	}
	bb := u.client.ServiceClient().NewContainerClient(u.container).NewBlockBlobClient(u.blobName)
	_, err := bb.Delete(ctx, nil)
	return err
}

func (u *AzureUploader) UploadID() string { return u.uploadID }

func (u *AzureUploader) GeneratePresignedURL(ctx context.Context, expiry time.Duration) (string, error) {
	if u.blobCfg == nil {
		if u.accountName == "" {
			return "", fmt.Errorf("azure blob config required for download URL")
		}
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", u.accountName, u.container, u.blobName), nil
	}
	return destination.GenerateBlobReadSASURL(ctx, u.blobCfg, u.container, u.blobName, expiry)
}

func (u *AzureUploader) GetLocation() string {
	if u.accountName == "" {
		return fmt.Sprintf("https://blob.core.windows.net/%s/%s", u.container, u.blobName)
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", u.accountName, u.container, u.blobName)
}

type nopSeekCloser struct{ *bytes.Reader }

func (nopSeekCloser) Close() error { return nil }
