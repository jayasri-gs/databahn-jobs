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

// BlockIDsThrough returns staged block IDs for parts 1..partNumber inclusive.
func BlockIDsThrough(partNumber int) []string {
	if partNumber <= 0 {
		return nil
	}
	ids := make([]string, partNumber)
	for i := 1; i <= partNumber; i++ {
		ids[i-1] = BlockIDForPart(i)
	}
	return ids
}

// PartInfosThrough rebuilds committed part metadata for Azure block uploads.
func PartInfosThrough(partNumber int) []PartInfo {
	if partNumber <= 0 {
		return nil
	}
	parts := make([]PartInfo, partNumber)
	for i := 1; i <= partNumber; i++ {
		id := BlockIDForPart(i)
		parts[i-1] = PartInfo{PartNumber: i, ETag: id}
	}
	return parts
}

// ReattachMultipart continues an in-flight Azure block blob upload after resume.
func (u *AzureUploader) ReattachMultipart(container, blobName, uploadID string, blockIDs []string, contentType string) error {
	if u.client == nil {
		return fmt.Errorf("azure client not configured")
	}
	u.container = container
	u.blobName = blobName
	u.uploadID = uploadID
	u.contentType = contentType
	u.blockIDs = append([]string(nil), blockIDs...)
	u.bbClient = u.client.ServiceClient().NewContainerClient(container).NewBlockBlobClient(blobName)
	return nil
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
	return u.AbortInFlight(ctx, u.container, u.blobName)
}

// AbortInFlight deletes an in-progress block blob upload using checkpoint bucket/key.
func (u *AzureUploader) AbortInFlight(ctx context.Context, container, blobName string) error {
	if u.client == nil || container == "" || blobName == "" {
		return nil
	}
	bb := u.client.ServiceClient().NewContainerClient(container).NewBlockBlobClient(blobName)
	_, err := bb.Delete(ctx, nil)
	return err
}

func (u *AzureUploader) UploadID() string { return u.uploadID }

func (u *AzureUploader) ListParts(ctx context.Context) ([]PartInfo, error) {
	if len(u.blockIDs) == 0 {
		return nil, nil
	}
	return PartInfosThrough(len(u.blockIDs)), nil
}

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
