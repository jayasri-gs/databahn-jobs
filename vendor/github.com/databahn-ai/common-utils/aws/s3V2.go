package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type Client struct {
	AuthType         string
	AccessKeyID      string
	SecretAccessKey  string
	Region           string
	RoleArn          string
	ExternalID       string
	URL              string
	S3ForcePathStyle bool
	_client          *s3.Client
}

func (c *Client) Connect() {
	var (
		cfg aws.Config
		err error
	)

	ctx := context.TODO()

	// Static credentials
	if c.AuthType == "static" {
		creds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""))
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(c.Region),
			config.WithCredentialsProvider(creds),
		)
		if err != nil {
			log.Fatalf("failed to load config with static creds: %v", err)
		}
	} else {
		// Default credential chain (IAM roles, environment, etc.)
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(c.Region))
		if err != nil {
			log.Fatalf("failed to load default config: %v", err)
		}
	}

	// Assume role if specified
	if c.RoleArn != "" {
		stsClient := sts.NewFromConfig(cfg)
		options := func(o *stscreds.AssumeRoleOptions) {
			if c.ExternalID != "" {
				o.ExternalID = &c.ExternalID
			}
		}
		creds := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, c.RoleArn, options))
		cfg.Credentials = creds
	}

	// Create S3 client with custom endpoint and path style if needed
	c._client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = c.S3ForcePathStyle
		if c.URL != "" {
			o.BaseEndpoint = aws.String(c.URL)
		}
	})
}

func (c *Client) UploadFile(ctx context.Context, bucketName, key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = c._client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	return nil
}

func (c *Client) DownloadFile(ctx context.Context, bucketName, key, destinationPath string) error {
	output, err := c._client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer output.Body.Close()

	file, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, output.Body)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func (c *Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	_, err := c._client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	return true, nil
}
