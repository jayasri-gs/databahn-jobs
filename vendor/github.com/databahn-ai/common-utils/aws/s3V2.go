package aws

import (
	"crypto/tls"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Client struct {
	AuthType           string
	AccessKeyID        string
	SecretAccessKey    string
	Region             string
	RoleArn            string
	ExternalID         string
	URL                string
	S3ForcePathStyle   bool
	InsecureSkipVerify bool
	_client            *s3.S3
}

func (c *Client) Connect() error {
	var config *aws.Config

	// Static credentials
	if c.AuthType == "KEY_BASED_AUTH" {
		config = &aws.Config{
			Region: aws.String(c.Region),
			Credentials: credentials.NewStaticCredentials(
				c.AccessKeyID, c.SecretAccessKey, "",
			),
		}
	} else {
		// Default credential chain
		config = &aws.Config{
			Region: aws.String(c.Region),
		}
	}

	// Custom endpoint and path style
	if c.URL != "" {
		config.Endpoint = aws.String(c.URL)
		config.S3ForcePathStyle = aws.Bool(c.S3ForcePathStyle)
	}

	// Custom HTTP client for TLS configuration
	config.HTTPClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: c.InsecureSkipVerify,
			},
		},
	}

	// Create session and S3 client
	sess, err := session.NewSession(config)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	c._client = s3.New(sess)
	return nil
}

func (c *Client) UploadFileFromLocation(bucketName, key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = c._client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	return nil
}

func (c *Client) UploadFile(bucketName, key string, file io.ReadSeeker) error {
	_, err := c._client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	return nil
}

func (c *Client) DownloadFileToLocation(bucketName, key, destinationPath string) error {
	output, err := c._client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
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

func (c *Client) DownloadFile(bucketName, key string) (io.ReadCloser, error) {
	output, err := c._client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}
	return output.Body, nil
}

func (c *Client) BucketExists(bucketName string) (bool, error) {
	_, err := c._client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NotFound" {
			return false, nil
		}
		return false, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	return true, nil
}
