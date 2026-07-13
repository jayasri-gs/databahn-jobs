package pramaan

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

/*
CloudPramaan starts an S3-compatible object store for module testing.
Job and service containers on the shared Docker network should use the network endpoint.
*/
type CloudPramaan struct {
	t                TestLogger
	Container        testcontainers.Container
	ExternalEndpoint string
	NetworkEndpoint  string
	Region           string
	AccessKey        string
	SecretKey        string
	ArtifactsBucket  string
}

const (
	cloudInternalNetworkDNS = "pramaanlocalstack"
	cloudPort               = "4566"
	cloudImage              = "localstack/localstack:3.4.0"
	defaultCloudRegion      = "us-east-1"
	defaultCloudAccessKey   = "test"
	defaultCloudSecretKey   = "test"
	defaultArtifactsBucket  = "test-artifacts-bucket"
)

func NewCloudPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *CloudPramaan {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        cloudImage,
			ExposedPorts: []string{cloudPort + "/tcp"},
			Env: map[string]string{
				"SERVICES": "s3",
			},
			Networks: []string{dockerNetwork.Name},
			NetworkAliases: map[string][]string{
				dockerNetwork.Name: {cloudInternalNetworkDNS},
			},
			WaitingFor: wait.ForHTTP("/_localstack/health").
				WithPort(cloudPort + "/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Could not create cloud object store container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get cloud object store host: %v", err)
	}

	port, err := container.MappedPort(ctx, cloudPort+"/tcp")
	if err != nil {
		log.Fatalf("failed to get cloud object store port: %v", err)
	}

	cloud := &CloudPramaan{
		t:                t,
		Container:        container,
		ExternalEndpoint: fmt.Sprintf("http://%s:%s", host, port.Port()),
		NetworkEndpoint:  fmt.Sprintf("http://%s:%s", cloudInternalNetworkDNS, cloudPort),
		Region:           defaultCloudRegion,
		AccessKey:        defaultCloudAccessKey,
		SecretKey:        defaultCloudSecretKey,
		ArtifactsBucket:  defaultArtifactsBucket,
	}

	if err := cloud.createArtifactsBucket(ctx); err != nil {
		t.Fatalf("Failed to create artifacts bucket in cloud object store: %v", err)
	}

	return cloud
}

func (c *CloudPramaan) createArtifactsBucket(ctx context.Context) error {
	client, err := c.newS3Client(c.ExternalEndpoint)
	if err != nil {
		return err
	}

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.ArtifactsBucket),
	})
	return err
}

func (c *CloudPramaan) newS3Client(endpoint string) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	}), nil
}

func (c *CloudPramaan) GetNetworkEndpoint() string {
	return c.NetworkEndpoint
}

func (c *CloudPramaan) GetExternalEndpoint() string {
	return c.ExternalEndpoint
}

func (c *CloudPramaan) GetRegion() string {
	return c.Region
}

func (c *CloudPramaan) GetAccessKey() string {
	return c.AccessKey
}

func (c *CloudPramaan) GetSecretKey() string {
	return c.SecretKey
}

func (c *CloudPramaan) GetArtifactsBucket() string {
	return c.ArtifactsBucket
}
