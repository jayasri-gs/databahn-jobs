package constants

const (
	// Constants for connectors to be used across multiple services
	PullMechanismS3SQS            = "S3_SQS"
	PullMechanismAzureBlobStorage = "AZURE_BLOB"
	LogTypeParquet                = "parquet"

	LogLineShortSeparator string = "----------------------------"
	LogLineSeparator      string = "----------------------------------------------------------"
	LogLineStarSeparator  string = "**********************************************************" // Example: 1 day ago

	AuthTypeKeyBased  = "key_based"
	AuthTypeRoleBased = "role_based"

	AzureStorageLogTypeOtherLogs = "OTHERLOGS"
	AzureStorageLogTypeNsgLogs   = "NSGLOGS"

	AWSSQSSourceName           = "aws-sqs-s3"
	AzureBlobStorageSourceName = "azure-blob-storage"
)

// The following struct is used to represent the file object in Kafka
// Use by aws-sqs-connector and object-preprocessor-service
type FileObjectKafka struct {
	SourceName string
	BucketName string
	ObjectKey  string
	ObjectETag string
	ObjectSize int
}
