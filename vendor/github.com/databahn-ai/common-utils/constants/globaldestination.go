package constants

const GlobalDestUndeliveredKey = "UNDELIVERED"
const GlobalDestUnparsedKey = "UNPARSED"
const GlobalDestSensitiveKey = "SENSITIVE"
const GlobalDestArchiveKey = "ARCHIVE"
const GlobalDestMetadataKey = "METADATA"

const GlobalDestUndeliveredDirectory = "undelivered"
const GlobalDestUnparsedDirectory = "unparsed"
const GlobalDestSensitiveDirectory = "sensitive"
const GlobalDestArchiveDirectory = "archive"
const GlobalDestMetadataDirectory = "metadata"

const (
	S3DestinationType        = "S3"
	AzureBlobDestinationType = "AZURE_BLOB"
	SnowflakeDestinationType = "SNOWFLAKE"
)
