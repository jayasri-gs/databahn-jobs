package configuration

import "github.com/databahn-ai/common-utils/configs"

const (
	SourceParsedTopicMapping           = "source_parsed_topic_mapping"
	DestinationTopicMapping            = "topic_mapping"
	StagingTopicMapping                = "staging"
	StagingEnrichment                  = "enrichment"
	StagingVc                          = "vc"
	StagingTransform                   = "transform"
	StagingSensitive                   = "sensitive"
	StagingAggregation                 = "aggregation"
	DestinationTopicAggregationMapping = "topic_aggregation_mapping"

	SecretBackend      = "secret.backend"
	SecretBackendAWS   = "aws"
	SecretBackendVault = "vault"
	SecretBackendAzure = "azure"
	SecretBackendGCP   = "gcp"

	DatabaseName       = "database.database"
	DatabaseSchema     = "database.schema"
	DatabaseHost       = "database.host"
	DatabasePort       = "database.port"
	DatabaseSSLMode    = "database.ssl_mode"
	DataBaseSecretName = "database.secret_name"

	VaultAddress = "vault.address"
	VaultToken   = "vault.token"

	//for backend service to read from both old and new config
	KafkaBootstrapServers               = configs.KafkaBootstrapServer
	InputKafkaClusterBootstrapServers   = "kafka.input.bootstrap_brokers"
	ProcessKafkaClusterBootstrapServers = "kafka.processing.bootstrap_brokers"

	OpenSearchUrl        = "open_search.url"
	OpenSearchSecretName = "open_search.os_secret_name"
	OpenSearchSkipTls    = "open_search.skip_tls"

	OAuthClientCredentialsSecretName = configs.OAuthClientCredentialsSecretName

	OpenTelemetryCollectorUrl           = "urls.optl_collector"
	OpenTelemetryCollectorGrpcUrl       = "urls.optl_collector_grpc"
	OpenTelemetryVectorCollectorUrl     = "urls.optl_collector_vector"
	OpenTelemetryVectorCollectorGrpcUrl = "urls.optl_collector_grpc_vector"
	RedisUrl                            = "urls.redis"

	RedisDefaultIsClusterKey  = "redis.default.is_cluster"
	RedisSamplingIsClusterKey = "redis.sampling.is_cluster"
	RedisPreviewIsClusterKey  = "redis.preview.is_cluster"

	RedisSamplingUrl    = "urls.redis_sampling"
	GatewayUrl          = "urls.gateway"
	ControlPlaneBaseUrl = "urls.control_plane_base_url"
	KsqlDbUrl           = configs.KSqlDbUrl
	// DataBahnApiUrl used to get the api url from app config
	DataBahnApiUrl             = "urls.databahn_api"
	DataBahnAppUrl             = "urls.databahn_app"
	DataBahnRegistryUrl        = "urls.registry_url"
	DataPlaneControllerBaseUrl = "urls.data_plane_controller_internal_base_url"

	// AuthenticationUrl identity urls
	AuthenticationUrl = configs.AuthenticationUrl

	BackupEventsS3Bucket                = "s3.events.bucket"
	BackupEventsS3Region                = "s3.events.region"
	ArtifactsS3Bucket                   = "s3.artifacts.bucket"
	S3BinaryPrefix                      = "s3.artifacts.paths.binaries"
	S3BinaryName                        = "s3.artifacts.paths.binary_name"
	AuthCognitoId                       = "auth.auth_cognito_id"
	AuthClientId                        = "auth.client_id"
	Region                              = "region"
	S3Endpoint                          = "s3.endpoint"
	S3ForcePathStyle                    = "s3.force_path_style"
	S3AccessKeyId                       = "s3.access_key"
	S3SecretKey                         = "s3.secret_key"
	EventHubCheckpointStorageConnection = "event_hub.checkpoint_storage_connection"

	GlobalDestMaxS3Topics        = "globalDestination.max_s3_topics"         // Maximum number of topics for S3 destination
	GlobalDestMaxAzureBlobTopics = "globalDestination.max_azure_blob_topics" // Maximum number of topics for Azure Blob destination
	GlobalDestMaxSnowflakeTopics = "globalDestination.max_snowflake_topics"  // Maximum number of topics for Snowflake destination
	FileServerUserName           = "file_server.username"
	FileServerPassword           = "file_server.password"
	FileServerUrl                = "file_server.url"

	KafkaConsumerCount   = "kafka.consumer.count"
	MaxProcessingWorkers = "max_processing_workers"
	BufferSize           = "buffer_size"

	// Global destination configuration for unparsed topics
	GlobalDestUnparsedMaxS3Topics        = "globalDestination.unparsed.max_s3_topics"
	GlobalDestUnparsedMaxAzureblobTopics = "globalDestination.unparsed.max_azure_blob_topics"
	GlobalDestUnparsedMaxSnowflakeTopics = "globalDestination.unparsed.max_snowflake_topics"

	AzureInfraKeyVaultUrl    = "azure.secrets.infra.url"
	AzureCustomerKeyVaultUrl = "azure.secrets.customer.url"

	GcpInfraProjectId    = "gcp.secrets.infra.project-id"
	GcpCustomerProjectId = "gcp.secrets.customer.project-id"

	EmailBackend       = "email.backend"
	EmailFrom          = "email.from"
	EmailRegion        = "email.region"
	EmailReplyTo       = "email.reply_to"
	AzureACSEndpoint   = "email.azure.endpoint"
	EmailSMTPHost      = "email.smtp.host"
	EmailSMTPPort      = "email.smtp.port"
	EmailSMTPUsername  = "email.smtp.username"
	EmailSMTPPassword  = "email.smtp.password"
	EmailSMTPUseTLS    = "email.smtp.use_tls"
	EmailBackendSES    = "ses"
	EmailBackendACS    = "acs"
	EmailBackendSMTP   = "smtp"
	DefaultEmailReplyTo = "support@databahn.ai"

	// Object storage configuration
	ObjectBackend              = "object.backend"
	ObjectEventCollection      = "object.events.collection"
	ObjectArtifactCollection   = "object.artifacts.collection"
	ObjectS3Region             = "object.s3.region"
	ObjectS3Endpoint           = "object.s3.endpoint"
	ObjectS3AccessKey          = "object.s3.access_key"
	ObjectS3SecretKey          = "object.s3.secret_key"
	ObjectS3ForcePathStyle     = "object.s3.force_path_style"
	ObjectBlobAccountName      = "object.blob.account_name"
	ObjectBlobAccountKey       = "object.blob.account_key"
	ObjectBlobConnectionString = "object.blob.connection_string"
	ObjectGcsProjectID         = "object.gcs.project_id"
	ObjectGcsCredentialsPath   = "object.gcs.credentials_path"

	SearchSecretNameKey = "search.secret_name"
)
