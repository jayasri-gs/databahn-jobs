package configuration

import "github.com/databahn-ai/common-utils/configs"

const (
	SourceParsedTopicMapping           = "source_parsed_topic_mapping"
	DestinationTopicMapping            = "topic_mapping"
	DestinationTopicAggregationMapping = "topic_aggregation_mapping"

	DatabaseName   = "database.database"
	DatabaseSchema = "database.schema"
	DatabaseHost   = "database.host"
	DatabasePort   = "database.port"

	//for backend service to read from both old and new config
	KafkaBootstrapServers = configs.KafkaBootstrapServer

	OpenSearchUrl                    = "open_search.url"
	OpenSearchSecretName             = "open_search.os_secret_name"
	OAuthClientCredentialsSecretName = configs.OAuthClientCredentialsSecretName

	OpenTelemetryCollectorUrl = "urls.optl_collector"
	RedisUrl                  = "urls.redis"
	GatewayUrl                = "urls.gateway"
	ControlPlaneBaseUrl       = "urls.control_plane_base_url"
	KsqlDbUrl                 = configs.KSqlDbUrl
	// DataBahnApiUrl used to get the api url from app config
	DataBahnApiUrl = "urls.databahn_api"
	DataBahnAppUrl = "urls.databahn_app"

	// AuthenticationUrl identity urls
	AuthenticationUrl = configs.AuthenticationUrl

	BackupEventsS3Bucket = "s3.events.bucket"
	BackupEventsS3Region = "s3.events.region"
	ArtifactsS3Bucket    = "s3.artifacts.bucket"
	S3BinaryPrefix       = "s3.artifacts.paths.binaries"
	S3BinaryName         = "s3.artifacts.paths.binary_name"
	AuthCognitoId        = "auth.auth_cognito_id"
	AuthClientId         = "auth.client_id"
	Region               = "region"
)
