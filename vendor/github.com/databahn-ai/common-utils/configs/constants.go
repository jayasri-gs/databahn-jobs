package configs

const (
	EnvIdentifier        = "ENV_IDENTIFIER"
	AppConfigParameter   = "APP_CONFIG_PARAMETER"
	InfraConfigParameter = "INFRA_CONFIG_PARAMETER"

	DbSecretsName         = "DbSecretsName"
	OpenSearchSecretsName = "OpenSearchSecretsName"
	KafkaBootstrapServer  = "kafka.bootstrap_brokers"
	ZookeeperServers      = "ZookeeperServers"
	ApplicationUrl        = "ApplicationUrl"
	GatewayUrl            = "GatewayUrl"
	ApiUrl                = "ApiUrl"
	ServerlessBackendUrl  = "ServerlessBackendUrl"
	SchemaRegistryUrl     = "SchemaRegistryUrl"
	KSqlDbUrl             = "urls.ksql_db"
	OptlCollectorUrl      = "OptlCollectorUrl"
	RedisUrl              = "RedisUrl"
	ElasticsearchURl      = "ELASTICSEARCH_URL"
	ElasticsearchPassword = "ELASTICSEARCH_PASSWORD"
	ElasticsearchUsername = "ELASTICSEARCH_USERNAME"
	LoggingMode           = "LOG_LEVEL"
	LogModeProd           = "PROD"
	LogModeDev            = "DEV"
	LogModeUt             = "UNIT_TEST"

	REGION = "REGION"

	S3ScriptBucket    = "ScriptBucket"
	S3SetupScriptPath = "S3_SETUP_SCRIPT_PATH"
	S3BinaryPrefix    = "S3_BINARY_PREFIX"

	S3SystemdFile = "S3_SYSTEMD_FILE"
	S3ConfigFile  = "S3_CONFIG_FILE"
	S3BinaryName  = "S3_BINARY_NAME"

	DBUsername          = "DB_USER_NAME"
	DBPassword          = "DB_PASSWORD"
	DBEngine            = "DB_ENGINE"
	DBHost              = "DB_HOST"
	DBPort              = "DB_PORT"
	DBSchema            = "DB_SCHEMA"
	DbClusterIdentifier = "DB_CLUSTER_IDENTIFIER"
	DBDatabase          = "DB_DATABASE"

	ScriptEndpointVersion = "SCRIPT_ENDPOINT_VERSION"

	CognitoPoolId   = "COGNITO_POOL_ID"
	CognitoClientId = "CLIENT_ID"

	AuthMode = "AUTH_MODE"
	// Deprecated: All new services will run with token based lambda. Service need to use RUN_MODE instead from configs package
	AuthModeDeveloper = "AUTH_MODE_DEVELOPER"
	AuthModeCognito   = "AUTH_MODE_COGNITO"
	AuthModeIam       = "AUTH_MODE_IAM"
	CommitHash        = "COMMIT_HASH"
	CurrentDeployer   = "CURRENT_DEPLOYER"
	GitBranch         = "GIT_BRANCH"

	BackupBucketName = "BACKUP_BUCKET_NAME"

	OpenSearchUrl      = "OPEN_SEARCH_URL"
	OpenSearchUserName = "OPEN_SEARCH_USER_NAME"
	OpenSearchPassword = "OPEN_SEARCH_PASSWORD"

	OpenSearchStatsIndex      = "OPEN_SEARCH_STATS_INDEX"
	OpenSearchStatisticsIndex = "OPEN_SEARCH_STATISTICS_INDEX"

	CW_LOGS_REVIEW_SIZE     = "CW_LOGS_REVIEW_SIZE"
	TeamNotificationWebHook = "TEAMS_NOTIFICATION_WEBHOOK"

	RunMode        = "RUN_MODE"
	RunModeLambda  = "RUN_MODE_LAMBDA"
	RunModeService = "RUN_MODE_SERVICE"

	OAuthClientCredentialsSecretName = "oauth.client_credentials_secret_name"
	AuthenticationUrl                = "urls.authentication_url"
)
