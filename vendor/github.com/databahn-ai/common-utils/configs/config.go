package configs

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/databahn-ai/common-utils/aws"
	"github.com/spf13/viper"
)

type Database struct {
	SecretArn  string `json:"secret_arn"`
	SecretName string `json:"secret_name"`
	Host       string `json:"host"`
	Port       string `json:"port"`
	Database   string `json:"database"`
	Schema     string `json:"schema"`
}

type OpenSearch struct {
	SecretName string `json:"os_secret_name"`
}

type Kafka struct {
	BootstrapBrokers       string `json:"bootstrap_brokers"`
	ZookeeperConnectString string `json:"zookeeper_connect_string"`
}

type Urls struct {
	ApiUrl            string `json:"api_url"`
	AppUrl            string `json:"app_url"`
	GatewayUrl        string `json:"gateway_url"`
	ServerlessApiUrl  string `json:"serverless_api_url"`
	SchemaRegistryUrl string `json:"schema_registry_url"`
	RegistryUrl       string `json:"registry_url"`
	OptlCollectorUrl  string `json:"optl_collector_url"`
	RedisUrl          string `json:"redis_url"`
	KSqlDbUrl         string `json:"ksql_db_url"`
	AuthenticationUrl string `json:"authentication_url"`
}

type S3 struct {
	Logs struct {
		Name string `json:"name"`
	} `json:"logs"`
	Scripts struct {
		Name  string            `json:"name"`
		Paths map[string]string `json:"paths"`
	}
}

type BackupBucket struct {
	BucketName string
}

type Auth struct {
	AuthCognitoId           string `json:"auth_cognito_id"`
	AuthCognitoIdentityPool string `json:"auth_cognito_identity_pool"`
	ClientId                string `json:"client_id"`
	CognitoArn              string `json:"cognito_arn"`
}

type InfraConfig struct {
	Auth Auth `json:"auth"`
}

type AppConfig struct {
	Database     Database     `json:"database"`
	OpenSearch   OpenSearch   `json:"open_search"`
	Kafka        Kafka        `json:"kafka"`
	Urls         Urls         `json:"urls"`
	S3           S3           `json:"s3"`
	BackupBucket BackupBucket `json:"backup_bucket"`
	SecretNames  SecretNames  `json:"secret_names"`
}

type SecretNames struct {
	OAuthClientCredentials string `json:"oauth_client_credentials"`
}

type DbConfig struct {
}

func InitializeConfig() (*viper.Viper, *AppConfig, error) {
	config := viper.New()
	appConfig := &AppConfig{}
	infraConfig := &InfraConfig{}
	config.AutomaticEnv()

	config.SetDefault(LoggingMode, LogModeProd)
	config.SetDefault(REGION, "us-east-1")
	config.SetDefault(EnvIdentifier, "dev")
	config.SetDefault(AppConfigParameter, "appconfig")
	config.SetDefault(InfraConfigParameter, "infra")
	config.SetDefault(ScriptEndpointVersion, "/ccms/script/setup.sh")
	config.SetDefault(CW_LOGS_REVIEW_SIZE, 50)
	config.SetDefault(TeamNotificationWebHook, "https://databahn954.webhook.office.com/webhookb2/40d46a0f-c74f-4f33-b1e3-f7998b9446ab@07af3855-b64b-4be9-a62e-4c66201ab1e1/IncomingWebhook/e05fcb34d53d4d6b9d8103310031dfba/e4ab525d-5e4a-4955-9033-ac19da60ca33")
	config.SetDefault(AuthMode, AuthModeCognito)
	err := loadAppConfig(config, appConfig)
	if err == nil {
		err = loadSecrets(config)
		if err == nil {
			err = loadInfraConfig(config, infraConfig)
		}
	}
	return config, appConfig, err
}

func loadAppConfig(config *viper.Viper, appConfig *AppConfig) error {
	parameterName := fmt.Sprintf("/%s/%s", config.GetString(EnvIdentifier), config.GetString(AppConfigParameter))
	if os.Getenv("LOG_LEVEL") == "debug" {
		fmt.Printf("reading parameter from parameter store with path %s \n", parameterName)
	}
	data, err := aws.ParameterStoreByName(parameterName, config.GetString(REGION))
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(*data.Parameter.Value), appConfig)
	if err != nil {
		return err
	}
	setConfig(config, appConfig)
	return nil
}

func loadInfraConfig(config *viper.Viper, infraConfig *InfraConfig) error {
	parameterName := fmt.Sprintf("/%s/%s", config.GetString(EnvIdentifier), config.GetString(InfraConfigParameter))
	if os.Getenv("LOG_LEVEL") == "debug" {
		fmt.Printf("reading parameter from parameter store with path %s \n", parameterName)
	}
	data, err := aws.ParameterStoreByName(parameterName, config.GetString(REGION))
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(*data.Parameter.Value), infraConfig)
	if err != nil {
		return err
	}
	config.SetDefault(CognitoPoolId, infraConfig.Auth.AuthCognitoId)
	config.SetDefault(CognitoClientId, infraConfig.Auth.ClientId)
	return nil
}

func setConfig(config *viper.Viper, appConfig *AppConfig) {
	config.SetDefault(DbSecretsName, appConfig.Database.SecretName)
	config.SetDefault(OpenSearchSecretsName, appConfig.OpenSearch.SecretName)
	config.SetDefault(KafkaBootstrapServer, appConfig.Kafka.BootstrapBrokers)
	config.SetDefault(ZookeeperServers, appConfig.Kafka.ZookeeperConnectString)
	config.SetDefault(ApplicationUrl, appConfig.Urls.AppUrl)
	config.SetDefault(GatewayUrl, appConfig.Urls.GatewayUrl)
	config.SetDefault(ApiUrl, appConfig.Urls.ApiUrl)
	config.SetDefault(ServerlessBackendUrl, appConfig.Urls.ServerlessApiUrl)
	config.SetDefault(SchemaRegistryUrl, appConfig.Urls.SchemaRegistryUrl)
	config.SetDefault(KSqlDbUrl, appConfig.Urls.KSqlDbUrl)
	config.SetDefault(OptlCollectorUrl, appConfig.Urls.OptlCollectorUrl)
	config.SetDefault(RedisUrl, appConfig.Urls.RedisUrl)
	config.SetDefault(S3ScriptBucket, appConfig.S3.Scripts.Name)
	config.SetDefault(S3SetupScriptPath, appConfig.S3.Scripts.Paths["setup_script"])
	config.SetDefault(S3BinaryPrefix, appConfig.S3.Scripts.Paths["binaries"])
	config.SetDefault(S3SystemdFile, appConfig.S3.Scripts.Paths["systemd_unit"])
	config.SetDefault(S3ConfigFile, appConfig.S3.Scripts.Paths["config"])
	config.SetDefault(S3BinaryName, appConfig.S3.Scripts.Paths["binary_name"])
	config.SetDefault(BackupBucketName, appConfig.S3.Logs.Name)
	config.SetDefault(DBDatabase, appConfig.Database.Database)
	config.SetDefault(DBHost, appConfig.Database.Host)
	config.SetDefault(DBPort, appConfig.Database.Port)
	config.SetDefault(DBSchema, appConfig.Database.Schema)
	config.SetDefault(OAuthClientCredentialsSecretName, appConfig.SecretNames.OAuthClientCredentials)
	config.SetDefault(AuthenticationUrl, appConfig.Urls.AuthenticationUrl)
}
