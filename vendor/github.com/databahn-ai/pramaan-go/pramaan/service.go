package pramaan

import (
	"context"
	"fmt"
	"testing"
	"time"

	dbkafka "github.com/databahn-ai/common-utils/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

/*
ServicePramaan is a utility to start your service for module testing.
It can start service with dependencies like Kafka and Redis.
It provides methods to get the service logger, Kafka instance, and Redis instance.
*/
type ServicePramaan struct {
	tg         TestLogger
	kafka      *KafkaPramaan
	service    *testcontainers.Container
	network    *testcontainers.DockerNetwork
	sg         *ServiceLogger
	redis      *RedisPramaan
	postgres   *PostgresPramaan
	openSearch *OpenSearchPramaan
	cloud      *CloudPramaan
	vault      *VaultPramaan
	prometheus *PrometheusPramaan
}

func (s ServicePramaan) GetKafka(t *testing.T) *KafkaPramaan {
	if s.kafka == nil {
		t.Fatalf("Kafka not enabled/initialized")
	}
	return s.kafka
}

func (s ServicePramaan) GetRedis(t *testing.T) *RedisPramaan {
	if s.redis == nil {
		t.Fatalf("Redis not enabled/initialized")
	}
	return s.redis
}

func (s ServicePramaan) GetPostgres(t *testing.T) *PostgresPramaan {
	if s.postgres == nil {
		t.Fatalf("Postgres not enabled/initialized")
	}
	return s.postgres
}

func (s ServicePramaan) GetOpenSearch(t *testing.T) *OpenSearchPramaan {
	if s.openSearch == nil {
		t.Fatalf("OpenSearch not enabled/initialized")
	}
	return s.openSearch
}

func (s ServicePramaan) GetCloud(t *testing.T) *CloudPramaan {
	if s.cloud == nil {
		t.Fatalf("Cloud object store not enabled/initialized")
	}
	return s.cloud
}

func (s ServicePramaan) GetPrometheus(t *testing.T) *PrometheusPramaan {
	if s.prometheus == nil {
		t.Fatalf("Prometheus not enabled/initialized")
	}
	return s.prometheus
}

func (s ServicePramaan) GetServiceLogger() *ServiceLogger {
	return s.sg
}

// GetServiceURL returns the HTTP URL to access the service running in the container on the default port 8080.
// This is a convenience method that calls GetServiceURLForPort with port "8080".
func (s ServicePramaan) GetServiceURL(ctx context.Context) (string, error) {
	return s.GetServiceURLForPort(ctx, "8080")
}

// GetServiceURLForPort returns the HTTP URL to access the service on a specific container port.
// The port parameter should be just the port number (e.g., "8080", "9090").
// Returns the URL in format http://host:mappedPort
func (s ServicePramaan) GetServiceURLForPort(ctx context.Context, port string) (string, error) {
	if s.service == nil {
		return "", fmt.Errorf("service not started")
	}

	container := *s.service
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, port+"/tcp")
	if err != nil {
		return "", fmt.Errorf("failed to get mapped port for %s: %w", port, err)
	}

	return fmt.Sprintf("http://%s:%s", host, mappedPort.Port()), nil
}

// GetServiceHost returns the host where the service container is running.
func (s ServicePramaan) GetServiceHost(ctx context.Context) (string, error) {
	if s.service == nil {
		return "", fmt.Errorf("service not started")
	}

	container := *s.service
	return container.Host(ctx)
}

// GetServiceMappedPort returns the host-mapped port for a given container port.
// The port parameter should be just the port number (e.g., "8080", "9090").
func (s ServicePramaan) GetServiceMappedPort(ctx context.Context, port string) (string, error) {
	if s.service == nil {
		return "", fmt.Errorf("service not started")
	}

	container := *s.service
	mappedPort, err := container.MappedPort(ctx, port+"/tcp")
	if err != nil {
		return "", fmt.Errorf("failed to get mapped port for %s: %w", port, err)
	}

	return mappedPort.Port(), nil
}

// GetAllServiceURLs returns HTTP URLs for all exposed ports of the service container.
// Returns a map of containerPort -> URL (e.g., "8080" -> "http://localhost:32768")
func (s ServicePramaan) GetAllServiceURLs(ctx context.Context) (map[string]string, error) {
	if s.service == nil {
		return nil, fmt.Errorf("service not started")
	}

	container := *s.service
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	// Get container ports info
	inspect, err := container.Inspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container: %w", err)
	}

	urls := make(map[string]string)
	for portProto, bindings := range inspect.NetworkSettings.Ports {
		if len(bindings) > 0 {
			// portProto is like "8080/tcp"
			containerPort := portProto.Port()
			hostPort := bindings[0].HostPort
			urls[containerPort] = fmt.Sprintf("http://%s:%s", host, hostPort)
		}
	}

	return urls, nil
}

type ServicePramaanBuilder struct {
	tg                     TestLogger
	kafka                  bool
	redis                  bool
	prometheus             bool
	postgres               bool
	openSearch             bool
	s3ObjectStore          bool
	kafkaTopics            []TopicDetails
	kafkaMessages          map[string][]dbkafka.Message
	dockerFilePath         string
	serviceName            string
	serviceLogLineToWait   string
	kafkaTopicsToCollect   []string
	configModifiers        []ConfigModifier
	scaleConfigOverrides   map[string]string
	EnvironmentVariables   map[string]string
	StartUpCommands        []string
	openSearchTemplateDirs []string
}

func NewServicePramaanBuilder(t *testing.T) *ServicePramaanBuilder {
	tg := NewGoTestLogger(t)
	return &ServicePramaanBuilder{
		tg: tg,
	}
}

func NewServiceMainPramaanBuilder(tg TestLogger) *ServicePramaanBuilder {
	return &ServicePramaanBuilder{
		tg: tg,
	}
}

func (s *ServicePramaanBuilder) WithKafka() *ServicePramaanBuilder {
	s.kafka = true
	return s
}

func (s *ServicePramaanBuilder) WithRedis() *ServicePramaanBuilder {
	s.redis = true
	return s
}

func (s *ServicePramaanBuilder) WithPrometheus() *ServicePramaanBuilder {
	s.prometheus = true
	return s
}

func (s *ServicePramaanBuilder) ForControlPlane(withPostgres bool, withOpenSearch bool, withS3ObjectStore bool) *ServicePramaanBuilder {
	s.postgres = withPostgres
	s.openSearch = withOpenSearch
	s.s3ObjectStore = withS3ObjectStore
	return s
}

func (s *ServicePramaanBuilder) WithEnvironmentVariables(envVars map[string]string) *ServicePramaanBuilder {
	s.EnvironmentVariables = envVars
	return s
}

func (s *ServicePramaanBuilder) WithKafkaTopics(topicDetails []TopicDetails) *ServicePramaanBuilder {
	s.kafkaTopics = topicDetails
	return s
}

func (s *ServicePramaanBuilder) WithKafkaMessages(topic string, messages []dbkafka.Message) *ServicePramaanBuilder {
	if s.kafkaMessages == nil {
		s.kafkaMessages = make(map[string][]dbkafka.Message)
	}
	s.kafkaMessages[topic] = append(s.kafkaMessages[topic], messages...)
	return s
}

func (s *ServicePramaanBuilder) WithServiceDetails(serviceName, dockerFilePath, serviceLogLineToWait string) *ServicePramaanBuilder {
	s.dockerFilePath = dockerFilePath
	s.serviceName = serviceName
	s.serviceLogLineToWait = serviceLogLineToWait
	return s
}

func (s *ServicePramaanBuilder) WithKafkaTopicsToCollect(topics []string) *ServicePramaanBuilder {
	s.kafkaTopicsToCollect = topics
	return s
}

func (s *ServicePramaanBuilder) WithOpenSearchTemplateDirs(dirs ...string) *ServicePramaanBuilder {
	s.openSearchTemplateDirs = append(s.openSearchTemplateDirs, dirs...)
	return s
}

func (s *ServicePramaanBuilder) WithConfigModifiers(configModifiers []ConfigModifier) *ServicePramaanBuilder {
	s.configModifiers = configModifiers
	return s
}

/*
WithScaleConfigOverride sets a value in the scale config YAML.
path is a dot-separated key path matching the YAML hierarchy,
e.g. "schemaless-normalization-service.buffer_size".
*/
func (s *ServicePramaanBuilder) WithScaleConfigOverride(path, value string) *ServicePramaanBuilder {
	if s.scaleConfigOverrides == nil {
		s.scaleConfigOverrides = make(map[string]string)
	}
	s.scaleConfigOverrides[path] = value
	return s
}

func (s *ServicePramaanBuilder) WithStartupCommands(cmd []string) *ServicePramaanBuilder {
	s.StartUpCommands = cmd
	return s
}

/*
Build creates a new ServicePramaan instance with the specified configurations.
It initializes the Docker network, Kafka, Redis, and the service container.
It also sets up the service logger and applies any configuration modifiers.
*/
func (s *ServicePramaanBuilder) Build(ctx context.Context) *ServicePramaan {
	pramaan := ServicePramaan{}
	dockerNetwork, err := network.New(ctx)
	if err != nil {
		s.tg.Fatalf("Could not create network: %v", err)
	}
	pramaan.network = dockerNetwork
	config := &Config{}
	if s.kafka {
		kafka := NewKafkaPramaan(ctx, s.tg, dockerNetwork)
		pramaan.kafka = kafka
		kafka.CreateTopics(ctx, s.tg, s.kafkaTopics)
		kafka.StartConsumers(ctx, s.tg, s.kafkaTopicsToCollect)
		config.BootstrapBrokers = kafka.NetworkBroker
		if s.kafkaMessages != nil {
			for topic, messages := range s.kafkaMessages {
				kafka.SendMessagesSync(ctx, s.tg, topic, messages)
			}
		}
		fmt.Printf("[%s] Kafka started\n", s.tg.Name())
	}

	if s.redis {
		redis := NewRedisPramaan(ctx, s.tg, dockerNetwork)
		pramaan.redis = redis
		config.RedisUrl = redis.redisUrl
		fmt.Printf("[%s] Redis Started\n", s.tg.Name())
	}

	if s.prometheus {
		prom := NewPrometheusPramaan(ctx, s.tg, dockerNetwork)
		pramaan.prometheus = prom
		config.PrometheusUrl = prom.prometheusIntURL
		fmt.Printf("[%s] Prometheus Started\n", s.tg.Name())
	}

	if s.postgres || s.openSearch {
		vault := NewVaultPramaan(ctx, s.tg, dockerNetwork)
		pramaan.vault = vault
		fmt.Printf("[%s] Vault Started\n", s.tg.Name())
		config.VaultAddress = vault.GetExternalURL()
		config.VaultToken = vault.GetRootToken()
		config.SecretBackend = "vault"
	}

	if s.postgres {
		postgress := NewPostgresPramaan(ctx, s.tg, dockerNetwork)
		pramaan.postgres = postgress
		config.DatabaseHost = postgress.GetHost()
		config.DatabasePort = postgress.GetPort()
		config.DatabaseName = postgress.GetDatabase()
		config.DatabaseSecretName = "secret/data/db_database"
		if pramaan.vault != nil {
			if err := pramaan.vault.StorePostgresCredentials(ctx, postgress, config.DatabaseSecretName); err != nil {
				s.tg.Fatalf("Failed to store postgres credentials: %v", err)
			}
		}
		fmt.Printf("[%s] Postgres Started\n", s.tg.Name())
	}
	if s.openSearch {
		opensearch := NewOpenSearchPramaan(ctx, s.tg, dockerNetwork, s.openSearchTemplateDirs...)
		pramaan.openSearch = opensearch
		config.OpenSearchSecretName = "secret/data/db_open_search"
		if pramaan.vault != nil {
			if err := pramaan.vault.StoreOpenSearchCredentials(ctx, opensearch, config.OpenSearchSecretName); err != nil {
				s.tg.Fatalf("Failed to store open search credentials: %v", err)
			}
		}
		fmt.Printf("[%s] OpenSearch Started\n", s.tg.Name())
	}

	if s.s3ObjectStore {
		cloud := NewCloudPramaan(ctx, s.tg, dockerNetwork)
		pramaan.cloud = cloud
		applyCloudObjectStoreConfig(config, cloud)
		fmt.Printf("[%s] Cloud object store started\n", s.tg.Name())
	}

	pramaan.service, pramaan.sg = s.buildService(ctx, s.tg, dockerNetwork, config)
	fmt.Printf("[%s] service started\n", s.tg.Name())
	return &pramaan
}

func (s *ServicePramaanBuilder) buildService(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork, config *Config) (*testcontainers.Container, *ServiceLogger) {
	for _, configModifier := range s.configModifiers {
		configModifier(config)
	}
	appConfig := getAppConfig(t, config)

	// 0644: files are root-owned in the container; non-root service users must be able to read configs (DB-26723).
	appConfigFile := testcontainers.ContainerFile{
		Reader:            appConfig,
		ContainerFilePath: containerAppConfigFilePath,
		FileMode:          0o644,
	}
	destinationConfig := getDestinationConfig(t)
	destinationConfigFile := testcontainers.ContainerFile{
		Reader:            destinationConfig,
		ContainerFilePath: containerDestinationConfigFilePath,
		FileMode:          0o644,
	}
	schemaLessConfig := getScaleConfig(t, s.scaleConfigOverrides)
	schemaLessConfigFile := testcontainers.ContainerFile{
		Reader:            schemaLessConfig,
		ContainerFilePath: containerScaleConfigFilePath,
		FileMode:          0o644,
	}
	containerFiles := []testcontainers.ContainerFile{appConfigFile, destinationConfigFile, schemaLessConfigFile}
	logger := NewServiceLogger(t.Name())
	req := testcontainers.ContainerRequest{
		Name: s.serviceName,
		Env:  s.EnvironmentVariables,
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    s.dockerFilePath,
			Dockerfile: "Dockerfile",
		},
		Files:        containerFiles,
		ExposedPorts: []string{"8080"},
		WaitingFor:   wait.ForLog(s.serviceLogLineToWait).WithStartupTimeout(1 * time.Minute),
		Networks:     []string{dockerNetwork.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{},
			Consumers: []testcontainers.LogConsumer{logger},
		},
	}
	if len(s.StartUpCommands) > 0 {
		req.Cmd = s.StartUpCommands
	}

	service, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
	if err != nil {
		t.Fatalf("Could not get service running: %s", err)
	}
	return &service, logger
}
