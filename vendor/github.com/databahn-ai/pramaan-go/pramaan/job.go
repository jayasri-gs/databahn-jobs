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
JobPramaan is a utility for integration-testing batch/CLI jobs built from a Dockerfile.
The image is built once (typically in TestMain); each test can start a fresh container
from that image with different commands and environment variables.
*/
type JobPramaan struct {
	tg                   TestLogger
	image                string
	network              *testcontainers.DockerNetwork
	config               *Config
	kafka                *KafkaPramaan
	redis                *RedisPramaan
	postgres             *PostgresPramaan
	openSearch           *OpenSearchPramaan
	vault                *VaultPramaan
	prometheus           *PrometheusPramaan
	configModifiers      []ConfigModifier
	scaleConfigOverrides map[string]string
	environmentVariables map[string]string
}

/*
JobRunOptions configures a single job container execution.
*/
type JobRunOptions struct {
	Cmd         []string
	Env         map[string]string
	Timeout     time.Duration
	LogCaptures []string
}

/*
JobRunResult contains the outcome of a single job run.
*/
type JobRunResult struct {
	ExitCode      int
	Logger        *ServiceLogger
	LogCaptureIDs []string
}

func (j JobPramaan) GetKafka(t *testing.T) *KafkaPramaan {
	if j.kafka == nil {
		t.Fatalf("Kafka not enabled/initialized")
	}
	return j.kafka
}

func (j JobPramaan) GetRedis(t *testing.T) *RedisPramaan {
	if j.redis == nil {
		t.Fatalf("Redis not enabled/initialized")
	}
	return j.redis
}

func (j JobPramaan) GetPostgres(t *testing.T) *PostgresPramaan {
	if j.postgres == nil {
		t.Fatalf("Postgres not enabled/initialized")
	}
	return j.postgres
}

func (j JobPramaan) GetOpenSearch(t *testing.T) *OpenSearchPramaan {
	if j.openSearch == nil {
		t.Fatalf("OpenSearch not enabled/initialized")
	}
	return j.openSearch
}

func (j JobPramaan) GetPrometheus(t *testing.T) *PrometheusPramaan {
	if j.prometheus == nil {
		t.Fatalf("Prometheus not enabled/initialized")
	}
	return j.prometheus
}

func (j JobPramaan) Image() string {
	return j.image
}

type JobPramaanBuilder struct {
	tg                     TestLogger
	kafka                  bool
	redis                  bool
	prometheus             bool
	postgres               bool
	openSearch             bool
	kafkaTopics            []TopicDetails
	kafkaMessages          map[string][]dbkafka.Message
	dockerFilePath         string
	jobName                string
	kafkaTopicsToCollect   []string
	configModifiers        []ConfigModifier
	scaleConfigOverrides   map[string]string
	environmentVariables   map[string]string
	openSearchTemplateDirs []string
}

func NewJobPramaanBuilder(t *testing.T) *JobPramaanBuilder {
	return &JobPramaanBuilder{tg: NewGoTestLogger(t)}
}

func NewJobMainPramaanBuilder(tg TestLogger) *JobPramaanBuilder {
	return &JobPramaanBuilder{tg: tg}
}

func (b *JobPramaanBuilder) WithKafka() *JobPramaanBuilder {
	b.kafka = true
	return b
}

func (b *JobPramaanBuilder) WithRedis() *JobPramaanBuilder {
	b.redis = true
	return b
}

func (b *JobPramaanBuilder) WithPrometheus() *JobPramaanBuilder {
	b.prometheus = true
	return b
}

func (b *JobPramaanBuilder) ForControlPlane(withPostgres bool, withOpenSearch bool) *JobPramaanBuilder {
	b.postgres = withPostgres
	b.openSearch = withOpenSearch
	return b
}

func (b *JobPramaanBuilder) WithEnvironmentVariables(envVars map[string]string) *JobPramaanBuilder {
	b.environmentVariables = envVars
	return b
}

func (b *JobPramaanBuilder) WithKafkaTopics(topicDetails []TopicDetails) *JobPramaanBuilder {
	b.kafkaTopics = topicDetails
	return b
}

func (b *JobPramaanBuilder) WithKafkaMessages(topic string, messages []dbkafka.Message) *JobPramaanBuilder {
	if b.kafkaMessages == nil {
		b.kafkaMessages = make(map[string][]dbkafka.Message)
	}
	b.kafkaMessages[topic] = append(b.kafkaMessages[topic], messages...)
	return b
}

func (b *JobPramaanBuilder) WithJobDetails(jobName, dockerFilePath string) *JobPramaanBuilder {
	b.jobName = jobName
	b.dockerFilePath = dockerFilePath
	return b
}

func (b *JobPramaanBuilder) WithKafkaTopicsToCollect(topics []string) *JobPramaanBuilder {
	b.kafkaTopicsToCollect = topics
	return b
}

func (b *JobPramaanBuilder) WithOpenSearchTemplateDirs(dirs ...string) *JobPramaanBuilder {
	b.openSearchTemplateDirs = append(b.openSearchTemplateDirs, dirs...)
	return b
}

func (b *JobPramaanBuilder) WithConfigModifiers(configModifiers []ConfigModifier) *JobPramaanBuilder {
	b.configModifiers = configModifiers
	return b
}

func (b *JobPramaanBuilder) WithScaleConfigOverride(path, value string) *JobPramaanBuilder {
	if b.scaleConfigOverrides == nil {
		b.scaleConfigOverrides = make(map[string]string)
	}
	b.scaleConfigOverrides[path] = value
	return b
}

/*
Build starts optional dependencies, builds the job Docker image once, and returns JobPramaan.
Call Run from individual tests with different JobRunOptions.
*/
func (b *JobPramaanBuilder) Build(ctx context.Context) *JobPramaan {
	job := &JobPramaan{
		tg:                   b.tg,
		configModifiers:      b.configModifiers,
		scaleConfigOverrides: b.scaleConfigOverrides,
		environmentVariables: b.environmentVariables,
		config:               &Config{},
	}

	dockerNetwork, err := network.New(ctx)
	if err != nil {
		b.tg.Fatalf("Could not create network")
	}
	job.network = dockerNetwork

	if b.kafka {
		kafka := NewKafkaPramaan(ctx, b.tg, dockerNetwork)
		job.kafka = kafka
		kafka.CreateTopics(ctx, b.tg, b.kafkaTopics)
		kafka.StartConsumers(ctx, b.tg, b.kafkaTopicsToCollect)
		job.config.BootstrapBrokers = kafka.NetworkBroker
		if b.kafkaMessages != nil {
			for topic, messages := range b.kafkaMessages {
				kafka.SendMessagesSync(ctx, b.tg, topic, messages)
			}
		}
		fmt.Printf("[%s] Kafka started\n", b.tg.Name())
	}

	if b.redis {
		redis := NewRedisPramaan(ctx, b.tg, dockerNetwork)
		job.redis = redis
		job.config.RedisUrl = redis.redisUrl
		fmt.Printf("[%s] Redis started\n", b.tg.Name())
	}

	if b.prometheus {
		prom := NewPrometheusPramaan(ctx, b.tg, dockerNetwork)
		job.prometheus = prom
		job.config.PrometheusUrl = prom.prometheusIntURL
		fmt.Printf("[%s] Prometheus started\n", b.tg.Name())
	}

	if b.postgres || b.openSearch {
		vault := NewVaultPramaan(ctx, b.tg, dockerNetwork)
		job.vault = vault
		job.config.VaultAddress = vault.GetNetworkURL()
		job.config.VaultToken = vault.GetRootToken()
		job.config.SecretBackend = "vault"
		fmt.Printf("[%s] Vault started\n", b.tg.Name())
	}

	if b.postgres {
		postgres := NewPostgresPramaan(ctx, b.tg, dockerNetwork)
		job.postgres = postgres
		job.config.DatabaseHost = postgres.GetNetworkHost()
		job.config.DatabasePort = postgres.GetNetworkPort()
		job.config.DatabaseName = postgres.GetDatabase()
		job.config.DatabaseSecretName = "secret/data/db_database"
		if job.vault != nil {
			if err := job.vault.StorePostgresCredentials(ctx, postgres, job.config.DatabaseSecretName); err != nil {
				b.tg.Fatalf("Failed to store postgres credentials: %v", err)
			}
		}
		fmt.Printf("[%s] Postgres started\n", b.tg.Name())
	}

	if b.openSearch {
		opensearch := NewOpenSearchPramaan(ctx, b.tg, dockerNetwork, b.openSearchTemplateDirs...)
		job.openSearch = opensearch
		job.config.OpenSearchSecretName = "secret/data/db_open_search"
		if job.vault != nil {
			if err := job.vault.StoreOpenSearchCredentials(ctx, opensearch, job.config.OpenSearchSecretName); err != nil {
				b.tg.Fatalf("Failed to store open search credentials: %v", err)
			}
		}
		fmt.Printf("[%s] OpenSearch started\n", b.tg.Name())
	}

	job.image = b.buildImage(ctx)
	fmt.Printf("[%s] job image built: %s\n", b.tg.Name(), job.image)
	return job
}

func (b *JobPramaanBuilder) buildImage(ctx context.Context) string {
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		b.tg.Fatalf("Could not create docker provider: %v", err)
	}
	defer provider.Close()

	req := &testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    b.dockerFilePath,
			Dockerfile: "Dockerfile",
			Repo:       b.jobName,
			Tag:        "pramaan",
			KeepImage:  true,
		},
	}

	image, err := provider.BuildImage(ctx, req)
	if err != nil {
		b.tg.Fatalf("Could not build job image: %v", err)
	}
	return image
}

/*
Run starts a one-shot job container from the pre-built image, waits for it to exit,
and fails the test if the exit code is non-zero.
*/
func (j *JobPramaan) Run(ctx context.Context, tg TestLogger, opts JobRunOptions) *JobRunResult {
	result := j.runJob(ctx, tg, opts)
	if result.ExitCode != 0 {
		tg.Fatalf("Job exited with code %d", result.ExitCode)
	}
	return result
}

/*
RunUnchecked starts a one-shot job container and returns the exit code without failing the test.
Use this when the expected outcome is a non-zero exit code.
*/
func (j *JobPramaan) RunUnchecked(ctx context.Context, tg TestLogger, opts JobRunOptions) *JobRunResult {
	return j.runJob(ctx, tg, opts)
}

func (j *JobPramaan) runJob(ctx context.Context, tg TestLogger, opts JobRunOptions) *JobRunResult {
	config := *j.config
	for _, configModifier := range j.configModifiers {
		configModifier(&config)
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}

	logger := NewServiceLogger(tg.Name())
	logCaptureIDs := registerJobLogCaptures(logger, opts)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      j.image,
			Cmd:        opts.Cmd,
			Env:        mergeEnv(j.environmentVariables, opts.Env),
			Files:      containerFilesForConfig(tg, &config, j.scaleConfigOverrides),
			Networks:   []string{j.network.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(timeout),
			LogConsumerCfg: &testcontainers.LogConsumerConfig{
				Consumers: []testcontainers.LogConsumer{logger},
			},
		},
		Started: true,
	})
	if err != nil {
		tg.Fatalf("Could not run job container: %v", err)
	}
	defer container.Terminate(ctx)

	state, err := container.State(ctx)
	if err != nil {
		tg.Fatalf("Could not read job container state: %v", err)
	}

	return &JobRunResult{
		ExitCode:      state.ExitCode,
		Logger:        logger,
		LogCaptureIDs: logCaptureIDs,
	}
}

func mergeEnv(base map[string]string, overrides map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}

func registerJobLogCaptures(logger *ServiceLogger, opts JobRunOptions) []string {
	logCaptureIDs := make([]string, 0, len(opts.LogCaptures))
	for _, capture := range opts.LogCaptures {
		logCaptureIDs = append(logCaptureIDs, logger.CaptureLogsContaining(capture))
	}
	return logCaptureIDs
}

func containerFilesForConfig(t TestLogger, config *Config, scaleOverrides map[string]string) []testcontainers.ContainerFile {
	return []testcontainers.ContainerFile{
		{
			Reader:            getAppConfig(t, config),
			ContainerFilePath: containerAppConfigFilePath,
			FileMode:          0o644,
		},
		{
			Reader:            getDestinationConfig(t),
			ContainerFilePath: containerDestinationConfigFilePath,
			FileMode:          0o644,
		},
		{
			Reader:            getScaleConfig(t, scaleOverrides),
			ContainerFilePath: containerScaleConfigFilePath,
			FileMode:          0o644,
		},
	}
}
