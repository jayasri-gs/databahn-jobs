package pramaan

import (
	"bytes"
	"io"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

const confFile = `
kafka:
  bootstrap_brokers: {{.BootstrapBrokers}}
  processing:
    bootstrap_brokers: {{.BootstrapBrokers}}
  input:
    bootstrap_brokers: {{.BootstrapBrokers}}
urls:
  redis: {{.RedisUrl}}
  redis_sampling: {{.RedisUrl}}
  prometheus: {{.PrometheusUrl}}
dataplane.id:
  a1d97664-275c-4adc-9438-b87d563b868d
secret:
  backend: {{.SecretBackend}}
vault:
  address: {{.VaultAddress}}
  token: {{.VaultToken}}
database:
  secret_name: {{.DatabaseSecretName}}
  host: {{.DatabaseHost}}
  port: {{.DatabasePort}}
  database: {{.DatabaseName}}
open_search:
  os_secret_name: {{.OpenSearchSecretName}}
object:
  backend: {{.ObjectBackend}}
  s3:
    region: {{.ObjectS3Region}}
redis:
  default:
    is_cluster: false
  sampling:
    is_cluster: false
  preview:
    is_cluster: false
`

const destConfFile = `
topic_mapping:
  Kafka: db.destination.kafka
  Elasticsearch: db.destination.elasticsearch
  Snowflake: db.destination.snowflake
  Syslog: db.destination.syslog
  S3: db.destination.s3
  Sandbox: db.destination.sandbox
  AzureEventHub: db.destination.azureeventhub
topic_aggregation_mapping:
  Kafka: db.destination.aggregation.kafka
  Elasticsearch: db.destination.aggregation.elasticsearch
  Snowflake: db.destination.aggregation.snowflake
  Syslog: db.destination.aggregation.syslog
  S3: db.destination.aggregation.s3
  Sandbox: db.destination.aggregation.sandbox
  AzureEventHub: db.destination.aggregation.azureeventhub
staging:
  transform: db.staging.transform
  Sampling: db.staging.sampling
  Suppression: db.staging.suppression
  aggregation: db.staging.aggregation
  vc: db.staging.vc
  enrich: db.staging.enrich
  preprocessing: db.staging.preprocessing
  normalization: db.staging.norm
  sensitive: db.staging.sensitive`

const scaleConfFile = `
schemaless-normalization-service:
  kafka.consumer.count: 1
  max_processing_workers: 1
  buffer_size: 100
  globalDestination.unparsed.max_s3_topics: 1
  globalDestination.unparsed.max_azure_blob_topics: 1
  globalDestination.unparsed.max_snowflake_topics: 1
  drift_check_interval: 10s
  drift_sample_interval: 5s
sampling-rule-engine:
  suppression_key_limit: 100
  suppression_batch_size: 50
  suppression_batch_timeout_ms: 2000
  channel_buffer_size: 1000
data-insights-service:
  aggregation_window: 5
`

const containerAppConfigFilePath string = "/opt/databahn/config/app.yaml"
const containerDestinationConfigFilePath string = "/opt/databahn/config/destination.yaml"
const containerScaleConfigFilePath string = "/opt/databahn/config/scale.yaml"

type Config struct {
	BootstrapBrokers  string
	AuthenticationUrl string
	RedisUrl          string
	PrometheusUrl     string

	SecretBackend string
	VaultAddress  string
	VaultToken    string

	OpenSearchSecretName string
	DatabaseSecretName   string

	DatabaseHost string
	DatabasePort string
	DatabaseName string

	ObjectBackend  string
	ObjectS3Region string
}

/*
ConfigModifier is a function that modifies the Config struct.
It is used to apply various configurations to the service.
*/
type ConfigModifier func(*Config)

func getAppConfig(t TestLogger, config *Config) io.Reader {
	parse, err := template.New("config").Parse(confFile)
	if err != nil {
		t.Fatalf("failed to build config %v", err)
	}
	var tpl bytes.Buffer
	err = parse.Execute(&tpl, config)
	if err != nil {
		t.Fatalf("failed to parse config %v", err)
	}
	return bytes.NewReader(tpl.Bytes())
}

func WithAuthenticationUrl(authenticationUrl string) ConfigModifier {
	return func(config *Config) {
		config.AuthenticationUrl = authenticationUrl
	}
}

func WithObjectStoreS3(region string) ConfigModifier {
	return func(config *Config) {
		config.ObjectBackend = "s3"
		config.ObjectS3Region = region
	}
}

func getDestinationConfig(t TestLogger) io.Reader {
	return bytes.NewReader([]byte(destConfFile))
}

func getScaleConfig(t TestLogger, overrides map[string]string) io.Reader {
	var parsed yaml.Node
	if err := yaml.Unmarshal([]byte(scaleConfFile), &parsed); err != nil {
		t.Fatalf("failed to parse scale config: %v", err)
	}

	for path, value := range overrides {
		setNestedYAMLValue(&parsed, strings.Split(path, "."), value)
	}

	out, err := yaml.Marshal(&parsed)
	if err != nil {
		t.Fatalf("failed to marshal scale config: %v", err)
	}
	return bytes.NewReader(out)
}

// setNestedYAMLValue traverses (or creates) the mapping nodes along segments
// and sets the leaf to a scalar with the given value.
func setNestedYAMLValue(root *yaml.Node, segments []string, value string) {
	node := root
	// Unwrap document node
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		node = node.Content[0]
	}

	for i, seg := range segments {
		if node.Kind != yaml.MappingNode {
			return
		}
		found := false
		for j := 0; j < len(node.Content)-1; j += 2 {
			if node.Content[j].Value == seg {
				if i == len(segments)-1 {
					node.Content[j+1] = &yaml.Node{Kind: yaml.ScalarNode, Value: value}
					return
				}
				node = node.Content[j+1]
				found = true
				break
			}
		}
		if !found {
			keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: seg}
			if i == len(segments)-1 {
				valNode := &yaml.Node{Kind: yaml.ScalarNode, Value: value}
				node.Content = append(node.Content, keyNode, valNode)
				return
			}
			valNode := &yaml.Node{Kind: yaml.MappingNode}
			node.Content = append(node.Content, keyNode, valNode)
			node = valNode
		}
	}
}
