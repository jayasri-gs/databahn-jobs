package destination

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Splunk connector configuration keys, mirroring backend-service SplunkConstants and the
// external-store connector shape.
const (
	splunkHostKey              = "splunk_host"
	splunkPortKey              = "splunk_port"
	splunkSchemeKey            = "splunk_scheme"
	splunkSchemeAlternateKey   = "scheme"
	splunkSSLCertValidationKey = "ssl_cert_validation"
	splunkIndexKey             = "index"
	splunkTokenKey             = "token"
	splunkSearchTokenKey       = "splunk_search_token"

	defaultSplunkSearchPort = 8089
)

// SplunkSearchConfiguration is persisted under
// search_data_store.configuration.splunkSearchConfiguration for pipeline SPLUNK_HEC stores.
type SplunkSearchConfiguration struct {
	SplunkSearchPort   json.Number `json:"splunkSearchPort"`
	SplunkSearchScheme string      `json:"splunkSearchScheme"`
	SSLCertValidation  *bool       `json:"sslCertValidation"`
	SecretID           string      `json:"secretId"`
	SplunkSearchToken  string      `json:"splunkSearchToken"`
	Index              string      `json:"index"`
}

// SplunkConfig holds the credentials needed to run SPL against a Splunk search head.
type SplunkConfig struct {
	Host              string
	Port              int
	Scheme            string
	Token             string
	SSLCertValidation bool
	Index             string
}

// SplunkConfigFromExternalConnector maps an EXTERNAL_STORAGE connectorConfig onto a
// SplunkConfig. The search token is normally blank here and supplied by
// ApplySplunkCredentialOverrides.
func SplunkConfigFromExternalConnector(connector map[string]string) *SplunkConfig {
	if len(connector) == 0 {
		return nil
	}
	token := strings.TrimSpace(firstSplunkNonEmpty(
		connector[splunkSearchTokenKey],
		connector[splunkTokenKey],
	))
	return &SplunkConfig{
		Host:              strings.TrimSpace(connector[splunkHostKey]),
		Port:              splunkPortFromString(connector[splunkPortKey]),
		Scheme:            normalizeSplunkScheme(firstSplunkNonEmpty(connector[splunkSchemeKey], connector[splunkSchemeAlternateKey])),
		Token:             token,
		SSLCertValidation: splunkSSLCertValidationFromString(connector[splunkSSLCertValidationKey], true),
		Index:             strings.TrimSpace(connector[splunkIndexKey]),
	}
}

// ApplySplunkCredentialOverrides overlays AWS Secrets Manager fields onto a SplunkConfig.
// splunk_search_token is the confidential Splunk attribute in backend-service.
func ApplySplunkCredentialOverrides(cfg *SplunkConfig, credentialOverrides map[string]string) {
	if cfg == nil {
		return
	}
	for k, v := range credentialOverrides {
		switch k {
		case splunkHostKey:
			cfg.Host = strings.TrimSpace(v)
		case splunkPortKey:
			if port := splunkPortFromString(v); port > 0 {
				cfg.Port = port
			}
		case splunkSchemeKey, splunkSchemeAlternateKey:
			if scheme := normalizeSplunkScheme(v); scheme != "" {
				cfg.Scheme = scheme
			}
		case splunkSSLCertValidationKey:
			cfg.SSLCertValidation = splunkSSLCertValidationFromString(v, cfg.SSLCertValidation)
		case splunkIndexKey:
			cfg.Index = strings.TrimSpace(v)
		case splunkTokenKey, splunkSearchTokenKey:
			if strings.TrimSpace(v) != "" {
				cfg.Token = v
			}
		}
	}
}

// Validate reports missing fields using the same wording as the export worker's Splunk
// executor and backend Splunk connector validation.
func (c *SplunkConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("splunk configuration is required")
	}
	if c.Host == "" {
		return fmt.Errorf("splunk_host is required in connector configuration")
	}
	if c.Scheme == "" {
		c.Scheme = "https"
	}
	switch c.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("splunk scheme must be http or https, got %q", c.Scheme)
	}
	if c.Port <= 0 {
		c.Port = defaultSplunkSearchPort
	}
	if c.Token == "" {
		return fmt.Errorf("splunk search token is required")
	}
	return nil
}

func (c *SplunkSearchConfiguration) searchPort() int {
	if c == nil || strings.TrimSpace(c.SplunkSearchPort.String()) == "" {
		return defaultSplunkSearchPort
	}
	n, err := c.SplunkSearchPort.Int64()
	if err != nil || n <= 0 {
		return defaultSplunkSearchPort
	}
	return int(n)
}

func (c *SplunkSearchConfiguration) searchScheme() string {
	return normalizeSplunkScheme(c.SplunkSearchScheme)
}

func (c *SplunkSearchConfiguration) sslCertValidation(defaultValue bool) bool {
	if c == nil || c.SSLCertValidation == nil {
		return defaultValue
	}
	return *c.SSLCertValidation
}

func splunkPortFromString(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultSplunkSearchPort
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultSplunkSearchPort
	}
	return n
}

func normalizeSplunkScheme(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func splunkSSLCertValidationFromString(raw string, defaultValue bool) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "":
		return defaultValue
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return defaultValue
	}
}

func firstSplunkNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
