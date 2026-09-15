package destination

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DestTypeSplunkHEC = "SPLUNK_HEC"

	// Pipeline Splunk destination configuration keys, mirroring backend-service
	// SplunkConnectorConfigBuilder / SplunkConstants.
	pipelineSplunkSearchHostKey  = "splunk_search_host"
	pipelineSplunkHECEndpointKey = "splunk_hec_endpoint"
)

// LoadPipelineSplunkConfig resolves search-head credentials for a pipeline SPLUNK_HEC
// destination (DATABAHN_DESTINATION search stores). Host comes from the destination;
// port, scheme, TLS, and the search token secret come from the linked store's
// splunkSearchConfiguration. Mirrors LoadPipelineSentinelConfig and backend
// SplunkConnectorConfigBuilder / SplunkDatastoreSearchConfigMerger precedence.
func LoadPipelineSplunkConfig(
	ctx context.Context,
	db *gorm.DB,
	destID, dataStoreID, tenantID uuid.UUID,
	searchCfg *SplunkSearchConfiguration,
) (*SplunkConfig, error) {
	merged, err := LoadMergedConfiguration(ctx, db, destID, tenantID)
	if err != nil {
		return nil, err
	}
	cfg, err := pipelineSplunkConfig(merged, searchCfg)
	if err != nil {
		return nil, fmt.Errorf("pipeline Splunk destination %s: %w", destID, err)
	}
	applyPipelineSplunkToken(cfg, merged, searchCfg)
	if searchCfg != nil && strings.TrimSpace(searchCfg.SecretID) != "" {
		overrides, err := ResolveCredentialOverrides(ctx, db, searchCfg.SecretID, dataStoreID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("resolve Splunk store secret: %w", err)
		}
		ApplySplunkCredentialOverrides(cfg, overrides)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline Splunk destination %s: %w", destID, err)
	}
	return cfg, nil
}

func pipelineSplunkConfig(config map[string]string, searchCfg *SplunkSearchConfiguration) (*SplunkConfig, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("Splunk pipeline destination configuration is empty")
	}
	host, err := splunkSearchHostFromDestination(config)
	if err != nil {
		return nil, err
	}
	port := defaultSplunkSearchPort
	scheme := "https"
	sslCertValidation := true
	index := ""
	if searchCfg != nil {
		port = searchCfg.searchPort()
		if s := searchCfg.searchScheme(); s != "" {
			scheme = s
		}
		sslCertValidation = searchCfg.sslCertValidation(true)
		index = strings.TrimSpace(searchCfg.Index)
	}
	return &SplunkConfig{
		Host:              host,
		Port:              port,
		Scheme:            scheme,
		SSLCertValidation: sslCertValidation,
		Index:             index,
	}, nil
}

func splunkSearchHostFromDestination(config map[string]string) (string, error) {
	if host := strings.TrimSpace(config[pipelineSplunkSearchHostKey]); host != "" {
		return host, nil
	}
	endpoint := strings.TrimSpace(config[pipelineSplunkHECEndpointKey])
	if endpoint == "" {
		return "", fmt.Errorf("%s or %s is required on the Splunk destination", pipelineSplunkSearchHostKey, pipelineSplunkHECEndpointKey)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("%s %q is not a valid URL: %w", pipelineSplunkHECEndpointKey, endpoint, err)
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return "", fmt.Errorf("%s %q has no host", pipelineSplunkHECEndpointKey, endpoint)
	}
	return host, nil
}

// applyPipelineSplunkToken resolves the search token with backend precedence:
// legacy inline splunkSearchToken, then destination inline/secret (merged config),
// then the datastore secret applied afterward by the caller.
func applyPipelineSplunkToken(cfg *SplunkConfig, merged map[string]string, searchCfg *SplunkSearchConfiguration) {
	if cfg == nil {
		return
	}
	if searchCfg != nil {
		cfg.Token = strings.TrimSpace(searchCfg.SplunkSearchToken)
	}
	if token := strings.TrimSpace(firstSplunkNonEmpty(
		merged[splunkSearchTokenKey],
		merged[splunkTokenKey],
	)); token != "" {
		cfg.Token = token
	}
}
