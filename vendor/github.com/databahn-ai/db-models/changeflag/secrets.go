package changeflag

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type WithSecret interface {
	GetSecretId() string
	AddConfig(extraConfig map[string]string)
}

var client *http.Client
var dpBaseUrl string

func initClient(configReader configuration.ConfigReader) error {
	client = &http.Client{}
	dpBaseUrl = configReader.GetString("urls.data_plane_controller_internal_base_url")
	if dpBaseUrl == "" {
		return errors.New("internal data plane base url is empty")
	}
	return nil
}

type Secrets struct {
	TenantId string
	Secrets  map[string]map[string]string `json:"secrets"`
	Errors   map[string]string            `json:"errors"`
}

type secretsResponse struct {
	Secrets map[string]map[string]string `json:"secrets"`
	Errors  map[string]string            `json:"errors"`
}

type secretsRequest struct {
	SecretIds []string `json:"secretIds"`
	TenantId  string   `json:"tenantId"`
}

func LoadSecrets(configReader configuration.ConfigReader, secretIdsByTenant map[string][]string) ([]*Secrets, error) {
	if client == nil {
		err := initClient(configReader)
		if err != nil {
			return nil, err
		}
	}

	searchRequests := batchSecretIds(secretIdsByTenant, 20)
	var secrets []*Secrets
	for _, request := range searchRequests {
		tenantId := request.TenantId
		secResp, err := loadRemoteSecrets(request)
		responseSecrets := Secrets{
			TenantId: tenantId,
			Secrets:  make(map[string]map[string]string),
			Errors:   make(map[string]string),
		}
		if err != nil {
			errorMessage := err.Error()
			for _, secretId := range request.SecretIds {
				responseSecrets.Errors[secretId] = errorMessage
			}
		} else {
			for id, sec := range secResp.Secrets {
				responseSecrets.Secrets[id] = sec
			}
			for id, errStr := range secResp.Errors {
				responseSecrets.Errors[id] = errStr
			}
		}
		secrets = append(secrets, &responseSecrets)
	}
	return secrets, nil
}

func loadRemoteSecrets(request secretsRequest) (*secretsResponse, error) {
	url := dpBaseUrl + "/internal/v1/secrets"
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	data := bytes.NewReader(requestBody)
	req, err := http.NewRequest("POST", url, data)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		errorResp, er := io.ReadAll(resp.Body)
		if er == nil {
			logger.GetLogger().Error("failed to fetch secrets", zap.String("status", resp.Status),
				zap.String("response", string(errorResp)), zap.String("tenantId", string(requestBody)))
		}
		return nil, errors.New("failed to fetch secrets with code " + resp.Status)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var secResp secretsResponse
	err = json.Unmarshal(bodyBytes, &secResp)
	if err != nil {
		return nil, err
	}
	return &secResp, nil
}

func batchSecretIds(secretIdsByTenantId map[string][]string, batchSize int) []secretsRequest {
	var batches []secretsRequest
	for tenantId, secretIds := range secretIdsByTenantId {
		for i := 0; i < len(secretIds); i += batchSize {
			end := i + batchSize
			if end > len(secretIds) {
				end = len(secretIds)
			}
			batches = append(batches, secretsRequest{
				SecretIds: secretIds[i:end],
				TenantId:  tenantId,
			})
		}
	}
	return batches
}
