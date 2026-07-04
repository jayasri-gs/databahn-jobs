package changeflag

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/utils"
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
	timeoutMillis := utils.GetEnvInt("CHANGE_FLAG_SECRET_HTTP_TIMEOUT_MILLIS", 10000)
	if timeoutMillis <= 0 {
		timeoutMillis = 10000
	}
	client = &http.Client{
		Timeout: time.Duration(timeoutMillis) * time.Millisecond,
	}
	dpBaseUrl = configReader.GetString("urls.data_plane_controller_internal_base_url")
	if dpBaseUrl == "" {
		return errors.New("internal data plane base url is empty")
	}

	log := logger.GetLogger()
	log.Debug("changeflag secret client initialized",
		zap.Int("baseUrlLen", len(dpBaseUrl)),
		zap.Bool("baseUrlTrailingSlash", len(dpBaseUrl) > 0 && dpBaseUrl[len(dpBaseUrl)-1] == '/'),
		zap.Int("httpTimeoutMillis", timeoutMillis))
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
	log := logger.GetLogger()

	totalSecretRefs := 0
	for _, ids := range secretIdsByTenant {
		totalSecretRefs += len(ids)
	}
	if totalSecretRefs == 0 {
		log.Debug("changeflag LoadSecrets: skipping data plane secret fetch",
			zap.String("reason", "no secret ids in input"),
			zap.Int("tenantMapSize", len(secretIdsByTenant)))
		return nil, nil
	}

	if client == nil {
		log.Debug("changeflag LoadSecrets: initializing HTTP client for secret fetch")
		err := initClient(configReader)
		if err != nil {
			log.Debug("changeflag LoadSecrets: init client failed", zap.Error(err))
			return nil, err
		}
	}

	tenantCount := len(secretIdsByTenant)
	for tenant, ids := range secretIdsByTenant {
		n := len(ids)
		log.Debug("changeflag LoadSecrets: input tenant secret id summary",
			zap.String("tenantId", tenant),
			zap.Int("secretIdCount", n))
	}
	log.Debug("changeflag LoadSecrets: starting batch fetch",
		zap.Int("tenantCount", tenantCount),
		zap.Int("totalSecretIdReferences", totalSecretRefs))

	searchRequests := batchSecretIds(secretIdsByTenant, 20)
	if len(searchRequests) == 0 {
		log.Debug("changeflag LoadSecrets: skipping data plane secret fetch",
			zap.String("reason", "batching produced no requests"),
			zap.Int("totalSecretRefs", totalSecretRefs))
		return nil, nil
	}
	log.Debug("changeflag LoadSecrets: batched requests",
		zap.Int("batchCount", len(searchRequests)))
	for i, req := range searchRequests {
		log.Debug("changeflag LoadSecrets: batch detail",
			zap.Int("batchIndex", i),
			zap.String("tenantId", req.TenantId),
			zap.Int("secretIdsInBatch", len(req.SecretIds)))
	}

	var secrets []*Secrets
	for batchIdx, request := range searchRequests {
		tenantId := request.TenantId
		log.Debug("changeflag LoadSecrets: fetching batch",
			zap.Int("batchIndex", batchIdx),
			zap.String("tenantId", tenantId),
			zap.Int("secretIdCount", len(request.SecretIds)))
		batchStart := time.Now()
		secResp, err := loadRemoteSecrets(request)
		log.Debug("changeflag LoadSecrets: batch HTTP finished",
			zap.Int("batchIndex", batchIdx),
			zap.String("tenantId", tenantId),
			zap.Duration("elapsed", time.Since(batchStart)),
			zap.Bool("error", err != nil))
		responseSecrets := Secrets{
			TenantId: tenantId,
			Secrets:  make(map[string]map[string]string),
			Errors:   make(map[string]string),
		}
		if err != nil {
			log.Debug("changeflag LoadSecrets: batch failed, aborting",
				zap.Int("batchIndex", batchIdx),
				zap.String("tenantId", tenantId),
				zap.Error(err))
			return nil, err
		}
		for id, sec := range secResp.Secrets {
			responseSecrets.Secrets[id] = sec
			log.Debug("changeflag LoadSecrets: secret entry resolved (values omitted)",
				zap.String("tenantId", tenantId),
				zap.Int("configKeyCount", len(sec)))
		}
		for id, errStr := range secResp.Errors {
			responseSecrets.Errors[id] = errStr
			log.Debug("changeflag LoadSecrets: secret entry error from API",
				zap.String("tenantId", tenantId),
				zap.String("errorMessage", errStr))
		}
		log.Debug("changeflag LoadSecrets: batch aggregate",
			zap.Int("batchIndex", batchIdx),
			zap.String("tenantId", tenantId),
			zap.Int("resolvedCount", len(responseSecrets.Secrets)),
			zap.Int("errorCount", len(responseSecrets.Errors)))
		secrets = append(secrets, &responseSecrets)
	}
	log.Debug("changeflag LoadSecrets: complete",
		zap.Int("responseShardCount", len(secrets)))
	return secrets, nil
}

func loadRemoteSecrets(request secretsRequest) (*secretsResponse, error) {
	log := logger.GetLogger()

	url := dpBaseUrl + "/internal/v1/secrets"
	requestBody, err := json.Marshal(request)
	if err != nil {
		log.Debug("changeflag loadRemoteSecrets: marshal request failed",
			zap.String("tenantId", request.TenantId),
			zap.Int("secretIdCount", len(request.SecretIds)),
			zap.Error(err))
		return nil, err
	}

	maxRetries := utils.GetEnvInt("MAX_RETRIES_ON_CHANGE_FLAG_SECRET_FETCH_ERROR", 3)
	waitMillisMultiplier := utils.GetEnvInt("WAIT_MILLIS_MULTIPLIER_ON_CHANGE_FLAG_SECRET_FETCH_ERROR", 500)
	log.Debug("changeflag loadRemoteSecrets: retry policy",
		zap.String("tenantId", request.TenantId),
		zap.Int("maxRetries", maxRetries),
		zap.Int("waitMillisMultiplier", waitMillisMultiplier),
		zap.Int("requestBodyBytes", len(requestBody)))
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debug("changeflag loadRemoteSecrets: attempt",
			zap.String("tenantId", request.TenantId),
			zap.Int("attempt", attempt),
			zap.Int("maxRetries", maxRetries),
			zap.String("method", "POST"),
			zap.String("path", "/internal/v1/secrets"),
			zap.Int("secretIdCount", len(request.SecretIds)))
		attemptStart := time.Now()
		data := bytes.NewReader(requestBody)
		req, err := http.NewRequest("POST", url, data)
		if err != nil {
			log.Debug("changeflag loadRemoteSecrets: NewRequest failed",
				zap.String("tenantId", request.TenantId),
				zap.Int("attempt", attempt),
				zap.Error(err))
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		log.Info("changeflag loadRemoteSecrets: data plane controller request",
			zap.String("target", "data_plane_controller"),
			zap.String("method", req.Method),
			zap.String("url", url),
			zap.String("tenantId", request.TenantId),
			zap.Int("attempt", attempt),
			zap.Int("secretIdCount", len(request.SecretIds)),
			zap.Int("requestBodyBytes", len(requestBody)))
		log.Debug("changeflag loadRemoteSecrets: data plane controller request metadata",
			zap.String("target", "data_plane_controller"),
			zap.String("method", req.Method),
			zap.String("url", url),
			zap.String("headerContentType", req.Header.Get("Content-Type")),
			zap.Int("requestBodyBytes", len(requestBody)),
			zap.Int("attempt", attempt))

		resp, err := client.Do(req)
		log.Debug("changeflag loadRemoteSecrets: HTTP round-trip finished",
			zap.String("tenantId", request.TenantId),
			zap.Int("attempt", attempt),
			zap.Duration("elapsed", time.Since(attemptStart)),
			zap.Bool("transportError", err != nil))
		if err != nil {
			lastErr = err
			if attempt < maxRetries {
				log.Debug("changeflag loadRemoteSecrets: transport error, sleeping before retry",
					zap.String("tenantId", request.TenantId),
					zap.Int("attempt", attempt),
					zap.Duration("backoff", time.Duration(attempt*waitMillisMultiplier)*time.Millisecond),
					zap.Error(err))
				log.Error("failed to fetch secrets, retrying",
					zap.Int("attempt", attempt),
					zap.Int("maxRetries", maxRetries),
					zap.Error(err),
					zap.String("tenantId", request.TenantId))
				time.Sleep(time.Duration(attempt*waitMillisMultiplier) * time.Millisecond)
				continue
			}
			log.Debug("changeflag loadRemoteSecrets: transport error, retries exhausted",
				zap.String("tenantId", request.TenantId),
				zap.Error(err))
			return nil, err
		}

		if resp.StatusCode == http.StatusOK {
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Debug("changeflag loadRemoteSecrets: OK response body read",
				zap.String("tenantId", request.TenantId),
				zap.Int("attempt", attempt),
				zap.Int("statusCode", resp.StatusCode),
				zap.Int("bodyBytes", len(bodyBytes)),
				zap.Error(err))
			if err != nil {
				return nil, err
			}
			var secResp secretsResponse
			err = json.Unmarshal(bodyBytes, &secResp)
			if err != nil {
				log.Debug("changeflag loadRemoteSecrets: JSON unmarshal failed",
					zap.String("tenantId", request.TenantId),
					zap.Int("attempt", attempt),
					zap.Error(err))
				return nil, err
			}
			nSec := 0
			if secResp.Secrets != nil {
				nSec = len(secResp.Secrets)
			}
			nErr := 0
			if secResp.Errors != nil {
				nErr = len(secResp.Errors)
			}
			log.Debug("changeflag loadRemoteSecrets: parsed response (secret values omitted)",
				zap.String("tenantId", request.TenantId),
				zap.Int("attempt", attempt),
				zap.Int("secretsMapSize", nSec),
				zap.Int("errorsMapSize", nErr))
			return &secResp, nil
		}

		// For non-OK status codes, retry on 5xx errors but not 4xx errors
		if resp.StatusCode >= 500 && attempt < maxRetries {
			errorResp, er := io.ReadAll(resp.Body)
			if er == nil {
				log.Debug("changeflag loadRemoteSecrets: 5xx response, will retry",
					zap.String("tenantId", request.TenantId),
					zap.Int("attempt", attempt),
					zap.Int("statusCode", resp.StatusCode),
					zap.String("status", resp.Status),
					zap.Int("errorBodyBytes", len(errorResp)))
				log.Error("failed to fetch secrets, retrying",
					zap.Int("attempt", attempt),
					zap.Int("maxRetries", maxRetries),
					zap.String("status", resp.Status),
					zap.String("response", string(errorResp)),
					zap.String("tenantId", request.TenantId))
			}
			resp.Body.Close()
			log.Debug("changeflag loadRemoteSecrets: sleeping after 5xx",
				zap.String("tenantId", request.TenantId),
				zap.Duration("backoff", time.Duration(attempt*waitMillisMultiplier)*time.Millisecond))
			time.Sleep(time.Duration(attempt*waitMillisMultiplier) * time.Millisecond)
			continue
		}

		// For 4xx errors or final attempt, return error
		errorResp, er := io.ReadAll(resp.Body)
		log.Debug("changeflag loadRemoteSecrets: non-retryable or final failure path",
			zap.String("tenantId", request.TenantId),
			zap.Int("attempt", attempt),
			zap.Int("statusCode", resp.StatusCode),
			zap.String("status", resp.Status),
			zap.Bool("readBodyOk", er == nil),
			zap.Int("errorBodyBytes", len(errorResp)))
		if er == nil {
			log.Error("failed to fetch secrets", zap.String("status", resp.Status),
				zap.String("response", string(errorResp)), zap.String("tenantId", request.TenantId))
		}
		resp.Body.Close()
		return nil, errors.New("failed to fetch secrets with code " + resp.Status)
	}

	log.Debug("changeflag loadRemoteSecrets: exited retry loop with lastErr",
		zap.String("tenantId", request.TenantId),
		zap.Error(lastErr))
	return nil, lastErr
}

func batchSecretIds(secretIdsByTenantId map[string][]string, batchSize int) []secretsRequest {
	log := logger.GetLogger()
	log.Debug("changeflag batchSecretIds: start",
		zap.Int("batchSize", batchSize),
		zap.Int("tenantCount", len(secretIdsByTenantId)))
	var batches []secretsRequest
	for tenantId, secretIds := range secretIdsByTenantId {
		n := len(secretIds)
		batchNum := 0
		for i := 0; i < n; i += batchSize {
			end := i + batchSize
			if end > n {
				end = n
			}
			log.Debug("changeflag batchSecretIds: slice",
				zap.String("tenantId", tenantId),
				zap.Int("tenantSecretTotal", n),
				zap.Int("batchIndexForTenant", batchNum),
				zap.Int("rangeStart", i),
				zap.Int("rangeEnd", end),
				zap.Int("sliceLen", end-i))
			batches = append(batches, secretsRequest{
				SecretIds: secretIds[i:end],
				TenantId:  tenantId,
			})
			batchNum++
		}
	}
	log.Debug("changeflag batchSecretIds: done", zap.Int("totalBatches", len(batches)))
	return batches
}
