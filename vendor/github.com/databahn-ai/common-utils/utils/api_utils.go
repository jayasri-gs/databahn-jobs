package utils

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func MakePostAPICallWithRetries(
	logger *zap.Logger,
	httpClient *http.Client,
	apiUrl string,
	maxRetryCount int,
	payload []byte,
) ([]byte, *http.Response, error) {
	logger.Info(fmt.Sprintf("Request URL: %s", apiUrl))

	var reqBody io.Reader = http.NoBody
	if len(payload) > 0 {
		reqBody = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(http.MethodPost, apiUrl, reqBody)
	if err != nil {
		return nil, nil, err
	}

	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("mock-data", "False")

	var body []byte
	retryCount := 0
	var resp *http.Response

	for {
		retryCount++
		resp, err = httpClient.Do(req)
		if err != nil {
			return nil, nil, err
		}

		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, resp, err
		}

		logger.Info(fmt.Sprintf("Response Status Code: %v", resp.StatusCode))

		if resp.StatusCode == 429 || resp.StatusCode == 503 {
			logger.Warn(fmt.Sprintf("Too many requests sent to API. Current retry count: %d", retryCount))
			time.Sleep(time.Duration(200 * time.Millisecond))
			if retryCount >= maxRetryCount {
				logger.Warn(fmt.Sprintf("Maximum retries exceeded, breaking import: %d", retryCount))
				return nil, resp, fmt.Errorf("too many requests sent to API")
			}
			continue
		} else if resp.StatusCode != http.StatusOK {
			logger.Error(fmt.Sprintf("API call failed with status: %s, body: %s", resp.Status, string(body)))
			return nil, resp, fmt.Errorf("api call failed with status: %s, body: %s", resp.Status, string(body))
		}

		break
	}

	defer func() {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				logger.Error(fmt.Sprintf("Error closing response body: %v", err))
			}
		}
	}()
	logger.Info(fmt.Sprintf("Response Body: %s", string(body)))
	return body, resp, err
}
