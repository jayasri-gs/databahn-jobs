package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/authentication"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"io"
	"net/http"
)

type Client struct {
	Context    context.Context
	RestClient *http.Client
	Endpoint   string
}

type Request struct {
	Alerts []alerts_common.Alert `json:"alerts"`
}

func NewAlertClient(ctx context.Context, appConfig configuration.ConfigReader) (*Client, error) {
	httpClient, err := authentication.GetOAuthHttpClient(ctx, appConfig)
	if err != nil {
		return nil, err
	}
	return &Client{
		Context:    ctx,
		RestClient: httpClient,
		Endpoint:   fmt.Sprintf("%s%s%s", "https://", appConfig.GetString(configuration.ControlPlaneBaseUrl), "/v1/alerts"),
	}, nil
}

func (c *Client) SendAlert(data Request) (io.ReadCloser, error) {
	if c.RestClient == nil {
		return nil, errors.New("empty rest client")
	}
	return c.sendAlertToCPController(data)
}

func (c *Client) sendAlertToCPController(data Request) (io.ReadCloser, error) {
	alertBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	resp, err := c.RestClient.Post(c.Endpoint, "application/json", bytes.NewBuffer(alertBytes))
	if err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (c *Client) SendAlertWithRetry(data Request, retry int) (io.ReadCloser, error) {
	response, err := c.sendAlertToCPController(data)
	if err != nil {
		if retry > 0 {
			logger.GetLoggerWithContext(c.Context).Error("error while sending change flag. retrying", zap.Error(err), zap.Int("retry", retry))
			return c.SendAlertWithRetry(data, retry-1)
		} else {
			return nil, err
		}
	} else {
		return response, nil
	}
}
