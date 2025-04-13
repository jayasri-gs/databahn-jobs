package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var awsMetaDataServer = "http://169.254.169.254"
var awsMetaDataEndpoint = "/latest/meta-data/"
var awsTokenEndpoint = "/latest/api/token"

var headerTokenGenKey = "X-aws-ec2-metadata-token-ttl-seconds"
var headerTokenGenValue = "21600"
var headerAuthKey = "X-aws-ec2-metadata-token"

func GetAvailabilityZoneId(ctx context.Context) (string, error) {
	resp, statusCode, err := getData(constants.EndpointAvailabilityZoneId)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Info("error while making request for availablity zone id", zap.String("reason", err.Error()))
		return "", err
	}
	if statusCode != http.StatusOK {
		logger.GetLoggerWithContext(ctx).Error("request completed with non ok status code", zap.Int("status", statusCode), zap.String("response", resp))
		return "", errors.New("response from api is not ok. Response is " + resp)
	}
	return resp, nil
}

func GetAvailabilityZone(ctx context.Context) (string, error) {
	resp, statusCode, err := getData(constants.EndpointAvailabilityZone)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while making request for availablity zone", zap.Error(err))
		return "", err
	}
	if statusCode != http.StatusOK {
		logger.GetLoggerWithContext(ctx).Error("request completed with non ok status code", zap.Int("status", statusCode), zap.String("response", resp))
		return "", errors.New("response from api is not ok. Response is " + resp)
	}
	return resp, nil
}

func GetRegion(ctx context.Context) (string, error) {
	resp, statusCode, err := getData(constants.EndpointRegion)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while making request", zap.Error(err))
		return "", err
	}
	if statusCode != http.StatusOK {
		logger.GetLoggerWithContext(ctx).Error("request completed with non ok status code", zap.Int("status", statusCode), zap.String("response", resp))
		return "", errors.New("response from api is not ok. Response is " + resp)
	}
	return resp, nil
}

func getToken() (string, error) {
	url := fmt.Sprintf("%s%s", awsMetaDataServer, awsTokenEndpoint)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set(headerTokenGenKey, headerTokenGenValue)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", errors.New("request status not ok")
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

func getData(endpoint string) (response string, statusCode int, err error) {
	token, err := getToken()
	if err != nil {
		return "", 0, err
	}
	url := fmt.Sprintf("%s%s%s", awsMetaDataServer, awsMetaDataEndpoint, endpoint)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set(headerAuthKey, token)
	if err != nil {
		return "", 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	return string(body), resp.StatusCode, nil
}
