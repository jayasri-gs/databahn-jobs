package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/auth"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type Alert struct {
	//Id                      string `json:"id"`
	Title                   string    `json:"title"`
	Message                 string    `json:"message"`
	CreatedAt               time.Time `json:"createdAt"`
	UpdatedAt               time.Time `json:"updatedAt"`
	FirstObservedAt         time.Time `json:"firstObservedAt"`
	LastObservedAt          time.Time `json:"lastObservedAt"`
	TenantId                string    `json:"tenantId"`
	FunctionalityType       string    `json:"functionalityType"`
	Functionality           string    `json:"functionality"`
	FunctionalityEntityId   string    `json:"functionalityEntityId"`
	FunctionalityEntityName string    `json:"functionalityEntityName"`
	Dismissed               bool      `json:"dismissed"`
	DismissedAt             time.Time `json:"dismissedAt"`
	DismissedBy             string    `json:"dismissedBy"`
	Criticality             string    `json:"criticality"`
}

type AlertRequest struct {
	Alerts []Alert `json:"alerts"`
}

func SendAlertToControlFlag(ctx context.Context, entityArray []alerts_common.AlertEntityObject, title string, message string, functionalityType string, functionality string, severity string) error {
	var alerts []Alert
	for _, entity := range entityArray {
		temp := Alert{
			Title:                   title,
			Message:                 message,
			CreatedAt:               time.Now(),
			UpdatedAt:               time.Now(),
			FirstObservedAt:         time.Now(),
			LastObservedAt:          time.Now(),
			TenantId:                entity.EntityTenantUUId.String(),
			FunctionalityType:       functionalityType,
			Functionality:           functionality,
			FunctionalityEntityId:   entity.EntityId.String(),
			FunctionalityEntityName: entity.EntityName,
			Dismissed:               false,
			DismissedBy:             "",
			Criticality:             severity,
		}
		alerts = append(alerts, temp)
	}
	var alertRequest AlertRequest
	alertRequest.Alerts = alerts
	alertBytes, err := json.Marshal(alertRequest)
	if err != nil {
		return err
	}

	resp, err := trySendingChangeFlag(ctx, 0, alertBytes, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		logger.GetLogger().Info("successfully sent alert call to control plane")
		return nil
	} else {
		body, _ := io.ReadAll(resp.Body)
		logger.GetLogger().Error("error while sending alert call to control plane", zap.String("response", string(body)))
		return fmt.Errorf("[%d] : non success response from data plane", resp.StatusCode)
	}
}

func trySendingChangeFlag(ctx context.Context, attempt int, body []byte, err error) (*http.Response, error) {
	if attempt >= 3 {
		return nil, err
	}
	baseUrl := "https://controller.dev.databahn.app"
	client, err := auth.GetOAuthHttpClient(ctx)
	if err != nil {
		logger.GetLogger().Error("error while getting oauth http client", zap.Error(err))
		return nil, err
	}
	apiUrl := baseUrl + "/v1/alerts"
	resp, err := client.Post(apiUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		resp, err = trySendingChangeFlag(ctx, attempt+1, body, err)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}
