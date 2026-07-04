//go:build integration

package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type ExternalAlertNotificationState struct {
	NotificationCount    int
	LastNotificationTime int64
	LastActivationTime   int64
	FirstObservedAt      int64
}

func BuildExternalAlert(tenantID, entityID, entityName string) (*alerts_async.Alert, error) {
	return BuildExternalAlertWithTitle(tenantID, entityID, entityName, "notifications integration test alert")
}

func BuildExternalAlertWithTitle(tenantID, entityID, entityName, title string) (*alerts_async.Alert, error) {
	return alerts_async.NewAlert(
		alerts_async.LogSource,
		alerts_async.WithEntityDetails(entityID, entityName, uuid.NewString(), tenantID),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.ConfigurationProcessingFailure),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage("notifications-for-alerts integration test"),
		alerts_async.WithErrorCode(alerts_async.DIOE30001, "integration test"),
		alerts_async.WithAlertType(alerts_async.External),
	)
}

func BuildInternalAlert(entityID, entityName string) (*alerts_async.Alert, error) {
	return alerts_async.NewAlert(
		alerts_async.LogSource,
		alerts_async.WithEntityDetails(entityID, entityName, uuid.NewString(), common.DatabahnTenantId),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.ConfigurationProcessingFailure),
		alerts_async.WithTitle("internal notifications integration test alert"),
		alerts_async.WithMessage("notifications-for-alerts internal integration test"),
		alerts_async.WithErrorCode(alerts_async.DIOE30001, "integration test"),
		alerts_async.WithAlertType(alerts_async.Internal),
	)
}

func IndexExternalAlert(ctx context.Context, t *testing.T, openSearch *pramaan.OpenSearchPramaan, alert *alerts_async.Alert) {
	t.Helper()
	IndexExternalAlertAt(ctx, t, openSearch, alert, time.Now().UTC().Add(-5*time.Minute))
}

func IndexExternalAlertAt(ctx context.Context, t *testing.T, openSearch *pramaan.OpenSearchPramaan, alert *alerts_async.Alert, observedAt time.Time) {
	t.Helper()
	IndexExternalAlertWithState(ctx, t, openSearch, alert, observedAt, ExternalAlertNotificationState{})
}

func IndexExternalAlertWithState(
	ctx context.Context,
	t *testing.T,
	openSearch *pramaan.OpenSearchPramaan,
	alert *alerts_async.Alert,
	observedAt time.Time,
	state ExternalAlertNotificationState,
) {
	t.Helper()
	indexAlertAt(ctx, t, openSearch, common.AlertsIndex, alert, observedAt, state)
}

func IndexInternalAlert(ctx context.Context, t *testing.T, openSearch *pramaan.OpenSearchPramaan, alert *alerts_async.Alert) {
	t.Helper()
	indexAlertAt(ctx, t, openSearch, common.AlertsIndexInternal, alert, time.Now().UTC().Add(-5*time.Minute), ExternalAlertNotificationState{})
}

func indexAlertAt(
	ctx context.Context,
	t *testing.T,
	openSearch *pramaan.OpenSearchPramaan,
	index string,
	alert *alerts_async.Alert,
	observedAt time.Time,
	state ExternalAlertNotificationState,
) {
	t.Helper()

	observedAtMillis := observedAt.UTC().UnixMilli()
	firstObservedAtMillis := observedAtMillis
	if state.FirstObservedAt > 0 {
		firstObservedAtMillis = state.FirstObservedAt
	}
	alert.LastObservedAt = observedAtMillis
	alert.FirstObservedAt = firstObservedAtMillis
	alert.CreatedAt = observedAtMillis
	alert.UpdatedAt = observedAtMillis
	alert.NotificationCount = state.NotificationCount
	alert.LastNotificationTime = state.LastNotificationTime
	alert.LastActivationTime = state.LastActivationTime

	document, err := alertDocument(alert)
	if err != nil {
		t.Fatalf("marshal alert document: %v", err)
	}

	jsonData, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal alert json: %v", err)
	}

	req := opensearchapi.IndexRequest{
		Index:      index,
		DocumentID: alert.Id,
		Body:       bytes.NewReader(jsonData),
		Refresh:    "true",
	}
	resp, err := req.Do(ctx, openSearch.GetClient())
	if err != nil {
		t.Fatalf("index alert: %v", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("index alert failed, status %d: %s", resp.StatusCode, string(body))
	}
}

func alertDocument(alert *alerts_async.Alert) (map[string]interface{}, error) {
	data, err := json.Marshal(alert)
	if err != nil {
		return nil, err
	}
	var document map[string]interface{}
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	return document, nil
}
