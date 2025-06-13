package changeflag

import (
	"context"
	"fmt"
	"github.com/databahn-ai/db-models/alerts_async"
	"math/rand"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type Acknowledgement struct {
	Status       string `json:"status"`
	EntityId     string `json:"entity_id"`
	EntityName   string `json:"entity_name"`
	RequestId    string `json:"request_id"`
	TenantId     string `json:"tenant_id"`
	Action       string `json:"action"`
	EntityType   string `json:"entity_type"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func (a *Acknowledgement) IsSuccess() bool {
	return a.Status == ack.StatusSuccess
}

func (a *Acknowledgement) NewAlert(dataPlaneId string) (*alerts_async.Alert, error) {
	functionality := mapEntityTypeToAlertFunctionality(a.EntityType)
	entityTypeInLog := strings.ReplaceAll(strings.ToLower(a.EntityType), "_", " ")
	title := fmt.Sprintf("Failed to process %s '%s'.", entityTypeInLog, a.EntityName)
	msg := fmt.Sprintf("Failed to %s %s '%s'.", strings.ToLower(a.Action), entityTypeInLog, a.EntityName)
	return alerts_async.NewAlert(functionality,
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(msg),
		alerts_async.WithErrorCode(alerts_async.DCFE10003, a.ErrorMessage),
		alerts_async.WithEntityDetails(a.EntityId, a.EntityName, dataPlaneId, a.TenantId),
		alerts_async.WithFunctionalityType(alerts_async.ConfigurationProcessingFailure),
		alerts_async.WithCriticality(alerts_async.Critical),
	)
}

func mapEntityTypeToAlertFunctionality(entityType string) alerts_async.Functionality {
	if strings.HasPrefix(entityType, "destination_") {
		return alerts_async.Dispenser
	}
	switch entityType {
	case constants.EntitySource:
		return alerts_async.LogSource
	case constants.EntityRule:
		return alerts_async.VolumeControlRule
	case constants.EntityLookup:
		return alerts_async.Lookup
	case constants.EntityEnrichment:
		return alerts_async.Enrichment
	case constants.EntityTransformer:
		return alerts_async.Transformer
	case constants.EntityRouteProcessor:
		return alerts_async.RouteProcessor
	case constants.EntityInsightsRule:
		return alerts_async.InsightsRule
	case constants.EntityGlobalDestination:
		return alerts_async.GlobalDestination
	default:
		return alerts_async.Unknown
	}
}

func newAcknowledgement(ackStatus, requestId, entityType, entityId, entityName, tenantId, action string, errorMessage string) *Acknowledgement {
	return &Acknowledgement{
		RequestId:    requestId,
		Status:       ackStatus,
		EntityId:     entityId,
		EntityName:   entityName,
		TenantId:     tenantId,
		Action:       action,
		EntityType:   entityType,
		ErrorMessage: errorMessage,
	}
}
func SuccessAcknowledgement(requestId, entityType, entityId, entityName, tenantId, action string) *Acknowledgement {
	return newAcknowledgement(ack.StatusSuccess, requestId, entityType, entityId, entityName, tenantId, action, "")
}

func SuccessAcknowledgementFromChangeFlag(cf ChangeFlag) *Acknowledgement {
	return SuccessAcknowledgement(cf.RequestId, cf.EntityType, cf.EntityId, cf.EntityName, cf.TenantId, cf.Action)
}

func ErrorAcknowledgement(requestId, entityType, entityId, entityName, tenantId, action, errorMessage string) *Acknowledgement {
	return newAcknowledgement(ack.StatusFailure, requestId, entityType, entityId, entityName, tenantId, action, errorMessage)
}

func ErrorAcknowledgementFromChangeFlag(cf ChangeFlag, errorMessage string) *Acknowledgement {
	return ErrorAcknowledgement(cf.RequestId, cf.EntityType, cf.EntityId, cf.EntityName, cf.TenantId, cf.Action, errorMessage)
}

func (t *Trigger) SendAcknowledgements(ctx context.Context, changeFlagAcks []Acknowledgement) {
	if changeFlagAcks == nil {
		logger.GetLogger().Debug("no acknowledgements to send")
		return
	}
	// sleep for random time to ensure cache hit
	time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
	for _, a := range changeFlagAcks {
		if !ackExistsInCache(ctx, a, t.redisUrl) {
			err := t.ackProducer.Produce(ctx, prepareAck(a), nil)
			if err != nil {
				logger.GetLogger().Error("failed to produce ack", zap.Error(err))
				// todo generate alert
			} else {
				setAckInCache(ctx, a, t.redisUrl)
			}
			if t.alertsManager != nil && (!a.IsSuccess()) {
				t.sendAlert(a)
			}
		} else {
			logger.GetLogger().Debug("acknowledgement already exists in cache", zap.Any("ack", a))
		}
	}
}

func (t *Trigger) sendAlert(a Acknowledgement) {
	alert, err := a.NewAlert(t.dataPlaneId)
	if err != nil {
		logger.GetLogger().Error("failed to create alert from acknowledgement", zap.Error(err), zap.Any("ack", a))
	} else {
		t.alertsManager.RecordAlert(alert)
		logger.GetLogger().Info("alert generated from acknowledgement", zap.String("entity_id", a.EntityId),
			zap.String("entity_type", a.EntityType), zap.String("tenant_id", a.TenantId),
			zap.String("error", a.ErrorMessage))
	}
}

func prepareAck(changeFlagAck Acknowledgement) ack.Ack {
	return ack.Ack{
		Type:        constants.AckTypeChangeFlag,
		EntityId:    changeFlagAck.EntityId,
		RequestId:   changeFlagAck.RequestId,
		TenantId:    changeFlagAck.TenantId,
		Action:      changeFlagAck.Action,
		EntityType:  changeFlagAck.EntityType,
		Status:      changeFlagAck.Status,
		Error:       changeFlagAck.ErrorMessage,
		ServiceName: utils.GetEnvOrDefault(constants.ServiceName, ""),
	}
}
