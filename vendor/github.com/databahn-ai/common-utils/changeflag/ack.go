package changeflag

import (
	"context"
	"math/rand"
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
	RequestId    string `json:"request_id"`
	TenantId     string `json:"tenant_id"`
	Action       string `json:"action"`
	EntityType   string `json:"entity_type"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func newAcknowledgement(ackStatus, requestId, entityType, entityId, tenantId, action string, errorMessage string) *Acknowledgement {
	return &Acknowledgement{
		RequestId:    requestId,
		Status:       ackStatus,
		EntityId:     entityId,
		TenantId:     tenantId,
		Action:       action,
		EntityType:   entityType,
		ErrorMessage: errorMessage,
	}
}
func SuccessAcknowledgement(requestId, entityType, entityId, tenantId, action string) *Acknowledgement {
	return newAcknowledgement(ack.StatusSuccess, requestId, entityType, entityId, tenantId, action, "")
}

func ErrorAcknowledgement(requestId, entityType, entityId, tenantId, action, errorMessage string) *Acknowledgement {
	return newAcknowledgement(ack.StatusFailure, requestId, entityType, entityId, tenantId, action, errorMessage)
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
		} else {
			logger.GetLogger().Debug("acknowledgement already exists in cache", zap.Any("ack", a))
		}
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
