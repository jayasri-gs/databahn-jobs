package acknowledgement

import (
	ackPkg "github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func markAckError(ack []db.ChangeFlagAck) {
	ackIds := make([]string, len(ack))
	for i, a := range ack {
		ackIds[i] = a.Id
	}
	err := db.MarkAcksError(ackIds)
	if err != nil {
		logger.GetLogger().Error("error while marking acks processed", zap.Error(err))
	}
}

func markAckSuppressed(ack []db.ChangeFlagAck) {
	ackIds := make([]string, len(ack))
	for i, a := range ack {
		ackIds[i] = a.Id
	}
	err := db.MarkAcksSuppressed(ackIds)
	if err != nil {
		logger.GetLogger().Error("error while marking acks suppressed", zap.Error(err))
	}
}

func markAckProcessed(successfulAck []db.ChangeFlagAck) {
	ackIds := make([]string, len(successfulAck))
	for i, a := range successfulAck {
		ackIds[i] = a.Id
	}
	err := db.MarkAcksProcessed(ackIds)
	if err != nil {
		logger.GetLogger().Error("error while marking acks processed", zap.Error(err))
	}
}

func getStatusInt(ack db.ChangeFlagAck) int {
	var ruleStatus int
	if ack.Status == ackPkg.StatusSuccess {
		if ack.Action == constants.ActionDelete {
			ruleStatus = constants.StatusDisabled
		} else {
			ruleStatus = constants.StatusActive
		}
	}

	if ack.Status == ackPkg.StatusFailure {
		if ack.Action == constants.ActionDelete {
			ruleStatus = constants.StatusErrorDisabling
		} else {
			ruleStatus = constants.StatusErrored
		}
	}

	return ruleStatus
}

func getStatusString(ack db.ChangeFlagAck) (ruleStatus string) {
	if ack.Status == ackPkg.StatusSuccess {
		if ack.Action == constants.ActionDelete {
			ruleStatus = "DISABLED"
		} else {
			ruleStatus = "ACTIVE"
		}
	}

	if ack.Status == ackPkg.StatusFailure {
		if ack.Action == constants.ActionDelete {
			ruleStatus = "DISABLE_ERROR"
		} else {
			ruleStatus = "ERROR"
		}
	}

	return ruleStatus
}
