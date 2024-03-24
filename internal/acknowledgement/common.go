package acknowledgement

import (
	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"

	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func markAckError(ack []db.ChangeFlagAck) {
	ackIds := make([]string, len(ack))
	for i, a := range ack {
		ackIds[i] = a.Id
	}
	numBatches := len(ackIds) / constants.QueryBatchSize
	for i := 0; i < numBatches; i++ {
		err := db.MarkAcksError(ackIds[i*constants.QueryBatchSize : (i+1)*constants.QueryBatchSize])
		if err != nil {
			logger.GetLogger().Error("error while marking acks errored", zap.Error(err))
		}
	}

	if len(ackIds)%constants.QueryBatchSize != 0 {
		err := db.MarkAcksError(ackIds[numBatches*constants.QueryBatchSize:])
		if err != nil {
			logger.GetLogger().Error("error while marking acks errored", zap.Error(err))
		}
	}
}

func markAckSuppressed(ack []db.ChangeFlagAck) {
	ackIds := make([]string, len(ack))
	for i, a := range ack {
		ackIds[i] = a.Id
	}
	numBatches := len(ackIds) / constants.QueryBatchSize
	for i := 0; i < numBatches; i++ {
		err := db.MarkAcksSuppressed(ackIds[i*constants.QueryBatchSize : (i+1)*constants.QueryBatchSize])
		if err != nil {
			logger.GetLogger().Error("error while marking acks suppressed", zap.Error(err))
		}
	}

	if len(ackIds)%constants.QueryBatchSize != 0 {
		err := db.MarkAcksSuppressed(ackIds[numBatches*constants.QueryBatchSize:])
		if err != nil {
			logger.GetLogger().Error("error while marking acks suppressed", zap.Error(err))
		}
	}
}

func getStatusStringFromInt(status int) string {
	switch status {
	case utilConst.StatusActive:
		return "ACTIVE"
	case utilConst.StatusDisabled:
		return "DISABLED"
	case utilConst.StatusErrored:
		return "ERRORED"
	case utilConst.StatusErrorDisabling:
		return "DISABLE_ERROR"
	default:
		logger.GetLogger().Error("unknown status", zap.Int("status", status))
	}
	return ""
}

func markAckProcessed(successfulAck []db.ChangeFlagAck) {
	ackIds := make([]string, len(successfulAck))
	for i, a := range successfulAck {
		ackIds[i] = a.Id
	}
	numBatches := len(ackIds) / constants.QueryBatchSize
	for i := 0; i < numBatches; i++ {
		err := db.MarkAcksProcessed(ackIds[i*constants.QueryBatchSize : (i+1)*constants.QueryBatchSize])
		if err != nil {
			logger.GetLogger().Error("error while marking acks processed", zap.Error(err))
		}
	}

	if len(ackIds)%constants.QueryBatchSize != 0 {
		err := db.MarkAcksProcessed(ackIds[numBatches*constants.QueryBatchSize:])
		if err != nil {
			logger.GetLogger().Error("error while marking acks processed", zap.Error(err))
		}
	}
}

func getStatusInt(ack db.ChangeFlagAck) int {
	var status int
	if ack.Status == ackPkg.StatusSuccess {
		if ack.Action == utilConst.ActionDelete {
			status = utilConst.StatusDisabled
		} else {
			status = utilConst.StatusActive
		}
	} else {
		if ack.Status == ackPkg.StatusFailure {
			if ack.Action == utilConst.ActionDelete {
				status = utilConst.StatusErrorDisabling
			} else {
				status = utilConst.StatusErrored
			}
		} else {
			logger.GetLogger().Error("unknown status", zap.String("ack_status", ack.Status))
		}
	}
	return status
}

func getStatusString(ack db.ChangeFlagAck) (status string) {
	if ack.Status == ackPkg.StatusSuccess {
		if ack.Action == utilConst.ActionDelete {
			status = "DISABLED"
		} else {
			status = "ACTIVE"
		}
	} else {
		if ack.Status == ackPkg.StatusFailure {
			if ack.Action == utilConst.ActionDelete {
				status = "DISABLE_ERROR"
			} else {
				status = "ERRORED"
			}
		} else {
			logger.GetLogger().Error("unknown status", zap.String("ack_status", ack.Status))
		}
	}
	return status
}
