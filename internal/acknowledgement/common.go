package acknowledgement

import (
	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func markAckError(ack []db.ChangeFlagAck, batchSize int) error {
	return markAcksByIDs(ack, batchSize, db.MarkAcksError)
}

func markAckSuppressed(ack []db.ChangeFlagAck, batchSize int) error {
	return markAcksByIDs(ack, batchSize, db.MarkAcksSuppressed)
}

func markAckProcessed(successfulAck []db.ChangeFlagAck, batchSize int) error {
	return markAcksByIDs(successfulAck, batchSize, db.MarkAcksProcessed)
}

func markAcksByIDs(acks []db.ChangeFlagAck, batchSize int, markFn func([]string) error) error {
	if len(acks) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = getQueryBatchSize()
	}

	ackIds := make([]string, len(acks))
	for i, a := range acks {
		ackIds[i] = a.Id
	}

	var markErr error
	for start := 0; start < len(ackIds); start += batchSize {
		end := start + batchSize
		if end > len(ackIds) {
			end = len(ackIds)
		}
		if err := markFn(ackIds[start:end]); err != nil {
			logger.GetLogger().Error("error while marking acks", zap.Error(err))
			markErr = err
		}
	}
	return markErr
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
