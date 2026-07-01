package db

import (
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

type ChangeFlagAck struct {
	Id            string `json:"id"`
	RequestId     string `json:"request_id"`
	EntityId      string `json:"entity_id"`
	Status        string `json:"status"`
	EntityType    string `json:"entity_type"`
	EntityVersion string `json:"entity_version"`
	ServiceName   string `json:"service_name"`
	TenantId      string `json:"tenant_id"`
	Action        string `json:"action"`
	Timestamp     string `json:"timestamp"`
	Error         string `json:"error"`
	ProcessStatus string `json:"process_status"`
}

func GetAllChangeFlagsToBeProcessed(timestampOlderThan time.Time, timestampNewerThan *time.Time) ([]ChangeFlagAck, error) {
	var changeFlagAcks []ChangeFlagAck
	query := config.GetDB().Table(constants.TableChangeFlagAck).
		Where("process_status IN (?,?) AND timestamp < ?", constants.StatusPending, constants.StatusErrored, timestampOlderThan)
	if timestampNewerThan != nil {
		query = query.Where("timestamp >= ?", *timestampNewerThan)
	}
	logger.GetLogger().Info("change flag ack query params",
		zap.String("process_status_1", constants.StatusPending),
		zap.String("process_status_2", constants.StatusErrored),
		zap.Time("timestamp_older_than", timestampOlderThan),
		zap.Any("timestamp_newer_than", timestampNewerThan),
	)
	err := query.Find(&changeFlagAcks).Error
	return changeFlagAcks, err
}

func MarkAcksProcessed(ids []string) error {
	return config.GetDB().Table(constants.TableChangeFlagAck).Where("id IN ?", ids).Update("process_status", constants.StatusProcessed).
		Update("process_status", constants.StatusProcessed).Error
}

func MarkAcksError(ids []string) error {
	return config.GetDB().Table(constants.TableChangeFlagAck).Where("id IN ?", ids).
		Update("process_status", constants.StatusErrored).Error
}

func MarkAcksSuppressed(ids []string) error {
	return config.GetDB().Table(constants.TableChangeFlagAck).Where("id IN ?", ids).
		Update("process_status", constants.StatusSuppressed).Error
}

func DeleteRecords(olderThan time.Time) error {
	return config.GetDB().Table(constants.TableChangeFlagAck).Unscoped().
		Delete(&ChangeFlagAck{}, "timestamp < ? AND process_status = ?", olderThan, "PROCESSED").Error
}
