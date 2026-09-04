package acknowledgement

import (
	"errors"
	"strings"

	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type entityUpdateGuard int

const (
	guardExcludeDeleted entityUpdateGuard = iota
	guardExcludeStatusOnly
)

type entityUpdateKey struct {
	table     string
	status    string
	guardKind entityUpdateGuard
}

type pendingEntityUpdate struct {
	entityID  string
	requestID string
}

type entityUpdateCollector struct {
	updates map[entityUpdateKey][]pendingEntityUpdate
}

func newEntityUpdateCollector() *entityUpdateCollector {
	return &entityUpdateCollector{updates: make(map[entityUpdateKey][]pendingEntityUpdate)}
}

func (c *entityUpdateCollector) add(spec entityUpdateSpec, entityID, requestID string) {
	key := entityUpdateKey{table: spec.table, status: spec.status, guardKind: spec.guardKind}
	c.updates[key] = append(c.updates[key], pendingEntityUpdate{entityID: entityID, requestID: requestID})
}

func (c *entityUpdateCollector) flush(batchSize int) (map[string]struct{}, map[string]struct{}) {
	successfulReqIds := make(map[string]struct{})
	failedReqIds := make(map[string]struct{})

	if batchSize <= 0 {
		batchSize = getEntityUpdateBatchSize()
	}

	for key, items := range c.updates {
		for start := 0; start < len(items); start += batchSize {
			end := start + batchSize
			if end > len(items) {
				end = len(items)
			}
			batch := items[start:end]
			entityIDs := make([]string, len(batch))
			for i, item := range batch {
				entityIDs[i] = item.entityID
			}

			err := executeEntityStatusBatchUpdate(key.table, key.status, key.guardKind, entityIDs)
			if err != nil {
				logger.GetLogger().Error("error while batch updating entity status, falling back to single updates",
					zap.Error(err), zap.String("table", key.table), zap.String("status", key.status))
				for _, item := range batch {
					if updateErr := executeEntityStatusSingleUpdate(key.table, key.status, key.guardKind, item.entityID); updateErr != nil {
						logger.GetLogger().Error("error while updating entity status", zap.Error(updateErr),
							zap.String("entityId", item.entityID), zap.String("table", key.table))
						failedReqIds[item.requestID] = struct{}{}
						continue
					}
					successfulReqIds[item.requestID] = struct{}{}
				}
				continue
			}

			for _, item := range batch {
				successfulReqIds[item.requestID] = struct{}{}
			}
		}
	}

	return successfulReqIds, failedReqIds
}

type entityUpdateSpec struct {
	table     string
	status    string
	guardKind entityUpdateGuard
}

func entityUpdateSpecForAck(ack db.ChangeFlagAck) (entityUpdateSpec, error) {
	if strings.HasPrefix(ack.EntityType, "destination_") {
		return entityUpdateSpec{table: "destination", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	}

	switch ack.EntityType {
	case utilConst.EntityLookup:
		return entityUpdateSpec{table: "lookup", status: getStatusString(ack), guardKind: guardExcludeStatusOnly}, nil
	case utilConst.EntityInsightsRule:
		return entityUpdateSpec{table: "insights_rule", status: getStatusString(ack), guardKind: guardExcludeStatusOnly}, nil
	case utilConst.EntityRule:
		return entityUpdateSpec{table: "vc_rule", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntitySource:
		return entityUpdateSpec{table: "log_source", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityEnrichment:
		return entityUpdateSpec{table: "enrichment", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityTransformer:
		return entityUpdateSpec{table: "data_transformation", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityRouteProcessor:
		return entityUpdateSpec{table: "route_processor", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntitySensitiveData:
		return entityUpdateSpec{table: "sensitive_data_config", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityGlobalDestination:
		return entityUpdateSpec{table: "global_destination_config", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityCustomNormalization:
		return entityUpdateSpec{table: "custom_normalization", status: getStatusString(ack), guardKind: guardExcludeDeleted}, nil
	case utilConst.EntityPipeline, utilConst.EntityDataReplay, "data-replay", "aif_workflow", utilConst.EntityAlertConfig:
		return entityUpdateSpec{}, errSuppressUnsupportedEntityType
	default:
		return entityUpdateSpec{}, errors.New("Ack does not support entity type:" + ack.EntityType)
	}
}

func executeEntityStatusBatchUpdate(table, status string, guard entityUpdateGuard, entityIDs []string) error {
	if len(entityIDs) == 0 {
		return nil
	}
	query := config.GetDB().Table(table)
	switch guard {
	case guardExcludeDeleted:
		return query.Where("id IN ? AND status NOT IN (?,?)", entityIDs, status, constants.StatusDeleted).
			Update("status", status).Error
	case guardExcludeStatusOnly:
		return query.Where("id IN ? AND status != ?", entityIDs, status).
			Update("status", status).Error
	default:
		return errors.New("unknown entity update guard")
	}
}

func executeEntityStatusSingleUpdate(table, status string, guard entityUpdateGuard, entityID string) error {
	query := config.GetDB().Table(table)
	switch guard {
	case guardExcludeDeleted:
		return query.Where("id = ? AND status NOT IN (?,?)", entityID, status, constants.StatusDeleted).
			Update("status", status).Error
	case guardExcludeStatusOnly:
		return query.Where("id = ? AND status != ?", entityID, status).
			Update("status", status).Error
	default:
		return errors.New("unknown entity update guard")
	}
}
