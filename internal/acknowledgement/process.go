package acknowledgement

import (
	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	ackConst "github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strings"
	"time"
)

/*
Flow:
1. Get all acknowledgements older than a certain time
2. Prepare a map of [entityId][requestId][]acks
3. Get all change flags by entity id
4. Get latest cf request for each entity keeping track of suppressed request ids
5. Start processing
	5.1. Get relevant acknowledgement for the latest request id
	5.2. Update status
	5.3  Capture failed and successful updates
6. Mark all acknowledgements processing status : suppressed, processed, error

*/

func ProcessAck() error {
	olderThan := utils.GetEnvInt("ACK_PROCESSOR_ACK_READ_OLDER_THAN_SECONDS", 60)
	t := time.Now().Add(time.Duration(-1*olderThan) * time.Second)
	// get all records from acknowledgement to be processed
	acks, err := db.GetAllChangeFlagsToBeProcessed(t)
	if err != nil {
		logger.GetLogger().Error("error while getting change flags to be processed", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("acknowledgements to be processed", zap.Int("count", len(acks)))

	// prepare map of [entityId][requestId][]acks
	mapOfEntityIdToRequestIdToAck := prepareMapOfEntityIdToRequestIdToAck(acks)
	logger.GetLogger().Debug("number of entities to be processed", zap.Int("count", len(mapOfEntityIdToRequestIdToAck)))

	// get all change flags for entities
	entityIdToChangeFlags, err := getChangeFlagsForEntities(mapOfEntityIdToRequestIdToAck)
	if err != nil {
		logger.GetLogger().Error("error while getting change flags", zap.Error(err))
		return err
	}

	// get latest cf request for each entity and collect suppressed request ids
	latestEntityIdToRequestId, suppressedReqIds := getLatestEntityToRequestId(entityIdToChangeFlags)

	// process acknowledgements and update status while collecting successful and failed update status
	successfulReqIds, failedReqIds := startProcessing(mapOfEntityIdToRequestIdToAck, latestEntityIdToRequestId)

	// mark acknowledgements as suppressed, processed and errored accordingly
	markAllAcks(mapOfEntityIdToRequestIdToAck, successfulReqIds, failedReqIds, suppressedReqIds)

	// delete processed records older than certain time (an hour )
	return db.DeleteRecords(t.Add(-1 * time.Hour))
}

func startProcessing(mapOfEntityIdToRequestIdToAck map[string]map[string][]db.ChangeFlagAck,
	latestEntityIdToRequestId map[string]string) (map[string]struct{}, map[string]struct{}) {
	failedReqIds := make(map[string]struct{})
	successfulReqIds := make(map[string]struct{})
	for entityId, requestIdToAck := range mapOfEntityIdToRequestIdToAck {
		latestReqId := latestEntityIdToRequestId[entityId]
		if latestReqId == "" {
			logger.GetLogger().Error("latest request id not found for entity", zap.String("entityId", entityId))
			continue
		}

		// loop over all acknowledgements for given entity and latest requestId
		acknowledgement := getRelevantAcknowledgement(requestIdToAck[latestReqId])
		err := updateStatus(acknowledgement)
		if err != nil {
			logger.GetLogger().Error("error while updating status", zap.Error(err),
				zap.String("entityId", acknowledgement.EntityId), zap.String("type", acknowledgement.EntityType))
			failedReqIds[acknowledgement.RequestId] = struct{}{}
		} else {
			successfulReqIds[acknowledgement.RequestId] = struct{}{}
		}
	}
	return successfulReqIds, failedReqIds
}

// gets the relevant acknowledgement for the latest request id
// failed OR latest one
func getRelevantAcknowledgement(acks []db.ChangeFlagAck) db.ChangeFlagAck {
	errorAckCount := 0
	if len(acks) == 0 {
		logger.GetLogger().Error("no acknowledgements found")
		return db.ChangeFlagAck{}
	}
	acknowledgement := acks[0]
	for _, aa := range acks {
		if aa.Status == ackPkg.StatusFailure {
			errorAckCount++
			acknowledgement = aa
		} else if errorAckCount == 0 {
			if aa.Timestamp > acknowledgement.Timestamp {
				acknowledgement = aa
			}
		}
	}
	logger.GetLogger().Debug("failed acknowledgements", zap.Int("count", errorAckCount),
		zap.String("entityId", acknowledgement.EntityId), zap.String("requestId", acknowledgement.RequestId))
	return acknowledgement
}

func markAllAcks(ack map[string]map[string][]db.ChangeFlagAck, successful map[string]struct{},
	failed map[string]struct{}, suppressed map[string]struct{}) {
	var successfulAck, failedAck, suppressedAck []db.ChangeFlagAck

	// collect all acknowledgements for each status by given requestIds
	for _, requestIdToAckMap := range ack {
		for reqId, acks := range requestIdToAckMap {
			if _, ok := successful[reqId]; ok {
				successfulAck = append(successfulAck, acks...)
			}
			if _, ok := suppressed[reqId]; ok {
				suppressedAck = append(suppressedAck, acks...)
			}
			if _, ok := failed[reqId]; ok {
				failedAck = append(failedAck, acks...)
			}
		}
	}

	logger.GetLogger().Debug("process status of acknowledgements", zap.Int("successful", len(successfulAck)),
		zap.Int("failed", len(failedAck)), zap.Int("suppressed", len(suppressedAck)))
	markAckProcessed(successfulAck)
	markAckError(failedAck)
	markAckSuppressed(suppressedAck)
}

func getLatestEntityToRequestId(entityIdToChangeFlags map[string][]db.ChangeFlagRequest) (map[string]string, map[string]struct{}) {
	latestEntityIdToRequestId := make(map[string]string)

	// get latest cf for each entity
	for entityId, changeFlags := range entityIdToChangeFlags {
		latestRequest := changeFlags[0]
		for _, cf := range changeFlags {
			if cf.Timestamp > latestRequest.Timestamp {
				latestRequest = cf
			}
		}
		latestEntityIdToRequestId[entityId] = latestRequest.RequestId
	}

	// collect all ignored change flags
	suppressedRequestIds := make(map[string]struct{})
	for entityId, cf := range entityIdToChangeFlags {
		for _, kk := range cf {
			if latestEntityIdToRequestId[entityId] != kk.RequestId {
				suppressedRequestIds[kk.RequestId] = struct{}{}
			}
		}
	}

	logger.GetLogger().Debug("number of suppressed request ids", zap.Int("count", len(suppressedRequestIds)))
	return latestEntityIdToRequestId, suppressedRequestIds
}

func getChangeFlagsForEntities(mapOfEntityIdToRequestIdToAck map[string]map[string][]db.ChangeFlagAck) (map[string][]db.ChangeFlagRequest, error) {
	var changeFlags []db.ChangeFlagRequest
	var entities []string

	// prepare all entity ids
	for entityId, _ := range mapOfEntityIdToRequestIdToAck {
		entities = append(entities, entityId)
	}

	// prepare queries in batches
	batches := len(entities) / 100
	for i := 0; i < batches; i++ {
		cf, err := db.GetChangeFlagRequest(entities[i*ackConst.QueryBatchSize : (i+1)*ackConst.QueryBatchSize])
		if err != nil {
			return nil, err
		}
		changeFlags = append(changeFlags, cf...)
	}

	// get remaining records from batches
	remaining, err1 := db.GetChangeFlagRequest(entities[batches*100:])
	if err1 != nil {
		return nil, err1
	}
	changeFlags = append(changeFlags, remaining...)

	// create map of entityId to change flag requests
	entityIdToChangeFlags := make(map[string][]db.ChangeFlagRequest)
	for _, cfAck := range changeFlags {
		entityIdToChangeFlags[cfAck.EntityId] = append(entityIdToChangeFlags[cfAck.EntityId], cfAck)
	}
	return entityIdToChangeFlags, nil
}

func prepareMapOfEntityIdToRequestIdToAck(acks []db.ChangeFlagAck) map[string]map[string][]db.ChangeFlagAck {
	mapOfEntityIdToRequestIdToAck := make(map[string]map[string][]db.ChangeFlagAck)
	for _, ack := range acks {
		// if entity id is not present in the map, create a new map
		if _, ok := mapOfEntityIdToRequestIdToAck[ack.EntityId]; !ok {
			mapOfEntityIdToRequestIdToAck[ack.EntityId] = make(map[string][]db.ChangeFlagAck)
			mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId] = append(mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId], ack)
		} else {
			mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId] = append(mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId], ack)
		}
	}
	return mapOfEntityIdToRequestIdToAck
}

func updateStatus(ack db.ChangeFlagAck) error {
	// handle destination cf separately
	if strings.HasPrefix(ack.EntityType, "destination_") {
		err := updateDestination(ack)
		if err != nil {
			return err
		}
	}

	switch ack.EntityType {
	case utilConst.EntityLookup:
		err := handleLookup(ack)
		if err != nil {
			return err
		}
	case utilConst.EntityRule:
		err := handleRule(ack)
		if err != nil {
			return err
		}
	case utilConst.EntitySource:
		err := handleSource(ack)
		if err != nil {
			return err
		}
	case utilConst.EntityEnrichment:
		err := handleEnrichment(ack)
		if err != nil {
			return err
		}
	case utilConst.EntityTransformer:
		err := handleTransformer(ack)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateDestination(ack db.ChangeFlagAck) error {
	statusV2 := getStatusString(ack)
	err := config.GetDB().Table("destination").Where("id = ? AND status not in (?,?)", ack.EntityId, statusV2, constants.StatusDeleted).
		Update("status", statusV2).Error
	if err != nil {
		logger.GetLogger().Error("error while updating destination status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("destination status updated", zap.String("entityId", ack.EntityId), zap.String("status", statusV2))
	return nil
}

func handleTransformer(ack db.ChangeFlagAck) error {
	statusV2 := getStatusString(ack)
	err := config.GetDB().Table("data_transformation").Where("id = ? AND status not in (?,?)", ack.EntityId, statusV2, constants.StatusDeleted).
		Update("status", statusV2).Error
	if err != nil {
		logger.GetLogger().Error("error while updating transformer status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("transformer status updated", zap.String("entityId", ack.EntityId), zap.String("status", statusV2))
	return nil
}

func handleEnrichment(ack db.ChangeFlagAck) error {
	enrichmentStatus := getStatusString(ack)
	err := config.GetDB().Table("enrichment").Where("id = ? AND status not in (?,?)", ack.EntityId, enrichmentStatus, constants.StatusDeleted).
		Update("status", enrichmentStatus).Error
	if err != nil {
		logger.GetLogger().Error("error while updating enrichment status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("enrichment status updated", zap.String("entityId", ack.EntityId), zap.String("status", enrichmentStatus))
	return nil
}

func handleSource(ack db.ChangeFlagAck) error {
	sourceStatusV2 := getStatusString(ack)
	err := config.GetDB().Table("log_source").Where("id = ? AND status not in (?,?)", ack.EntityId, sourceStatusV2, constants.StatusDeleted).
		Update("status", sourceStatusV2).Error
	if err != nil {
		logger.GetLogger().Error("error while updating source status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("source status updated", zap.String("entityId", ack.EntityId), zap.String("status", sourceStatusV2))
	return nil
}

func handleLookup(ack db.ChangeFlagAck) error {
	lookupStatus := getStatusString(ack)
	err := config.GetDB().Table("lookup").Where("id = ? AND status != ?", ack.EntityId, lookupStatus).Update("status", lookupStatus).Error
	if err != nil {
		logger.GetLogger().Error("error while updating lookup status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("lookup status updated", zap.String("entityId", ack.EntityId), zap.String("status", lookupStatus))
	return nil
}

func handleRule(ack db.ChangeFlagAck) error {
	ruleStatusV2 := getStatusString(ack)
	err := config.GetDB().Table("vc_rule").Where("id = ? and status not in (?,?)", ack.EntityId, ruleStatusV2, constants.StatusDeleted).
		Update("status", ruleStatusV2).Error
	if err != nil {
		logger.GetLogger().Error("error while updating rule status", zap.Error(err))
		return err
	}
	logger.GetLogger().Debug("rule status updated", zap.String("entityId", ack.EntityId), zap.String("status", ruleStatusV2))
	return nil
}
