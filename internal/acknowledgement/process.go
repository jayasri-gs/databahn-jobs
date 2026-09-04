package acknowledgement

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	ackPkg "github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	ackReadOlderThanSecondsEnv = "ACK_PROCESSOR_ACK_READ_OLDER_THAN_SECONDS"
	ackLookbackDurationEnv     = "ACK_PROCESSOR_ACK_LOOKBACK_DURATION"
)

var errSuppressUnsupportedEntityType = errors.New("unsupported ack entity type should be suppressed")

/*
Flow:
1. Page distinct entity ids using cursor pagination
2. For each page load acknowledgements and prepare map of [entityId][requestId][]acks
3. Get change flags by entity id
4. Get latest cf request for each entity keeping track of suppressed request ids
5. Start processing
	5.1. Get relevant acknowledgement for the latest request id
	5.2. Batch update entity status
	5.3  Capture failed and successful updates
6. Mark acknowledgements processing status : suppressed, processed, error
7. Repeat until no entities remain, then delete old processed records
*/

func ProcessAck() common.JobResult {
	var jobErrors []common.JobError
	now := time.Now()
	olderThan := utils.GetEnvInt(ackReadOlderThanSecondsEnv, 60)
	t := now.Add(time.Duration(-1*olderThan) * time.Second)
	lookbackStart, err := getAckLookbackStart(now)
	if err != nil {
		errorMsg := fmt.Sprintf("error while parsing ack lookback duration: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while parsing ack lookback duration", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	queryBatchSize := getQueryBatchSize()
	entityPageSize := getEntityPageSize()
	entityUpdateBatchSize := getEntityUpdateBatchSize()

	lastEntityID := ""
	totalPages := 0
	entitiesProcessed := 0
	acksProcessed := 0

	fetchEntityIds := func(afterEntityID string, limit int) ([]string, error) {
		return db.GetDistinctEntityIdsToProcess(t, lookbackStart, afterEntityID, limit)
	}

	totalPages, err = forEachEntityPage(entityPageSize, fetchEntityIds, func(entityIds []string, pageNum int) error {
		logger.GetLogger().Debug("processing acknowledgement entity page",
			zap.Int("page", pageNum),
			zap.Int("entity_count", len(entityIds)),
			zap.String("last_entity_id", lastEntityID))

		acks, err := db.GetChangeFlagsByEntityIds(entityIds, t, lookbackStart)
		if err != nil {
			return err
		}
		entitiesProcessed += len(entityIds)
		acksProcessed += len(acks)
		logger.GetLogger().Debug("acknowledgements in page", zap.Int("count", len(acks)))

		if err := processAckPage(acks, queryBatchSize, entityUpdateBatchSize); err != nil {
			return err
		}

		logger.GetLogger().Info("acknowledgement processing progress",
			zap.Int("page", pageNum),
			zap.Int("entities_in_page", len(entityIds)),
			zap.Int("acks_in_page", len(acks)),
			zap.Int("entities_processed", entitiesProcessed),
			zap.Int("acks_processed", acksProcessed))

		lastEntityID = entityIds[len(entityIds)-1]
		return nil
	})
	if err != nil {
		errorMsg := fmt.Sprintf("error while processing acknowledgement pages: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while processing acknowledgement pages", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	if totalPages > 0 {
		logger.GetLogger().Info("acknowledgement processing finished paging",
			zap.Int("pages", totalPages),
			zap.Int("entities_processed", entitiesProcessed),
			zap.Int("acks_processed", acksProcessed))
	}

	err = db.DeleteRecords(t.Add(-1 * time.Hour))
	if err != nil {
		errorMsg := fmt.Sprintf("error while deleting old records: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while deleting old records", zap.Error(err))
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed acknowledgement processing")
		return common.NewJobResultSuccess()
	}

	logger.GetLogger().Info("acknowledgement processing completed with errors", zap.Int("error_count", len(jobErrors)))
	return common.NewJobResultFromErrors(jobErrors)
}

func processAckPage(acks []db.ChangeFlagAck, queryBatchSize, entityUpdateBatchSize int) error {
	mapOfEntityIdToRequestIdToAck := prepareMapOfEntityIdToRequestIdToAck(acks)
	logger.GetLogger().Debug("number of entities in page", zap.Int("count", len(mapOfEntityIdToRequestIdToAck)))

	entityIdToChangeFlags, err := getChangeFlagsForEntities(mapOfEntityIdToRequestIdToAck, queryBatchSize)
	if err != nil {
		return err
	}

	latestEntityIdToRequestId, suppressedReqIds := getLatestEntityToRequestId(entityIdToChangeFlags)

	successfulReqIds, failedReqIds, unsupportedSuppressedReqIds := startProcessing(
		mapOfEntityIdToRequestIdToAck, latestEntityIdToRequestId, entityUpdateBatchSize)
	for reqId := range unsupportedSuppressedReqIds {
		suppressedReqIds[reqId] = struct{}{}
	}

	markAllAcks(mapOfEntityIdToRequestIdToAck, successfulReqIds, failedReqIds, suppressedReqIds, queryBatchSize)
	return nil
}

func getAckLookbackStart(now time.Time) (*time.Time, error) {
	rawDuration := strings.TrimSpace(os.Getenv(ackLookbackDurationEnv))
	if rawDuration == "" {
		return nil, nil
	}

	duration, err := parseAckLookbackDuration(rawDuration)
	if err != nil {
		return nil, err
	}
	if duration <= 0 {
		return nil, fmt.Errorf("%s must be greater than zero", ackLookbackDurationEnv)
	}

	start := now.Add(-duration)
	return &start, nil
}

func parseAckLookbackDuration(rawDuration string) (time.Duration, error) {
	durationValue := strings.TrimSpace(rawDuration)
	if durationValue == "" {
		return 0, fmt.Errorf("%s cannot be empty", ackLookbackDurationEnv)
	}

	if strings.HasSuffix(durationValue, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(durationValue, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid day duration %q: %w", rawDuration, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	duration, err := time.ParseDuration(durationValue)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", rawDuration, err)
	}
	return duration, nil
}

func startProcessing(mapOfEntityIdToRequestIdToAck map[string]map[string][]db.ChangeFlagAck,
	latestEntityIdToRequestId map[string]string, entityUpdateBatchSize int) (map[string]struct{}, map[string]struct{}, map[string]struct{}) {
	failedReqIds := make(map[string]struct{})
	successfulReqIds := make(map[string]struct{})
	suppressedReqIds := make(map[string]struct{})
	collector := newEntityUpdateCollector()

	for entityId, requestIdToAck := range mapOfEntityIdToRequestIdToAck {
		latestReqId := latestEntityIdToRequestId[entityId]
		if latestReqId == "" {
			logger.GetLogger().Error("latest request id not found for entity", zap.String("entityId", entityId))
			continue
		}

		acknowledgement := getRelevantAcknowledgement(requestIdToAck[latestReqId])
		spec, err := entityUpdateSpecForAck(acknowledgement)
		if errors.Is(err, errSuppressUnsupportedEntityType) {
			logger.GetLogger().Info("suppressing unsupported acknowledgement entity type",
				zap.String("entityId", acknowledgement.EntityId), zap.String("type", acknowledgement.EntityType))
			suppressedReqIds[acknowledgement.RequestId] = struct{}{}
			continue
		}
		if err != nil {
			logger.GetLogger().Error("error while resolving entity update", zap.Error(err),
				zap.String("entityId", acknowledgement.EntityId), zap.String("type", acknowledgement.EntityType))
			failedReqIds[acknowledgement.RequestId] = struct{}{}
			continue
		}

		collector.add(spec, acknowledgement.EntityId, acknowledgement.RequestId)
	}

	batchSuccessful, batchFailed := collector.flush(entityUpdateBatchSize)
	for reqId := range batchSuccessful {
		successfulReqIds[reqId] = struct{}{}
	}
	for reqId := range batchFailed {
		failedReqIds[reqId] = struct{}{}
	}

	return successfulReqIds, failedReqIds, suppressedReqIds
}

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
	failed map[string]struct{}, suppressed map[string]struct{}, queryBatchSize int) {
	var successfulAck, failedAck, suppressedAck []db.ChangeFlagAck

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
	markAckProcessed(successfulAck, queryBatchSize)
	markAckError(failedAck, queryBatchSize)
	markAckSuppressed(suppressedAck, queryBatchSize)
}

func getLatestEntityToRequestId(entityIdToChangeFlags map[string][]db.ChangeFlagRequest) (map[string]string, map[string]struct{}) {
	latestEntityIdToRequestId := make(map[string]string)

	for entityId, changeFlags := range entityIdToChangeFlags {
		latestRequest := changeFlags[0]
		for _, cf := range changeFlags {
			if cf.Timestamp > latestRequest.Timestamp {
				latestRequest = cf
			}
		}
		latestEntityIdToRequestId[entityId] = latestRequest.RequestId
	}

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

func getChangeFlagsForEntities(mapOfEntityIdToRequestIdToAck map[string]map[string][]db.ChangeFlagAck, queryBatchSize int) (map[string][]db.ChangeFlagRequest, error) {
	if len(mapOfEntityIdToRequestIdToAck) == 0 {
		return map[string][]db.ChangeFlagRequest{}, nil
	}

	var changeFlags []db.ChangeFlagRequest
	entities := make([]string, 0, len(mapOfEntityIdToRequestIdToAck))
	for entityId := range mapOfEntityIdToRequestIdToAck {
		entities = append(entities, entityId)
	}

	if queryBatchSize <= 0 {
		queryBatchSize = getQueryBatchSize()
	}

	for start := 0; start < len(entities); start += queryBatchSize {
		end := start + queryBatchSize
		if end > len(entities) {
			end = len(entities)
		}
		cf, err := db.GetChangeFlagRequest(entities[start:end])
		if err != nil {
			return nil, err
		}
		changeFlags = append(changeFlags, cf...)
	}

	entityIdToChangeFlags := make(map[string][]db.ChangeFlagRequest)
	for _, cfAck := range changeFlags {
		entityIdToChangeFlags[cfAck.EntityId] = append(entityIdToChangeFlags[cfAck.EntityId], cfAck)
	}
	return entityIdToChangeFlags, nil
}

func prepareMapOfEntityIdToRequestIdToAck(acks []db.ChangeFlagAck) map[string]map[string][]db.ChangeFlagAck {
	mapOfEntityIdToRequestIdToAck := make(map[string]map[string][]db.ChangeFlagAck)
	for _, ack := range acks {
		if _, ok := mapOfEntityIdToRequestIdToAck[ack.EntityId]; !ok {
			mapOfEntityIdToRequestIdToAck[ack.EntityId] = make(map[string][]db.ChangeFlagAck)
		}
		mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId] = append(mapOfEntityIdToRequestIdToAck[ack.EntityId][ack.RequestId], ack)
	}
	return mapOfEntityIdToRequestIdToAck
}

func updateStatus(ack db.ChangeFlagAck) error {
	spec, err := entityUpdateSpecForAck(ack)
	if err != nil {
		return err
	}
	return executeEntityStatusSingleUpdate(spec.table, spec.status, spec.guardKind, ack.EntityId)
}

