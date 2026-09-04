package acknowledgement

import (
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
)

const (
	ackProcessorQueryBatchSizeEnv       = "ACK_PROCESSOR_QUERY_BATCH_SIZE"
	ackProcessorEntityPageSizeEnv       = "ACK_PROCESSOR_ENTITY_PAGE_SIZE"
	ackProcessorEntityUpdateBatchSizeEnv = "ACK_PROCESSOR_ENTITY_UPDATE_BATCH_SIZE"

	defaultEntityPageSize        = 100
	defaultEntityUpdateBatchSize = 100
)

func getQueryBatchSize() int {
	size := utils.GetEnvInt(ackProcessorQueryBatchSizeEnv, constants.QueryBatchSize)
	if size <= 0 {
		return constants.QueryBatchSize
	}
	return size
}

func getEntityPageSize() int {
	size := utils.GetEnvInt(ackProcessorEntityPageSizeEnv, defaultEntityPageSize)
	if size <= 0 {
		return defaultEntityPageSize
	}
	return size
}

func getEntityUpdateBatchSize() int {
	size := utils.GetEnvInt(ackProcessorEntityUpdateBatchSizeEnv, defaultEntityUpdateBatchSize)
	if size <= 0 {
		return defaultEntityUpdateBatchSize
	}
	return size
}
