package changeflag

import (
	"context"
	"fmt"

	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/redis"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var redisClient *redis.Client

func getRedisClient(url string, isRedisClusterMode bool) (*redis.Client, error) {
	if redisClient == nil {
		var err error
		redisClient, err = redis.NewClient(url, "changeFlagAck", isRedisClusterMode)
		if err != nil {
			return nil, err
		}
		return redisClient, nil
	}
	return redisClient, nil
}

func ackExistsInCache(ctx context.Context, ack Acknowledgement, redisUrl string, redisClusterMode bool) bool {
	client, err := getRedisClient(redisUrl, redisClusterMode)
	if err != nil {
		logger.GetLogger().Error("failed to get redis client", zap.Error(err))
		return false
	}
	exists, err := client.Exists(ctx, prepareKey(ack))
	if err != nil {
		logger.GetLogger().Error("error while checking key", zap.Error(err))
		return false
	}
	return exists
}

func setAckInCache(ctx context.Context, ack Acknowledgement, redisUrl string, isRedisClusterMode bool) {
	client, err := getRedisClient(redisUrl, isRedisClusterMode)
	if err != nil {
		logger.GetLogger().Error("failed to get redis client", zap.Error(err))
		return
	}
	err = client.Set(ctx, prepareKey(ack), "true", constants.AckCacheTTL)
	if err != nil {
		logger.GetLogger().Error("error while setting key", zap.Error(err))
	}
}

// key format: ack#changeFlag#<entityType>#<tenantId>#<requestId>#<status>
func prepareKey(a Acknowledgement) string {
	if a.Status == ack.StatusFailure {
		errorMsgPart := a.ErrorMessage
		if len(a.ErrorMessage) > 100 {
			errorMsgPart = a.ErrorMessage[:100]
		}
		return fmt.Sprintf("ack#changeFlag#%s#%s#%s#%s#%s", a.EntityType, a.TenantId, a.RequestId, a.Status, errorMsgPart)
	}
	return fmt.Sprintf("ack#changeFlag#%s#%s#%s#%s", a.EntityType, a.TenantId, a.RequestId, a.Status)
}
