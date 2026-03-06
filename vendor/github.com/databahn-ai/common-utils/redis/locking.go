package redis

import (
	"context"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type LockPool struct {
	redisCli *Client
}

func NewLockPool(redisCli *Client) *LockPool {
	return &LockPool{redisCli: redisCli}
}

func (l *LockPool) Lock(ctx context.Context, key string, id string, ttl time.Duration) (bool, error) {
	setSuccess, err := l.redisCli.SetNx(ctx, key, id, ttl)
	if err != nil {
		logger.GetLogger().Error("failed to set lock", zap.Error(err), zap.String("key", key), zap.String("id", id))
	}
	return setSuccess, err
}

func (l *LockPool) Unlock(ctx context.Context, key string, id string) {
	existingVal, err := l.redisCli.Get(ctx, key)
	if err != nil {
		logger.GetLogger().Error("failed to get lock", zap.Error(err), zap.String("key", key))
		return
	}
	if existingVal == id {
		_, err := l.redisCli.DeleteKey(ctx, key)
		if err != nil {
			logger.GetLogger().Error("failed to delete lock", zap.Error(err), zap.String("key", key))
		}
	}
}
