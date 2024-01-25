package lookup

import (
	"github.com/databahn-ai/databahn-jobs/internal/replay/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var Cache FiFoCache

type FiFoCache struct {
	Keys   []string
	Values map[string]any
}

// Init function that initialises the cache and logger
func InitCache() {

	Cache = FiFoCache{
		Keys:   []string{},
		Values: make(map[string]interface{}),
	}

}

func (fc *FiFoCache) Set(key string, value any, traceId string) {

	if len(fc.Keys) >= 10 {

		delete(fc.Values, fc.Keys[0])
		fc.Keys = fc.Keys[1:]
		logger.GetLogger().Info("Cache is full, deleting the oldest key", zap.String("key", key), zap.String("traceId", traceId))
	}
	if _, exists := fc.Values[key]; !exists {
		fc.Keys = append(fc.Keys, key)
		logger.GetLogger().Info("Adding new key to cache", zap.String("key", utils.ReplaceChars(key)), zap.String("traceId", traceId))
	}
	fc.Values[key] = value
}

func (fc *FiFoCache) Get(key string, traceId string) (any, bool) {
	value, exists := fc.Values[key]
	logger.GetLogger().Info("Getting key from cache", zap.String("key", utils.ReplaceChars(key)), zap.String("traceId", traceId))
	return value, exists
}

func (fc *FiFoCache) Delete(key string, traceId string) {
	logger.GetLogger().Info("Deleting key from cache", zap.String("key", key), zap.String("traceId", traceId))
	delete(fc.Values, key)
}

func (fc *FiFoCache) Len(traceId string) int {
	logger.GetLogger().Info("Getting length of cache", zap.String("traceId", traceId))
	return len(fc.Keys)
}

func (fc *FiFoCache) Iter(traceId zap.Field) <-chan any {
	ch := make(chan any)
	go func() {
		for _, key := range fc.Keys {
			ch <- fc.Values[key]
		}
		close(ch)
	}()
	return ch
}
