package cache

import (
	"sync"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type CacheItem[T any] struct {
	Data        T
	CreatedAt   time.Time
	LastSeen    time.Time
	Occurrences int
	mx          sync.RWMutex
}

type Cache[T any] struct {
	cache    map[string]*CacheItem[T]
	mutex    sync.RWMutex
	maxItems int
	ttl      time.Duration
	cleanup  *time.Ticker
	done     chan bool
}

type CacheOption[T any] func(*Cache[T])

func WithCleanupInterval[T any](d time.Duration) CacheOption[T] {
	return func(c *Cache[T]) {
		c.cleanup = time.NewTicker(d)
	}
}

// NewCache creates a new cache with specified TTL, max items and other options
func NewCache[T any](ttl time.Duration, maxItems int, opts ...CacheOption[T]) *Cache[T] {
	cache := &Cache[T]{
		cache:    make(map[string]*CacheItem[T]),
		maxItems: maxItems,
		ttl:      ttl,
		cleanup:  time.NewTicker(1 * time.Minute),
		done:     make(chan bool),
	}

	for _, opt := range opts {
		opt(cache)
	}

	// Start cleanup goroutine
	go cache.cleanupExpired()

	return cache
}

// CacheItem adds an item to the cache using the provided key
func (c *Cache[T]) CacheItem(cacheKey string, data T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// If cache is full, remove oldest item
	if len(c.cache) >= c.maxItems {
		c.evictOldest()
	}

	c.cache[cacheKey] = &CacheItem[T]{
		Data:        data,
		CreatedAt:   time.Now(),
		LastSeen:    time.Now(),
		Occurrences: 1,
		mx:          sync.RWMutex{},
	}

	logger.GetLogger().Debug("Item cached",
		zap.String("key", cacheKey))
}

// GetCachedItem retrieves a cached item by key
func (c *Cache[T]) GetCachedItem(cacheKey string) (T, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if item, exists := c.cache[cacheKey]; exists {
		item.mx.Lock()
		data := item.Data
		item.LastSeen = time.Now()
		item.Occurrences++
		item.mx.Unlock()
		return data, true
	}

	var zero T
	return zero, false
}

// RemoveFromCache removes an item from cache by key
func (c *Cache[T]) RemoveFromCache(cacheKey string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.cache[cacheKey]; exists {
		delete(c.cache, cacheKey)
		logger.GetLogger().Debug("Item removed from cache", zap.String("key", cacheKey))
		return true
	}
	return false
}

// evictOldest removes the oldest item from the cache
func (c *Cache[T]) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, item := range c.cache {
		item.mx.RLock()
		if oldestKey == "" || item.CreatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = item.CreatedAt
		}
		item.mx.RUnlock()
	}

	if oldestKey != "" {
		delete(c.cache, oldestKey)
		logger.GetLogger().Debug("Evicted oldest item from cache", zap.String("key", oldestKey))
	}
}

// cleanupExpired removes expired items from the cache
func (c *Cache[T]) cleanupExpired() {
	for {
		select {
		case <-c.cleanup.C:
			c.mutex.Lock()
			now := time.Now()
			expiredKeys := []string{}

			for key, item := range c.cache {
				item.mx.RLock()
				if now.Sub(item.LastSeen) > c.ttl {
					expiredKeys = append(expiredKeys, key)
				}
				item.mx.RUnlock()
			}

			for _, key := range expiredKeys {
				delete(c.cache, key)
				logger.GetLogger().Debug("Removed expired item from cache", zap.String("key", key))
			}

			c.mutex.Unlock()

		case <-c.done:
			return
		}
	}
}

// Close stops the cleanup goroutine
func (c *Cache[T]) Close() {
	c.cleanup.Stop()
	close(c.done)
}
