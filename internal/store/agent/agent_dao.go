package agent

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Cache struct {
	Agents map[string]*Agent
	mu     sync.Mutex
}

var cache *Cache

func initCache() {
	cache = &Cache{}
	cache.Agents = make(map[string]*Agent)
	// invalidate cache evert 15 mins
	ticker := time.NewTicker(15 * time.Minute)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				cache.mu.Lock()
				cache.Agents = make(map[string]*Agent)
				cache.mu.Unlock()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

}

func GetFromCache(ctx context.Context, id, tenantId uuid.UUID) *Agent {
	if cache == nil {
		initCache()
	}
	if a, ok := cache.Agents[id.String()]; ok {
		return a
	}
	a, err := Get(ctx, id, tenantId)
	if err != nil {
		return nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.Agents[id.String()] = a
	return a
}

func Get(ctx context.Context, nodeId, tenantId uuid.UUID) (a *Agent, err error) {
	err = config.GetDB().WithContext(ctx).Where("id = ? AND tenant_id = ? ", nodeId, tenantId).Find(&a).Error
	return a, err
}

func HeartBeat(ctx context.Context, id, tenantId uuid.UUID) error {
	return config.GetDB().WithContext(ctx).Model(&Agent{}).Where("id = ? AND tenant_id = ?", id, tenantId).Update("heartbeat_at", time.Now().UTC()).Error
}

func Update(ctx context.Context, a *Agent, id, tenantId uuid.UUID) error {
	return config.GetDB().WithContext(ctx).Where("id = ? AND tenant_id = ?", id, tenantId).Save(&a).Error
}
