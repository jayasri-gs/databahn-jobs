package redis

import (
	"context"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type HealthCheck struct {
	failure        chan error
	check          chan struct{}
	errorHandler   func()
	successHandler func()
	cli            *Client
	isHealthy      atomic.Bool
	statusChanged  atomic.Bool
}

func NewHealthCheck(client *Client) *HealthCheck {
	f := make(chan error, 100)

	h := HealthCheck{
		failure: f,
		cli:     client,
	}
	h.isHealthy.Store(true)
	go startChecker(&h)
	return &h
}

func startChecker(h *HealthCheck) {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-h.failure:
			if h.IsHealthy() {
				err := h.cli.Ping(context.TODO())
				if err != nil {
					h.isHealthy.Store(false)
					h.statusChanged.Store(true)
				}
			}
		case <-ticker.C:
			if h.statusChanged.Load() {
				if !h.IsHealthy() {
					h.errorHandler()
					h.statusChanged.Store(false)
				} else {
					h.successHandler()
					h.statusChanged.Store(false)
				}
			} else if !h.IsHealthy() {
				ctx := context.TODO()
				logger.GetLogger().Info("checking redis health")
				err := h.cli.Ping(ctx)
				if err == nil {
					err = h.cli.reload(ctx)
					if err == nil {
						h.isHealthy.Store(true)
						h.statusChanged.Store(true)
						logger.GetLogger().Info("redis health success")
					} else {
						logger.GetLogger().Info("redis health reload:error", zap.Error(err))
					}
				} else {
					logger.GetLogger().Info("redis health ping:error", zap.Error(err))
				}
			}
		}
	}
}

func (h *HealthCheck) Error(err error) {
	h.failure <- err
}

func (h *HealthCheck) RegisterErrorCallBack(handler func()) {
	h.errorHandler = handler
}

func (h *HealthCheck) RegisterSuccessCallBack(handler func()) {
	h.successHandler = handler
}

func (h *HealthCheck) IsHealthy() bool {
	return h.isHealthy.Load()
}
