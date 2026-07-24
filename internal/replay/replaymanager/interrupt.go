package replaymanager

import (
	"sync"
	"sync/atomic"
)

var (
	interrupted atomic.Bool

	workersWgMu sync.Mutex
	workersWg   *sync.WaitGroup

	shutdownCompleteMu     sync.Mutex
	shutdownComplete       chan struct{}
	shutdownCompleteClosed bool
)

// MarkInterrupted signals replay workers to stop. Called from the shutdown hook.
func MarkInterrupted() {
	interrupted.Store(true)
}

// IsInterrupted reports whether a graceful shutdown is in progress.
func IsInterrupted() bool {
	return interrupted.Load()
}

// ResetInterrupted clears shutdown state. Intended for tests.
func ResetInterrupted() {
	interrupted.Store(false)
	resetShutdownComplete()
}

// NotifyShutdownComplete signals that the shutdown hook has finished, including status publish.
func NotifyShutdownComplete() {
	shutdownCompleteMu.Lock()
	defer shutdownCompleteMu.Unlock()
	initShutdownComplete()
	if !shutdownCompleteClosed {
		shutdownCompleteClosed = true
		close(shutdownComplete)
	}
}

// WaitForShutdownComplete blocks until the shutdown hook has finished when shutdown was triggered.
func WaitForShutdownComplete() {
	if !IsInterrupted() {
		return
	}
	shutdownCompleteMu.Lock()
	ch := shutdownComplete
	if shutdownComplete == nil {
		shutdownComplete = make(chan struct{})
		ch = shutdownComplete
	}
	shutdownCompleteMu.Unlock()
	<-ch
}

func initShutdownComplete() {
	if shutdownComplete == nil {
		shutdownComplete = make(chan struct{})
	}
}

func resetShutdownComplete() {
	shutdownCompleteMu.Lock()
	defer shutdownCompleteMu.Unlock()
	shutdownComplete = make(chan struct{})
	shutdownCompleteClosed = false
}

// RegisterWorkersWaitGroup registers the wait group used by replay file workers.
func RegisterWorkersWaitGroup(wg *sync.WaitGroup) {
	workersWgMu.Lock()
	defer workersWgMu.Unlock()
	workersWg = wg
}

// WaitForWorkers blocks until all registered replay workers have exited.
func WaitForWorkers() {
	workersWgMu.Lock()
	wg := workersWg
	workersWgMu.Unlock()
	if wg != nil {
		wg.Wait()
	}
}
