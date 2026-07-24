package replaymanager

import (
	"sync"
	"sync/atomic"
)

var (
	interrupted atomic.Bool

	workersWgMu sync.Mutex
	workersWg   *sync.WaitGroup
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
