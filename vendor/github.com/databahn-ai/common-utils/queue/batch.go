package queue

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	defaultBatchSizeBytes   = 100000
	defaultBatchSize        = 100
	defaultBatchTime        = 2 * time.Second
	defaultInputBufferSize  = 10
	defaultOutputBufferSize = 5
)

type BatchQueue[T any] struct {
	maxBatchSize      int
	maxBatchTime      time.Duration
	maxBatchSizeBytes int
	outputBufferSize  int
	inputBufferSize   int
	output            chan []T
	input             chan T
	closeWg           *sync.WaitGroup
	reload            chan struct{}
	configMu          sync.RWMutex
}

type BatchQueueOption[T any] func(*BatchQueue[T])

func NewBachQueue[T any](opts ...BatchQueueOption[T]) *BatchQueue[T] {

	bq := &BatchQueue[T]{
		maxBatchSizeBytes: defaultBatchSizeBytes,
		maxBatchSize:      defaultBatchSize,
		maxBatchTime:      defaultBatchTime,
		inputBufferSize:   defaultInputBufferSize,
		outputBufferSize:  defaultOutputBufferSize,
		closeWg:           &sync.WaitGroup{},
		reload:            make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(bq)
	}

	inputChan := make(chan T, bq.inputBufferSize)
	outputChan := make(chan []T, bq.outputBufferSize)

	bq.input = inputChan
	bq.output = outputChan

	go bq.startBatching()

	return bq
}

func WithMaxBatchSize[T any](batchSize int) BatchQueueOption[T] {
	return func(bq *BatchQueue[T]) {
		bq.maxBatchSize = batchSize
	}
}

func WithMaxBatchTime[T any](batchTime time.Duration) BatchQueueOption[T] {
	return func(bq *BatchQueue[T]) {
		bq.maxBatchTime = batchTime
	}
}
func WithMaxBatchSizeBytes[T any](batchSizeBytes int) BatchQueueOption[T] {
	return func(bq *BatchQueue[T]) {
		bq.maxBatchSizeBytes = batchSizeBytes
	}
}

func WithInputBufferSize[T any](inputBufferSize int) BatchQueueOption[T] {
	return func(bq *BatchQueue[T]) {
		bq.inputBufferSize = inputBufferSize
	}
}

func WithOutputBufferSize[T any](outputBufferSize int) BatchQueueOption[T] {
	return func(bq *BatchQueue[T]) {
		bq.outputBufferSize = outputBufferSize
	}
}

func (bq *BatchQueue[T]) Reload(opts ...BatchQueueOption[T]) {
	if len(opts) > 0 {
		bq.configMu.Lock()
		for _, opt := range opts {
			opt(bq)
		}
		bq.configMu.Unlock()
	}
	// Non-blocking signal: one pending reload is enough.
	select {
	case bq.reload <- struct{}{}:
	default:
	}
}

func (bq *BatchQueue[T]) Push(item T) {
	bq.input <- item
}

func (bq *BatchQueue[T]) OnOutput(process func([]T)) {
	bq.closeWg.Add(1)
	for outItem := range bq.output {
		process(outItem)
	}
	bq.closeWg.Done()
}

func (bq *BatchQueue[T]) Close() {
	close(bq.input)
	bq.closeWg.Wait()
}

func (bq *BatchQueue[T]) startBatching() {
	size, batchTime, sizeBytes := bq.getRuntimeBatchConfig()

	ticker := time.NewTicker(batchTime)
	var items []T
	byteSize := 0
	for {
		select {
		case item, ok := <-bq.input:
			if !ok {
				ticker.Stop()
				if len(items) > 0 {
					bq.output <- items
				}
				close(bq.output)
				return
			}
			byteSizeItem, err := getSize(item)
			if err != nil {
				logger.GetLogger().Error("failed to get size of item", zap.Error(err))
			}
			byteSize += byteSizeItem
			items = append(items, item)
			if len(items) >= size || byteSize >= sizeBytes {
				ticker.Reset(batchTime)
				bq.output <- items
				items = nil
				byteSize = 0
			}
		case <-ticker.C:
			if len(items) > 0 {
				bq.output <- items
				items = nil
				byteSize = 0
			}
		case <-bq.reload:
			size, batchTime, sizeBytes = bq.getRuntimeBatchConfig()
			if len(items) > 0 {
				bq.output <- items
			}
			items = nil
			byteSize = 0
			ticker.Reset(batchTime)
		}
	}
}

func (bq *BatchQueue[T]) getRuntimeBatchConfig() (int, time.Duration, int) {
	bq.configMu.RLock()
	defer bq.configMu.RUnlock()

	size := bq.maxBatchSize
	batchTime := bq.maxBatchTime
	sizeBytes := bq.maxBatchSizeBytes

	if size <= 0 {
		size = defaultBatchSize
	}
	if batchTime <= 0 {
		batchTime = defaultBatchTime
	}
	if sizeBytes <= 0 {
		sizeBytes = defaultBatchSizeBytes
	}

	return size, batchTime, sizeBytes
}

func getSize(v interface{}) (int, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	if err != nil {
		return 0, err
	}
	return buf.Len(), nil
}
