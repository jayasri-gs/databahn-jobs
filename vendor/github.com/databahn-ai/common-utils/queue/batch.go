package queue

import (
	"bytes"
	"encoding/gob"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

type BatchQueue[T any] struct {
	maxBatchSize      int
	maxBatchTime      time.Duration
	maxBatchSizeBytes int
	outputBufferSize  int
	inputBufferSize   int
	output            chan []T
	input             chan T
	done              chan struct{}
}

type BatchQueueOption[T any] func(*BatchQueue[T])

func NewBachQueue[T any](opts ...BatchQueueOption[T]) *BatchQueue[T] {
	const (
		defaultBatchSizeBytes   = 100000
		defaultBatchSize        = 100
		defaultBatchTime        = 2 * time.Second
		defaultInputBufferSize  = 10
		defaultOutputBufferSize = 5
	)
	bq := &BatchQueue[T]{
		maxBatchSizeBytes: defaultBatchSizeBytes,
		maxBatchSize:      defaultBatchSize,
		maxBatchTime:      defaultBatchTime,
		inputBufferSize:   defaultInputBufferSize,
		outputBufferSize:  defaultOutputBufferSize,
		done:              make(chan struct{}),
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

func (bq *BatchQueue[T]) Push(item T) {
	bq.input <- item
}

func (bq *BatchQueue[T]) GetOutputChan() chan []T {
	return bq.output
}

func (bq *BatchQueue[T]) Close() {
	close(bq.input)
	<-bq.done
}

func (bq *BatchQueue[T]) startBatching() {
	ticker := time.NewTicker(bq.maxBatchTime)
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
				close(bq.done)
				return
			}
			byteSizeItem, err := getSize(item)
			if err != nil {
				logger.GetLogger().Error("failed to get size of item", zap.Error(err))
			}
			byteSize += byteSizeItem
			items = append(items, item)
			if len(items) >= bq.maxBatchSize || byteSize >= bq.maxBatchSizeBytes {
				ticker.Reset(bq.maxBatchTime)
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
		}
	}
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
