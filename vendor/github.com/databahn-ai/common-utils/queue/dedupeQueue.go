package queue

import (
	"errors"
	"time"
)

type KeyFunc[T any] func(T) string

type DedupeQueue[T any] struct {
	maxUniqueItems   int
	maxBatchTime     time.Duration
	outputBufferSize int
	inputBufferSize  int
	output           chan []T
	input            chan T
	keyFunc          KeyFunc[T]
	done             chan struct{}
}

type DedupeQueueOption[T any] func(*DedupeQueue[T])

func NewDedupeQueue[T any](opts ...DedupeQueueOption[T]) (*DedupeQueue[T], error) {
	const (
		defaultMaxItemsSize     = 100
		defaultBatchTime        = 5 * time.Minute
		defaultInputBufferSize  = 10
		defaultOutputBufferSize = 5
	)
	dq := &DedupeQueue[T]{
		maxBatchTime:     defaultBatchTime,
		maxUniqueItems:   defaultMaxItemsSize,
		inputBufferSize:  defaultInputBufferSize,
		outputBufferSize: defaultOutputBufferSize,
		done:             make(chan struct{}),
	}

	for _, opt := range opts {
		opt(dq)
	}

	if dq.keyFunc == nil {
		return nil, errors.New("a key function must be provided for dedupe queue")
	}
	inputChan := make(chan T, dq.inputBufferSize)
	outputChan := make(chan []T, dq.outputBufferSize)

	dq.input = inputChan
	dq.output = outputChan

	go dq.startBatching()

	return dq, nil
}

func WithMaxUniqueItems[T any](size int) DedupeQueueOption[T] {
	return func(aq *DedupeQueue[T]) {
		aq.maxUniqueItems = size
	}
}

func WithMaxBatchDuration[T any](batchTime time.Duration) DedupeQueueOption[T] {
	return func(aq *DedupeQueue[T]) {
		aq.maxBatchTime = batchTime
	}
}
func WithInputBuffSize[T any](inputBufferSize int) DedupeQueueOption[T] {
	return func(bq *DedupeQueue[T]) {
		bq.inputBufferSize = inputBufferSize
	}
}
func WithOutputBuffSize[T any](outputBufferSize int) DedupeQueueOption[T] {
	return func(bq *DedupeQueue[T]) {
		bq.outputBufferSize = outputBufferSize
	}
}
func convertMapToArray[T any](m map[string]T) []T {
	var values []T
	for _, value := range m {
		values = append(values, value)
	}
	return values
}
func (aq *DedupeQueue[T]) Push(item T) {
	aq.input <- item
}

func WithKeyFunc[T any](keyFunc KeyFunc[T]) DedupeQueueOption[T] {
	return func(aq *DedupeQueue[T]) {
		aq.keyFunc = keyFunc
	}
}

func (aq DedupeQueue[T]) OnOutput(process func([]T)) {
	for outItem := range aq.output {
		process(outItem)
	}
	aq.done <- struct{}{}
}

func (aq *DedupeQueue[T]) Close() {
	close(aq.input)
	<-aq.done
}

func (aq *DedupeQueue[T]) startBatching() {
	ticker := time.NewTicker(aq.maxBatchTime)
	var items = make(map[string]T)
	for {
		select {
		case item, ok := <-aq.input:
			if !ok {
				ticker.Stop()
				if len(items) > 0 {
					aq.output <- convertMapToArray(items)
				}
				close(aq.output)
				return
			}
			key := aq.keyFunc(item)
			if _, ok := items[key]; !ok {
				items[key] = item
			}
			if len(items) >= aq.maxUniqueItems {
				aq.output <- convertMapToArray(items)
				items = make(map[string]T)
			}
		case <-ticker.C:
			if len(items) > 0 {
				aq.output <- convertMapToArray(items)
				items = make(map[string]T)
			}
		}
	}
}
