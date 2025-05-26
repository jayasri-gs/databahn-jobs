package queue

type Queue[T any] struct {
	bufferSize    int
	input         chan T
	useOutputChan bool
	output        chan T
	useCallBack   bool
	callFunc      func(T)
	doneInput     chan struct{}
}

type QueueOption[T any] func(*Queue[T])

func NewQueueWithCallBack[T any](callFunc func(T), opts ...QueueOption[T]) *Queue[T] {
	const (
		defaultBufferSize = 1
	)
	q := &Queue[T]{
		bufferSize: defaultBufferSize,
		callFunc:   callFunc,
	}

	for _, opt := range opts {
		opt(q)
	}

	inputChan := make(chan T, q.bufferSize)

	q.doneInput = make(chan struct{})
	q.input = inputChan
	q.useOutputChan = false
	q.useCallBack = true

	go q.startConsuming()

	return q
}

func NewQueueWithChan[T any](opts ...QueueOption[T]) (*Queue[T], chan T) {
	const (
		defaultBufferSize = 1
	)
	q := &Queue[T]{
		bufferSize: defaultBufferSize,
	}

	for _, opt := range opts {
		opt(q)
	}

	inputChan := make(chan T, q.bufferSize)
	outputChan := make(chan T, q.bufferSize)

	q.doneInput = make(chan struct{})

	q.input = inputChan
	q.output = outputChan
	q.useOutputChan = true
	q.useCallBack = false

	go q.startConsuming()

	return q, q.output
}

func WithBufferSize[T any](bufferSize int) QueueOption[T] {
	return func(q *Queue[T]) {
		q.bufferSize = bufferSize
	}
}

func (q *Queue[T]) Push(item T) {
	q.input <- item
}

func (q *Queue[T]) Close() {
	close(q.input)
	<-q.doneInput
}

func (q *Queue[T]) startConsuming() {
	for item := range q.input {
		if q.useCallBack {
			q.callFunc(item)
		} else {
			q.output <- item
		}
	}
	if q.useOutputChan {
		close(q.output)
	}
	q.doneInput <- struct{}{}
}
