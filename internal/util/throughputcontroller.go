package util

import (
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// ThroughputController is a struct to control the throughput of a process
// It allows you to set a rate limit on how many records can be processed per second
type ThroughputController struct {
	// The number of records to process per second
	rate int
	// The number of records processed in the current second
	count int
	// ticker to control the rate of processing
	ticker *time.Ticker
	// channel to receive increment requests
	incrementChannel chan struct{}
}

// func to create a new throughput object
func NewThroughputController(rate int) *ThroughputController {
	t := &ThroughputController{
		rate:             rate,
		ticker:           time.NewTicker(time.Second),
		incrementChannel: make(chan struct{}),
	}

	go t.startIncrementer()
	return t
}

// function to increment the count
func (t *ThroughputController) IncrementOrWait() {
	t.incrementChannel <- struct{}{}
}

// close increment channel
// which will stop ThroughputController
func (t *ThroughputController) Stop() {
	close(t.incrementChannel)
}

// func with select statement for the ticker & increment channel where increment will be received
func (t *ThroughputController) startIncrementer() {
	for {
		select {
		// case to handle tick ever second
		case <-t.ticker.C:
			logger.GetLogger().Info("events processed in last second", zap.Int("count", t.count))
			t.count = 0
		// case to handle increment request
		case _, ok := <-t.incrementChannel:
			if !ok {
				t.ticker.Stop()
				return
			}
			t.count++
			if t.count == t.rate {
				logger.GetLogger().Info("Rate limit reached, waiting for next tick")
				<-t.ticker.C
				t.count = 0
			}
		}
	}
}
