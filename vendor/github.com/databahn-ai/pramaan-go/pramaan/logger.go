package pramaan

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

/*
ServiceLogger is a wrapper around testcontainers.Log that logs to stdout.
It also provides methods to capture logs containing specific strings and wait for those logs to appear.
*/
type ServiceLogger struct {
	testName          string
	captureRequestMap map[string]string
	matchedRequestMap map[string]string
	cmx               sync.RWMutex
	mmx               sync.RWMutex
}

func NewServiceLogger(testName string) *ServiceLogger {
	return &ServiceLogger{
		testName:          testName,
		captureRequestMap: make(map[string]string),
		matchedRequestMap: make(map[string]string)}
}

func (t *ServiceLogger) Accept(log testcontainers.Log) {
	fmt.Printf("testname: %s, logtype: %s, content: %s", t.testName, log.LogType, string(log.Content))
	t.cmx.RLock()
	defer t.cmx.RUnlock()
	for requestId, stringToSearch := range t.captureRequestMap {
		if strings.Contains(string(log.Content), stringToSearch) {
			t.mmx.Lock()
			t.matchedRequestMap[requestId] = string(log.Content)
			t.mmx.Unlock()
		}
	}
}

/*
CaptureLogsContaining captures logs containing the input string and returns a unique request ID.
Returned request ID can be used to wait for the log to appear and to stop capturing logs.
*/
func (t *ServiceLogger) CaptureLogsContaining(inputStr string) string {
	requestId := uuid.New().String()
	t.cmx.Lock()
	t.captureRequestMap[requestId] = inputStr
	t.cmx.Unlock()
	return requestId
}

/*
WaitForCapturedLog waits for the logs to appear specified by request id.
It checks for the logs at regular intervals until the timeout is reached.
If the log appears, it returns the captured log.
If the log does not appear within the timeout, it fails the test with a fatal error.
*/
func (t *ServiceLogger) WaitForCapturedLog(tg TestLogger, captureId string, checkInterval time.Duration, timeout time.Duration) *string {
	t.cmx.RLock()
	msg := t.captureRequestMap[captureId]
	t.cmx.RUnlock()
	timeOutTicker := time.NewTicker(timeout)
	checkTicker := time.NewTicker(checkInterval)
	defer timeOutTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case <-timeOutTicker.C:
			tg.Fatalf("Timeout waiting for message : %s ", msg)
			return nil
		case <-checkTicker.C:
			t.mmx.RLock()
			c, ok := t.matchedRequestMap[captureId]
			t.mmx.RUnlock()
			if ok {
				return &c
			}
		}
	}
}

/*
StopCapture stops capturing logs for the given capture id.
*/
func (t *ServiceLogger) StopCapture(captureId string) {
	t.cmx.Lock()
	delete(t.captureRequestMap, captureId)
	t.cmx.Unlock()
	t.mmx.Lock()
	delete(t.matchedRequestMap, captureId)
	t.mmx.Unlock()
}

type TestLogger interface {
	Fatalf(format string, a ...any)
	Fatale(format string, err error)
	Logf(format string, a ...any)
	Name() string
}

/*
DbTestLogger is a TestLogger implementation that uses a test name as input.
Logs generated with this are prefixed with the test name.
Mainly used for service logs and logs from main of test that takes *testing.M.
*/
type DbTestLogger struct {
	testName string
}

func NewDbTestLogger(testName string) *DbTestLogger {
	return &DbTestLogger{testName: testName}
}

func (t DbTestLogger) Fatalf(format string, a ...any) {
	fmt.Printf("["+t.testName+"]:"+format, a...)
	os.Exit(1)
}

func (t DbTestLogger) Logf(format string, a ...any) {
	fmt.Printf("["+t.testName+"]:"+format+"\n", a...)
}

func (t DbTestLogger) Fatale(format string, err error) {
	fmt.Printf("["+t.testName+"]:"+format, err)
	os.Exit(1)
}

func (t DbTestLogger) Name() string {
	return t.testName
}

/*
GoTestLogger is a TestLogger implementation that uses *testing.T as input.
Logs generated with this are prefixed with the test name.
It is useful for logging in tests where you want to fail the test if an error occurs.
*/
type GoTestLogger struct {
	t *testing.T
}

func NewGoTestLogger(t *testing.T) *GoTestLogger {
	return &GoTestLogger{t: t}
}

func (g GoTestLogger) Fatalf(format string, a ...any) {
	g.t.Fatalf("["+g.t.Name()+"]:"+format, a...)
}

func (g GoTestLogger) Logf(format string, a ...any) {
	g.t.Logf("["+g.t.Name()+"]:"+format, a...)
}

func (g GoTestLogger) Fatale(message string, err error) {
	g.t.Fatalf("[%s]: %s: %v", g.t.Name(), message, err)
}

func (g GoTestLogger) Name() string {
	return g.t.Name()
}
