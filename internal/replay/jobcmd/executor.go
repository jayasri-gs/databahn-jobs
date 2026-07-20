package jobcmd

import (
	"context"
	"fmt"
	commConst "github.com/databahn-ai/common-utils/constants"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"

	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/ecryption"
	"github.com/databahn-ai/databahn-jobs/internal/replay/lookup"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/processor"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/replay/source/dbaws"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var (
	replayInterrupted  atomic.Bool
	replayShutdownOnce sync.Once
	publishReplayStatus = processor.ProduceStatus
)

func ExecuteReplayJob(input model.Message) common.JobResult {
	var jobErrors []common.JobError

	ctx := context.Background()
	//	input := ReadInputData()
	lookup.InitCache()
	mst, _ := replaymanager.NewMetaStore(input.RequestId)
	if input.DestinationTopic == "" {
		input.DestinationTopic = commConst.DataReplayTopicPrefix
	}
	err, _, _ := replaymanager.PreProcessMetaData(input, "TEST_JOB", mst)

	if err != nil {
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to pre process metadata: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}

	processor.InitProducer(input.RequestId, input.DestinationTopic, input.ReplayType)
	go closeResources(ctx, mst, input)
	start := time.Now()
	Process(input, mst)
	elapsed := time.Since(start)
	logger.GetLogger().Info("Execution Time Taken ", zap.Duration("time", elapsed))
	return common.NewJobResultSuccess()
}

func Process(inputReq model.Message, mst *replaymanager.MetaDataStore) {
	throughPutLimit := utils.GetEnvInt(constants.THROUGHPUT_ENV_VARIABLE, constants.THROUGHPUT_DEFAULT_RATE)
	throughPutController := util.NewThroughputController(throughPutLimit)
	var wg sync.WaitGroup
	mst.UpdateMetaData(constants.Global, constants.StatusInProgress, 0, 0, 0, 0, "")
	processList := mst.GetProcessList()
	totalFiles := len(processList)
	err := ecryption.DecryptKeys(&inputReq)
	if err != nil {
		for i := range totalFiles {
			mst.UpdateMetaData(processList[i], "", 0, 0, 0, 0, err.Error())
		}
		return
	}
	concurrency := inputReq.Concurrency
	if concurrency <= 0 {
		concurrency = constants.Concurrency
	}
	logger.GetLogger().Info("replay file concurrency configuration",
		zap.Int("requestedConcurrency", inputReq.Concurrency),
		zap.Int("effectiveConcurrency", concurrency))
	parallelCtrChan := make(chan struct{}, concurrency)
	wg.Add(totalFiles)
	logger.GetLogger().Info("wait group count is", zap.Int("totalFiles", totalFiles))
	for i := 0; i < totalFiles; i++ {
		parallelCtrChan <- struct{}{}

		go func(i int) {

			logger.GetLogger().Info(fmt.Sprintf("spawning thread {%d}", i))

			defer func(wg *sync.WaitGroup) {
				mst.TimeStampMetaData(processList[i], false, true)
				logger.GetLogger().Info(fmt.Sprintf("executing the wg.Done()"), zap.String("traceId", inputReq.RequestId), zap.Int("thread ", i))
				wg.Done()
				<-parallelCtrChan
			}(&wg)
			fileName := processList[i]
			metaValue, ok := mst.GetMetaData(fileName)
			if !ok {
				mst.UpdateMetaData(fileName, constants.StatusFailed, 0, 0, 0, 0, "metadata not found")
				return
			}
			logger.GetLogger().Info("spawning thread :", zap.String("traceId", inputReq.RequestId), zap.Int("thread", i), zap.String("FileName : ", fileName))
			err, status := dbaws.FileDownloader(inputReq, i, mst, fileName, metaValue)
			if err != nil {
				mst.UpdateMetaData(processList[i], status, 0, 0, 0, 0, err.Error())
				return
			}
			err, status = processor.ReadAndProduce(fileName, metaValue.Offset, mst, inputReq.RequestId, i, inputReq.DestinationTopic, inputReq, throughPutController)
			if err != nil {
				mst.UpdateMetaData(processList[i], status, 0, 0, 0, 0, err.Error())
				return
			}

		}(i)
	}
	logger.GetLogger().Info("waiting for threads to complete ")
	wg.Wait()
	throughPutController.Stop()
	logger.GetLogger().Info("input message : ", zap.Reflect("Input data : ", inputReq))
	logger.GetLogger().Info("metadata.json message : ", zap.Reflect(" JSON : ", mst.GetValuesOfMap()))
	logger.GetLogger().Info("Headers ", zap.Reflect("Headers ", processor.GetHeader(inputReq)))
	if !replayInterrupted.Load() {
		processor.ProduceStatus(mst, inputReq)
		logger.GetLogger().Info("threads jobs are completed ")
		mst.UpdateGlobalStatus()
	} else {
		logger.GetLogger().Info("replay processing interrupted by shutdown hook, skipping normal status publish")
	}

}

func closeResources(ctx context.Context, mst *replaymanager.MetaDataStore, input model.Message) {

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, os.Kill)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig

	replayShutdownOnce.Do(func() {
		handleReplayShutdown(ctx, mst, input)
	})
	cluster, err := kafka.GetKafkaCluster(constants.ClusterName)
	if err == nil {
		cluster.CloseConsumer(ctx, constants.ClusterName)
		logger.GetLogger().Info("closed kafka consumers")
	}
	processor.GetProducer(input.RequestId, input.DestinationTopic).Close(ctx)
	processor.AckProducer.Close(ctx)
	processor.GetProducer("reqId", constants.DataReplayStatusProducer).Close(ctx)
	logger.GetLogger().Info("closed producers")

}

func handleReplayShutdown(ctx context.Context, mst *replaymanager.MetaDataStore, input model.Message) int {
	replayInterrupted.Store(true)
	logger.GetLogger().Info("replay shutdown hook triggered, marking unfinished files as failed")
	interruptedFiles := mst.MarkUnfinishedExecutionsAsFailed(constants.ProcessingInterruptedErrorMsg)
	logger.GetLogger().Info("Flushed MetaData")
	mst.Flush()
	sendReplayShutdownInterruptedAlert(ctx, input, interruptedFiles)
	time.Sleep(1 * time.Second)
	publishReplayStatus(mst, input)
	return interruptedFiles
}

func sendReplayShutdownInterruptedAlert(ctx context.Context, input model.Message, interruptedFiles int) {
	if interruptedFiles == 0 {
		return
	}

	replayAlert, err := buildReplayShutdownAlert(input, interruptedFiles)
	if err != nil {
		logger.GetLogger().Error("failed to create replay shutdown alert", zap.Error(err), zap.String("partId", input.RequestId))
		return
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("failed to create alerts manager for replay shutdown alert", zap.Error(err))
		return
	}
	defer alertsManager.Close(ctx)

	if err = alertsManager.SendAlerts([]*alerts_async.Alert{replayAlert}); err != nil {
		logger.GetLogger().Error("failed to send replay shutdown alert", zap.Error(err), zap.String("partId", input.RequestId))
		return
	}

	logger.GetLogger().Info("replay shutdown interrupted alert sent successfully",
		zap.String("partId", input.RequestId),
		zap.Int("interruptedFiles", interruptedFiles),
		zap.String("alertId", replayAlert.Id))
}

func buildReplayShutdownAlert(input model.Message, interruptedFiles int) (*alerts_async.Alert, error) {
	tenantID := input.TenantId
	if tenantID == "" {
		tenantID = common.DatabahnTenantId
	}

	message := fmt.Sprintf(
		"Replay worker interrupted during processing for requestId=%s with %d unfinished file(s). Files were marked FAILED with message: %q.",
		input.RequestId,
		interruptedFiles,
		constants.ProcessingInterruptedErrorMsg,
	)

	return alerts_async.NewAlert(
		alerts_async.Job,
		alerts_async.WithTitle("Data Replay Processing Interrupted"),
		alerts_async.WithMessage(message),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.JobFailure),
		alerts_async.WithEntityDetails(input.RequestId, common.DATA_REPLAY, common.DatabahnDataPlaneId, tenantID),
		alerts_async.WithErrorCode(alerts_async.DJFE10001, constants.ProcessingInterruptedErrorMsg),
		alerts_async.WithAlertType(alerts_async.Internal),
		alerts_async.WithAction("Investigate why the replay worker was interrupted (pod eviction, OOM, timeout) and retry the affected replay job."),
	)
}
