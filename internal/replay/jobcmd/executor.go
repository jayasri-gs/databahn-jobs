package jobcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"

	commConst "github.com/databahn-ai/common-utils/constants"
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
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func ExecuteReplayJob(input model.Message) common.JobResult {
	var jobErrors []common.JobError

	ctx := context.Background()
	//	input := ReadInputData()
	lookup.InitCache()
	mst, _ := replaymanager.NewMetaStore(input.RequestId)
	input.DestinationTopic = commConst.DataReplayTopicPrefix
	err, _, _ := replaymanager.PreProcessMetaData(input, "TEST_JOB", mst)

	if err != nil {
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to pre process metadata: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}

	processor.InitProducer(input.RequestId, input.DestinationTopic)
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
	totalFiles := len(mst.GetProcessList())
	err := ecryption.DecryptKeys(&inputReq)
	if err != nil {
		for i := range totalFiles {
			mst.UpdateMetaData(mst.GetProcessList()[i], "", 0, 0, 0, 0, err.Error())
		}
		return
	}
	parallelCtrChan := make(chan struct{}, constants.Concurrency)
	wg.Add(totalFiles)
	logger.GetLogger().Info("wait group count is", zap.Int("totalFiles", totalFiles))
	for i := 0; i < totalFiles; i++ {
		parallelCtrChan <- struct{}{}

		go func(i int) {

			logger.GetLogger().Info(fmt.Sprintf("spawning thread {%d}", i))

			defer func(wg *sync.WaitGroup) {
				mst.TimeStampMetaData(mst.GetProcessList()[i], false, true)
				logger.GetLogger().Info(fmt.Sprintf("executing the wg.Done()"), zap.String("traceId", inputReq.RequestId), zap.Int("thread ", i))
				wg.Done()
				<-parallelCtrChan
			}(&wg)
			fileName := mst.GetProcessList()[i]
			metaValue := mst.GetMetaMap()[fileName]
			logger.GetLogger().Info("spawning thread :", zap.String("traceId", inputReq.RequestId), zap.Int("thread", i), zap.String("FileName : ", fileName))
			err, status := dbaws.FileDownloader(inputReq, i, mst, fileName, metaValue)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}
			err, status = processor.ReadAndProduce(fileName, metaValue.Offset, mst, inputReq.RequestId, i, inputReq.DestinationTopic, inputReq, throughPutController)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}

		}(i)
	}
	logger.GetLogger().Info("waiting for threads to complete ")
	wg.Wait()
	throughPutController.Stop()
	logger.GetLogger().Info("input message : ", zap.Reflect("Input data : ", inputReq))
	logger.GetLogger().Info("metadata.json message : ", zap.Reflect(" JSON : ", mst.GetMetaMap()))
	logger.GetLogger().Info("Headers ", zap.Reflect("Headers ", processor.GetHeader(inputReq)))
	processor.ProduceStatus(mst, inputReq)
	logger.GetLogger().Info("threads jobs are completed ")
	mst.UpdateGlobalStatus()

}

func closeResources(ctx context.Context, mst *replaymanager.MetaDataStore, input model.Message) {

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, os.Kill)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig

	logger.GetLogger().Info("Flushed MetaData")
	mst.Flush()
	time.Sleep(1 * time.Second)
	processor.ProduceStatus(mst, input)
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
