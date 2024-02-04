package jobcmd

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/lookup"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/processor"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/replay/source/dbaws"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func ExecuteReplayJob(input model.Message) {

	ctx := context.Background()
	//	input := ReadInputData()
	lookup.InitCache()
	mst, _ := replaymanager.NewMetaStore(input.RequestId)
	input.Destination = "db.raw.cloud"
	_, exit, code := replaymanager.PreProcessMetaData(input, "TEST_JOB", mst)
	if exit {
		logger.GetLogger().Info("shutdown started  with error code", zap.Int("code", code))
		_ = logger.GetLogger().Sync()
		os.Exit(code)
	}

	processor.InitProducer(input.RequestId, input.Destination)
	go closeResources(ctx, mst, input.RequestId, input.Destination)
	start := time.Now()
	Process(input, mst)
	elapsed := time.Since(start)
	logger.GetLogger().Info("Execution Time Taken  %s", zap.Duration("time", elapsed))
	mst.UpdateGlobalStatus()
}

func Process(inputReq model.Message, mst *replaymanager.MetaDataStore) {

	var wg sync.WaitGroup
	mst.UpdateMetaData(constants.Global, constants.StatusInProgress, 0, 0, 0, 0, "")
	totalFiles := len(mst.GetProcessList())
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
			err, status := dbaws.S3FileDownloader(inputReq, i, mst, fileName, metaValue)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}
			err, status = processor.ReadAndProduce(fileName, metaValue.Offset, mst, inputReq.RequestId, i, inputReq.Destination)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}

		}(i)
	}
	logger.GetLogger().Info("waiting for threads to complete ")
	wg.Wait()
	processor.ProduceStatus(mst)
	logger.GetLogger().Info("threads jobs are completed ")

}

func closeResources(ctx context.Context, mst *replaymanager.MetaDataStore, reqId string, topic string) {

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, os.Kill)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig

	logger.GetLogger().Info("Flushed MetaData")
	mst.Flush()
	time.Sleep(1 * time.Second)
	processor.ProduceStatus(mst)
	cluster, err := kafka.GetKafkaCluster(constants.ClusterName)
	if err == nil {
		cluster.CloseConsumer(ctx, constants.ClusterName)
		logger.GetLogger().Info("closed kafka consumers")
	}
	processor.GetProducer(reqId, topic).Close(ctx)
	processor.GetProducer("reqId", constants.DataReplayProducer).Close(ctx)
	logger.GetLogger().Info("closed producers")

}
