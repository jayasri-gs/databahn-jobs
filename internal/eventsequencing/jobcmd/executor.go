package jobcmd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/constants"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/lookup"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/processor"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/source/dbaws"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/objstore"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func ExecuteS3DataSequencing(input model.Message) common.JobResult {
	var jobErrors []common.JobError
	ctx := context.Background()
	updateInputMsg(&input)
	lookup.InitCache()
	mst, _ := replaymanager.NewMetaStore(input.RequestId)
	sst, _ := replaymanager.NewSortStore()
	err, _, _ := replaymanager.PreProcessMetaData(input, "TEST_JOB", mst)
	if err != nil {
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to pre process metadata: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}

	processor.InitProducer(input.RequestId)

	start := time.Now()
	Process(input, mst, sst)
	elapsed := time.Since(start)
	logger.GetLogger().Info("Execution Time Taken ", zap.Duration("time", elapsed))
	closeResources(ctx, mst, input.RequestId, input.DestinationTopic)
	return common.NewJobResultSuccess()
}

func Process(inputReq model.Message, mst *replaymanager.MetaDataStore, sst *replaymanager.SortStore) {

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
			status, err := dbaws.ObjectStoreFileDownloader(inputReq, i, mst, fileName, model.MetaDataValue(metaValue))
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}
			err, status = processor.ReadAndProduce(fileName, metaValue.Offset, mst, inputReq.RequestId, i, inputReq.DestinationTopic, inputReq, sst)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], status, 0, 0, 0, 0, err.Error())
				return
			}
			err = dbaws.DeleteFileFromObjectStore(inputReq, mst, fileName)
			if err != nil {
				mst.UpdateMetaData(mst.GetProcessList()[i], constants.StatusDeleteFailed, 0, 0, 0, 0, err.Error())
				return
			}

		}(i)
	}

	logger.GetLogger().Info("waiting for threads to complete ")
	wg.Wait()

	logger.GetLogger().Info("input message : ", zap.Reflect("Input data : ", inputReq))
	logger.GetLogger().Info("metadata.json message : ", zap.Reflect(" JSON : ", mst.GetMetaMap()))
	logger.GetLogger().Info("threads jobs are completed ")
	mst.UpdateGlobalStatus()

}

func closeResources(ctx context.Context, mst *replaymanager.MetaDataStore, reqId string, topic string) {

	logger.GetLogger().Info("Flushed MetaData")
	mst.Flush()
	time.Sleep(1 * time.Second)
	processor.GetProducer("").Close(ctx)
	logger.GetLogger().Info("closed producers")

}

func updateInputMsg(input *model.Message) {
	input.BucketName = objstore.GetBucket(objstore.BucketEvents)
	input.BucketPrefix = "sequence"
	input.Region = config.GetAppConfiguration().GetString(configuration.ObjectS3Region)
}
