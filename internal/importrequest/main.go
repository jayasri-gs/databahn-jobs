package importrequest

import (
	"context"
	"fmt"
	"sync"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/importrequest/consts"
	"github.com/databahn-ai/databahn-jobs/internal/importrequest/models"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func ProcessImportRequests(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	parallelism := utils.GetEnvInt("IMPORT_REQUEST_PARALLELISM", 2)

	log := logger.GetLogger()
	log.Info("starting import request processor", zap.Int("parallelism", parallelism))

	db := config.GetDB()
	requests, err := models.GetPendingImportRequests(db)
	if err != nil {
		jobErrors = append(jobErrors, common.JobError{Message: err.Error()})
		return common.NewJobResultFromErrors(jobErrors)
	}

	if len(requests) == 0 {
		log.Info("no import requests found during poll")
		return common.NewJobResultSuccess()
	}

	requestIDs := make([]string, len(requests))
	for i, req := range requests {
		requestIDs[i] = req.ID.String()
	}
	log.Info("polled import requests",
		zap.Int("count", len(requests)),
		zap.Strings("importRequestIds", requestIDs))

	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)

	for _, request := range requests {
		wg.Add(1)
		sem <- struct{}{}

		go func(req models.ImportRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			processImportRequest(ctx, db, req)
		}(request)
	}

	wg.Wait()
	log.Info("import request processor completed", zap.Int("processedCount", len(requests)))
	return common.NewJobResultSuccess()
}

func processImportRequest(ctx context.Context, db *gorm.DB, req models.ImportRequest) {
	log := logger.GetLogger().With(
		zap.String("importRequestId", req.ID.String()),
		zap.String("tenantId", req.TenantID.String()),
		zap.String("importType", req.ImportType),
	)

	log.Info("processing import request", importRequestLogFields(req)...)

	claimed, err := models.ClaimImportRequest(db, req.ID)
	if err != nil {
		log.Error("failed to claim import request", zap.Error(err))
		return
	}
	if !claimed {
		log.Info("import request already claimed by another pod — skipping")
		return
	}
	if req.Status == consts.PROCESSING {
		log.Info("resumed interrupted import request", zap.String("status", req.Status))
	} else {
		log.Info("import request claimed", zap.String("status", req.Status))
	}

	var stats *models.ImportStats
	var processErr error

	switch req.ImportType {
	case consts.ImportTypeDeviceTimezoneMapping:
		stats, processErr = processDeviceTimezoneMapping(ctx, req, log)
	default:
		processErr = fmt.Errorf("unsupported import type %q", req.ImportType)
	}

	if processErr != nil {
		handleImportFailure(db, log, req, processErr.Error(), stats)
		return
	}

	if err := models.UpdateImportRequestComplete(db, req.ID, *stats); err != nil {
		handleImportFailure(db, log, req, "failed to update completion status: "+err.Error(), stats)
		return
	}

	logImportRequestAfterProcessing(log, req, stats, consts.COMPLETED, "")
}

func handleImportFailure(db *gorm.DB, log *zap.Logger, req models.ImportRequest, errMsg string, stats *models.ImportStats) {
	log.Error("import request failed",
		zap.String("status", consts.FAILED),
		zap.String("error", errMsg))

	if err := models.UpdateImportRequestFailed(db, req.ID, errMsg, stats); err != nil {
		log.Error("failed to update import request status after failure", zap.Error(err))
		return
	}

	logImportRequestAfterProcessing(log, req, stats, consts.FAILED, errMsg)
}
