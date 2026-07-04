package svc

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/db-models/destination"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type ShutdownService struct {
	stopServiceChan chan os.Signal
	handler         func(context.Context)
	alertManager    *alerts_async.AlertsManager
	dataPlaneId     string
	serviceName     string
	entityType      alerts_async.Functionality
}

func NewShutdownService(entityType alerts_async.Functionality, serviceName string, alertManager *alerts_async.AlertsManager, dataPlaneId string, handler func(context.Context)) *ShutdownService {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, os.Kill)
	signal.Notify(sig, syscall.SIGTERM)

	return &ShutdownService{
		stopServiceChan: sig,
		entityType:      entityType,
		serviceName:     serviceName,
		alertManager:    alertManager,
		dataPlaneId:     dataPlaneId,
		handler:         handler,
	}
}

func (r *ShutdownService) StopDueToError(err error) {
	logger.GetLogger().Error("received stop signal, stopping service", zap.Error(err))
	r.recordAlertForServiceShutdownDueToError(err)
	r.stopServiceChan <- os.Kill
}

func (r *ShutdownService) Wait(ctx context.Context) {
	<-r.stopServiceChan
	logger.GetLogger().Info("shutting down service")
	r.handler(ctx)
	logger.GetLogger().Info("service stopped. bye!")
}

func (r *ShutdownService) recordAlertForServiceShutdownDueToError(err error) {
	alert, err := alerts_async.NewAlert(r.entityType,
		alerts_async.WithFunctionalityType(alerts_async.ServiceUnavailable),
		alerts_async.WithEntityDetails(destination.DataBahnCloudEntityID, r.serviceName, r.dataPlaneId, destination.DataBahnGlobalTenantID),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithTitle(r.serviceName+" shutting down due to error"),
		alerts_async.WithMessage(r.serviceName+" error:"+err.Error()),
		alerts_async.WithAction("Please check with Databahn support."),
		alerts_async.WithAlertType(alerts_async.Internal),
		alerts_async.WithErrorCode(alerts_async.DIOE30001, "Please check error for more details."))
	if err != nil {
		logger.GetLogger().Error("failed to create shutdown alert", zap.Error(err))
		return
	}
	r.alertManager.RecordAlert(alert)
}
