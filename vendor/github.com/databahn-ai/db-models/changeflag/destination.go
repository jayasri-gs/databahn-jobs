package changeflag

import (
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type FlagDestination struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	HistoryVersion      int               `json:"history_version"`
	TenantUUID          string            `json:"tenant_uuid"`
	DestinationType     string            `json:"destination_type"`
	ForwardDataType     int               `json:"forward_data_type"`
	Scope               string            `json:"scope"`
	Config              map[string]string `json:"config"`
	SecretId            string            `json:"secret_id"`
	PipelineId          string            `json:"pipeline_id"`
	DestinationOverride bool              `json:"destination_override"`
	BackupDestinationId string            `json:"backup_destination_id"`
}

func (fd FlagDestination) GetSecretId() string {
	log := logger.GetLogger()
	hasSecret := fd.SecretId != ""
	log.Debug("changeflag FlagDestination.GetSecretId",
		zap.String("destinationId", fd.ID),
		zap.String("destinationName", fd.Name),
		zap.String("tenantUUID", fd.TenantUUID),
		zap.String("destinationType", fd.DestinationType),
		zap.String("pipelineId", fd.PipelineId),
		zap.Bool("hasSecretId", hasSecret),
		zap.Bool("willPOSTDataPlaneControllerSecrets", hasSecret))
	return fd.SecretId
}

func (fd FlagDestination) AddConfig(extraConfig map[string]string) {
	for k, v := range extraConfig {
		fd.Config[k] = v
	}
}
