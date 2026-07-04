package pramaan

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type KafkaMetaData struct {
	TenantId               string
	SourceId               string
	DestinationId          string
	RuleId                 string
	TransformationId       string
	PipelineDone           string
	PipelineNext           string
	PipelineId             string
	EventId                string
	EdgeTs                 string
	SourceName             string
	DeviceVendor           string
	DeviceType             string
	LogType                string
	DestinationType        string
	GlblDstnEntityId       string
	SuppressionKey         string
	RouteProcessorId       string
	SecondaryDestinationId string
	ConnectorId            string
	EdgeId                 string
	SourceType             string
	SourceVersion          string
	SourceStatus           int
	SourceSecretId         string
	Scope                  string
	PullMechanism          string
}

func NewRandomKafkaMetaData() *KafkaMetaData {
	pipelineId := uuid.New().String()
	destinationId := uuid.New().String()
	return &KafkaMetaData{
		TenantId:               uuid.New().String(),
		SourceId:               uuid.New().String(),
		DestinationId:          destinationId,
		RuleId:                 uuid.New().String(),
		TransformationId:       uuid.New().String(),
		PipelineDone:           "CC",
		PipelineNext:           fmt.Sprintf("%s;%s:%s,%s_%s:%s,%s", "V1", pipelineId, "CC", "DEST", "sandbox", destinationId, "RAW"),
		PipelineId:             pipelineId,
		EventId:                uuid.New().String(),
		EdgeTs:                 strconv.FormatInt(time.Now().UnixMilli(), 10),
		SourceName:             "any source nam",
		DeviceVendor:           "unix",
		DeviceType:             "unix",
		LogType:                "syslog",
		DestinationType:        "S3",
		GlblDstnEntityId:       uuid.New().String(),
		RouteProcessorId:       uuid.New().String(),
		SecondaryDestinationId: uuid.New().String(),
		ConnectorId:            uuid.New().String(),
		EdgeId:                 uuid.New().String(),
		SourceType:             "syslog",
		SourceVersion:          "1",
		SourceStatus:           1,
		SourceSecretId:         uuid.New().String(),
		Scope:                  "FLEET",
		PullMechanism:          "",
	}
}
