package metrics

import (
	commConst "github.com/databahn-ai/common-utils/constants"
	replayconst "github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
)

func ReplayMetricTags(req model.Message) map[string]string {
	tags := map[string]string{
		replayconst.TrafficTypeHeader: replayconst.TrafficTypeReplay,
		replayconst.ReplayTypeHeader:  replayconst.ReplayTypeFromJobType(req.ReplayType),
		commConst.TenantId:            req.TenantId,
		commConst.EventSourceId:       req.Source,
	}
	if req.RequestId != "" {
		tags[replayconst.ReplayJobIdHeader] = req.RequestId
	}
	return tags
}
