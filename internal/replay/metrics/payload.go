package metrics

import (
	"encoding/json"
	"time"
)

const (
	replayServiceScope = "replay-service"
	serviceName        = "data-replay"
)

type otelAttributeValue struct {
	Type        string  `json:"Type"`
	StringValue *string `json:"StringValue,omitempty"`
}

type otelAttribute struct {
	Key   string             `json:"Key"`
	Value otelAttributeValue `json:"Value"`
}

type otelDataPoint struct {
	Attributes []otelAttribute `json:"Attributes"`
	StartTime  time.Time       `json:"StartTime"`
	Time       time.Time       `json:"Time"`
	Value      int64           `json:"Value"`
}

type otelSumData struct {
	DataPoints  []otelDataPoint `json:"DataPoints"`
	Temporality string          `json:"Temporality"`
	IsMonotonic bool            `json:"IsMonotonic"`
}

type otelMetric struct {
	Name string      `json:"Name"`
	Data otelSumData `json:"Data"`
}

type otelScope struct {
	Name string `json:"Name"`
}

type otelScopeMetrics struct {
	Scope   otelScope    `json:"Scope"`
	Metrics []otelMetric `json:"Metrics"`
}

type otelResource struct {
	Attributes []otelAttribute `json:"Attributes"`
}

type resourceMetricsPayload struct {
	Resource     otelResource       `json:"Resource"`
	ScopeMetrics []otelScopeMetrics `json:"ScopeMetrics"`
}

func buildCounterPayload(metricName string, tags map[string]string, value int64) ([]byte, error) {
	now := time.Now().UTC()
	attrs := make([]otelAttribute, 0, len(tags))
	for key, val := range tags {
		v := val
		attrs = append(attrs, otelAttribute{
			Key: key,
			Value: otelAttributeValue{
				Type:        "STRING",
				StringValue: &v,
			},
		})
	}

	service := serviceName
	payload := resourceMetricsPayload{
		Resource: otelResource{
			Attributes: []otelAttribute{
				{
					Key: "service.name",
					Value: otelAttributeValue{
						Type:        "STRING",
						StringValue: &service,
					},
				},
			},
		},
		ScopeMetrics: []otelScopeMetrics{
			{
				Scope: otelScope{Name: replayServiceScope},
				Metrics: []otelMetric{
					{
						Name: metricName,
						Data: otelSumData{
							DataPoints: []otelDataPoint{
								{
									Attributes: attrs,
									StartTime:  now,
									Time:       now,
									Value:      value,
								},
							},
							Temporality: "Delta",
							IsMonotonic: true,
						},
					},
				},
			},
		},
	}
	return json.Marshal(payload)
}
