package statistics

type SumResponse struct {
	Sum float64 `json:"sum"`
}

type AggregateResponse struct {
	Agg map[string]any `json:"agg"`
}
type AlertSearchResponse struct {
	Alerts []AlertDocument `json:"alerts"`
}
type HistogramBucket struct {
	Value      float64 `json:"value"`
	Time       int64   `json:"time"`
	TimeString string  `json:"time_string"`
}
type HistogramResponse struct {
	Buckets []HistogramBucket `json:"buckets"`
}

func NewSumResponse(resp *SumQueryResponse) SumResponse {
	val := resp.Aggregations.SumValue
	return SumResponse{
		Sum: val.Value,
	}
}

func buildAggregationResponse(grpBy GroupBy) map[string]any {
	buc := grpBy.Buckets
	res := make(map[string]any)
	for _, b := range buc {
		k := b.Key
		if b.SumValue.Value != 0 {
			res[k] = b.SumValue.Value
		} else {
			res[k] = buildAggregationResponse(b.GroupBy)
		}
	}
	return res
}

func NewAggregateResponse(resp *AggregateQueryResponse) AggregateResponse {
	data := buildAggregationResponse(resp.Aggregations.GroupBy)
	return AggregateResponse{
		Agg: data,
	}
}

func NewHistogramResponse(res *HistogramQueryResponse) HistogramResponse {
	buc := res.Aggregations.SumOverTime.Buckets
	var buckets []HistogramBucket
	for _, b := range buc {
		k := b.Key
		kStr := b.KeyAsString
		v := b.SumValue.Value
		hb := HistogramBucket{
			Value:      v,
			Time:       k,
			TimeString: kStr,
		}
		buckets = append(buckets, hb)
	}

	return HistogramResponse{
		Buckets: buckets,
	}

}

func NewAlertSearchResponse(res *AlertSearchQueryResponse) AlertSearchResponse {
	var alerts []AlertDocument
	for _, v := range res.Hits.Hits {
		alerts = append(alerts, v.Source)
	}
	return AlertSearchResponse{
		Alerts: alerts,
	}
}
