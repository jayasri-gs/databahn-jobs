package rule

type AggregationConfig struct {
	GroupByAttributes []string                     `json:"groupByAttributes"`
	Interval          AggregationInterval          `json:"interval"`
	SelectAttributes  []AggregationSelectAttribute `json:"selectAttributes"`
	OutputMode        OutputMode                   `json:"outputMode"`
}

type OutputMode struct {
	Stats       bool `json:"stats"`
	PassThrough bool `json:"passThrough"`
}

type AggregationInterval struct {
	Value int    `json:"value"`
	Unit  string `json:"unit"`
}

type AggregationSelectAttribute struct {
	Attribute string   `json:"attribute"`
	Function  string   `json:"function"`
	Label     string   `json:"label"`
	Arguments []string `json:"arguments,omitempty"`
	Implicit  bool     `json:"implicit"`
}
