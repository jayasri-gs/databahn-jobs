package models

type Violation struct {
	Functionality       string `yaml:"functionality"`
	FunctionalityType   string `yaml:"functionalityType"`
	Severity            string `yaml:"severity"`
	PercentageReduction string `yaml:"percentageReduction"`
	Message             string `yaml:"message"`
}
