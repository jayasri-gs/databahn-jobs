package utils

import (
	"github.com/databahn-ai/databahn-jobs/internal/datahealthscore/models"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
)

// Read configuration file

type Config struct {
	Violations []models.Violation `yaml:"violations"`
}

func ReadViolationsFromConfig(filePath string) map[string]models.Violation {
	config := Config{}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("failed to read config file: %v", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("failed to parse config file: %v", err)
	}
	violationMap := make(map[string]models.Violation)
	for _, v := range config.Violations {
		violationMap[v.FunctionalityType] = v
	}
	return violationMap
}
