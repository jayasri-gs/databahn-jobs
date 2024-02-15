package log_source

import (
	"gorm.io/gorm"
)

type LogSourceMapping struct {
	Device                     string `json:"device" gorm:"type:varchar(64);uniqueIndex:unique_mapping"`
	Vendor                     string `json:"vendor" gorm:"type:varchar(64);uniqueIndex:unique_mapping"`
	LogType                    string `json:"logType" gorm:"type:varchar(64);uniqueIndex:unique_mapping"`
	Scope                      string `json:"scope" gorm:"type:varchar(64)"`
	ParsedTopic                string `json:"ParsedTopic" gorm:"type:varchar(64)"`
	RawTopic                   string `json:"rawTopic" gorm:"type:varchar(64)"`
	ConnectorType              string `json:"connectorType" gorm:"type:varchar(64)"`
	ParserName                 string `json:"parserName" gorm:"type:varchar(64)"`
	TransformationApplicable   bool   `json:"transformationApplicable"`
	NoParserPipelineApplicable bool   `json:"noParserPipelineApplicable"`
}

func GetAllLogSourceMappings(db *gorm.DB) (mappings []LogSourceMapping, err error) {
	err = db.Find(&mappings).Error
	return mappings, err
}

func GetParsedTopics(db *gorm.DB) (parsedTopics []string, err error) {
	err = db.Model(&LogSourceMapping{}).Distinct().Pluck("parsed_topic", &parsedTopics).Error
	return parsedTopics, err
}
