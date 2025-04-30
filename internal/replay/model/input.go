package model

type Message struct {
	Source           string            `json:"source"`
	DestinationTopic string            `json:"destination"`
	NewSource        string            `json:"newSource"`
	RequestId        string            `json:"requestId"`
	BucketName       string            `json:"bucketName"`
	BucketPrefix     string            `json:"bucketPrefix"`
	AccessKeyID      string            `json:"accessKeyId"`
	SecretAccessKey  string            `json:"secretAccessKey"`
	Region           string            `json:"region"`
	FileName         []string          `json:"fileName"`
	TenantId         string            `json:"tenantId"`
	DeviceType       string            `json:"deviceType"`
	DeviceVendor     string            `json:"deviceVendor"`
	LogType          string            `json:"logType"`
	FleetId          string            `json:"fleetId"`
	ConnectId        string            `json:"connectId"`
	AckId            string            `json:"ackId"`
	SourceName       string            `json:"sourceName"`
	DataStore        string            `json:"dataStore"`
	AdditionalConfig map[string]string `json:"additionalConfig"`
}
