package model

type Message struct {
	Source          string   `json:"source"`
	Destination     string   `json:"destination"`
	RequestId       string   `json:"requestId"`
	BucketName      string   `json:"bucketName"`
	BucketPrefix    string   `json:"bucketPrefix"`
	AccessKeyID     string   `json:"accessKeyId"`
	SecretAccessKey string   `json:"secretAccessKey"`
	Region          string   `json:"region"`
	FileName        []string `json:"fileName"`
}
