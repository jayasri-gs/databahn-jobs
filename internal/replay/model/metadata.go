package model

import "time"

type MetaDataValue struct {
	Key         string    `json:"key"`
	FileName    string    `json:"fileName"`
	Offset      int       `json:"offset"`
	Retry       int       `json:"retry"`
	JobName     string    `json:"jobName"`
	RequestId   string    `json:"requestId"`
	Status      string    `json:"status"`
	Time        time.Time `json:"time"`
	FileSize    int64     `json:"fileSize"`
	CurrentSize int64     `json:"currentSize"`
	ErrorMsg    []string  `json:"errorMsg"`
}
