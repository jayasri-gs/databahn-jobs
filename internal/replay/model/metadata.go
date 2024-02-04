package model

import "time"

type MetaDataValue struct {
	Key         string    `json:"key"`
	FileName    string    `json:"fileName"`
	Prefix      string    `json:"prefix"`
	Offset      int       `json:"offset"`
	Retry       int       `json:"retry"`
	JobName     string    `json:"jobName"`
	RequestId   string    `json:"requestId"`
	Status      string    `json:"status"`
	Time        time.Time `json:"time"`
	EndTime     time.Time `json:"endTime"`
	FileSize    int64     `json:"fileSize"`
	CurrentSize int64     `json:"currentSize"`
	ErrorMsg    []string  `json:"errorMsg"`
}

type Status struct {
	FileName    string    `json:"fileName"`
	RequestId   string    `json:"requestId"`
	Status      string    `json:"status"`
	FileSize    int64     `json:"fileSize"`
	CurrentSize int64     `json:"currentSize"`
	FilePath    string    `json:"filePath"`
	ErrorMsg    []string  `json:"errorMsg"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	Percentage  float64   `json:"percentage"`
	Lines       int       `json:"lines"`
}
