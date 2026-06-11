package ack

import "time"

const (
	StatusSuccess = "SUCCESS"

	StatusFailure = "FAILURE"

	StatusPending = "PENDING"

	StatusInProgress = "IN_PROGRESS"

	StatusSkipped = "SKIPPED"

	StatusPartialFailure = "PARTIAL_FAILURE"
)

var ValidAckStatus = []string{StatusSuccess, StatusFailure, StatusPending, StatusInProgress, StatusSkipped, StatusPartialFailure}

type Ack struct {
	Id            string        `json:"id"`
	RequestId     string        `json:"request_id"`
	CustomerId    string        `json:"customer_id"`
	Type          string        `json:"type"` // change_flag, replay_file_status
	ReplayStatus  *ReplayStatus `json:"replay_status,omitempty"`
	EntityId      string        `json:"entity_id"`
	EntityType    string        `json:"entity_type"`
	EntityVersion string        `json:"entity_version"`
	ServiceName   string        `json:"service_name"`
	TenantId      string        `json:"tenant_id"`
	Action        string        `json:"action"` //create, update, delete, replay
	Status        string        `json:"status"`
	Error         string        `json:"error,omitempty"`
	Progress      string        `json:"progress,omitempty"`
	IsPlayground  bool          `json:"is_playground,omitempty"`
}

type ReplayStatus struct {
	Status []Status `json:"status"`
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
