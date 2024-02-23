package ack

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
	Id            string       `json:"id"`
	RequestId     string       `json:"request_id"`
	CustomerId    string       `json:"customer_id"`
	Type          string       `json:"type"` // change_flag, replay_file_status
	ReplayStatus  ReplayStatus `json:"type_replay;omitempty"`
	EntityId      string       `json:"entity_id"`
	EntityType    string       `json:"entity_type"`
	EntityVersion string       `json:"entity_version"`
	TenantId      string       `json:"tenant_id"`
	Action        string       `json:"action"` //create, update, delete, replay
	Status        string       `json:"status"`
	Error         string       `json:"error"`
	Progress      string       `json:"progress"`
}

type ReplayStatus struct {
	FileName    string `json:"file_name"`
	FilePath    string `json:"file_path"`
	FileSize    int    `json:"file_size"`
	CurrentSize int    `json:"current_size"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Percentage  string `json:"percentage"`
	Lines       int    `json:"lines"`
}
