package alerts_async


type Alert struct {
	Id                      string    `json:"id"`
	Criticality             string    `json:"criticality"`
	Title                   string    `validate:"required" json:"title"`
	Message                 string    `json:"message"`
	CreatedAt               int64 `json:"createdAt,omitempty"`
	UpdatedAt               int64 `json:"updatedAt,omitempty"`
	FirstObservedAt         int64 `json:"firstObservedAt,omitempty"`
	LastObservedAt          int64 `json:"lastObservedAt,omitempty"`
	TenantId                string    `json:"tenantId"`
	Functionality           string    `json:"functionality"`
	FunctionalityEntityId   string    `json:"functionalityEntityId"`
	FunctionalityEntityName string    `json:"functionalityEntityName"`
	FunctionalityType       string    `json:"functionalityType"`
	Status                  int       `json:"status"`
	UpdatedBy               string    `json:"updatedBy"`
	Dismissed               bool      `json:"dismissed"`
	AlertType               string    `json:"alertType"`
	ErrorMessage            string    `json:"errorMessage"`
	ErrorCode               string    `json:"errorCode"`
	DataPlaneId             string    `json:"dataPlaneId"`
}
