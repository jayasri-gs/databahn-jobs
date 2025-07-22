package notification

type OpsGenieNotificationRequest struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}
