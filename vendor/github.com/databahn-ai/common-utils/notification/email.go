package notification

type EmailRecipients struct {
	To  []string `json:"to"`
	CC  []string `json:"cc"`
	BCC []string `json:"bcc"`
}

type EmailNotificationRequest struct {
	Recipients *EmailRecipients  `json:"recipients"`
	Targets    []*DatabahnTarget `json:"targets"`
	Body       string            `json:"body"`
	Subject    string            `json:"subject"`
}
