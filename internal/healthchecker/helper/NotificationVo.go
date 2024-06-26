package helper

import (
	"github.com/google/uuid"
	"time"
)

type Notification struct {
	ID                      uuid.UUID
	UserId                  string // USER ID
	TenantId                string // TENANT ID
	Subject                 string // EMAIL SUBJECT
	Message                 string // MESSAGE
	NotificationType        string // EMAIL, SMS, PUSH<Only Email Supported for now>
	Suggestion              string // SUGGESTION FOR ACTION
	AlertInfo               string // ALERT INFO
	Severity                string // INFO , WARNING, CRITICAL
	Service                 string // SERVICE NAME<Pre-defined>
	Granularity             string // GRANULARITY user or System level
	Version                 string // VERSION
	Title                   string
	Functionality           string
	FunctionalityEntityId   string
	FunctionalityEntityName string
	FunctionalityType       string
	FirstObservedAt         time.Time
	LastObservedAt          time.Time
}
