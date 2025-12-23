package audit

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// AuditEntry represents an entry in the db_audit table
type AuditEntry struct {
	ID               uuid.UUID `gorm:"type:uuid;primary_key;column:id" json:"id"`
	TenantUUID       uuid.UUID `gorm:"type:uuid;column:tenant_uuid" json:"tenant_uuid"`
	ObjectID         string    `gorm:"type:varchar(64);column:object_id" json:"object_id"`
	ObjectType       string    `gorm:"type:varchar(64);column:object_type" json:"object_type"`
	ObjectName       string    `gorm:"type:varchar(256);column:object_name" json:"object_name"`
	AggregationQuery string    `gorm:"type:varchar(1024);column:aggregation_query" json:"aggregation_query"`
	IsSuccess        *bool     `gorm:"column:is_success" json:"is_success"`
	Action           string    `gorm:"type:varchar(64);column:action" json:"action"`
	Timestamp        time.Time `gorm:"type:timestamp with time zone;column:timestamp" json:"timestamp"`
	SubAction        string    `gorm:"type:varchar(64);column:sub_action" json:"sub_action"`
	SubjectID        string    `gorm:"type:varchar(64);column:subject_id" json:"subject_id"`
	SubjectName      string    `gorm:"type:varchar(256);column:subject_name" json:"subject_name"`
	SubjectEmail     string    `gorm:"type:varchar(256);column:subject_email" json:"subject_email"`
	Message          string    `gorm:"type:varchar(512);column:message" json:"message"`
	Error            string    `gorm:"type:varchar(512);column:error" json:"error"`
	Origin           string    `gorm:"type:varchar(512);column:origin" json:"origin"`
	Path             string    `gorm:"type:varchar(512);column:path" json:"path"`
	Rev              int       `gorm:"column:rev" json:"rev"`
	AuditObjects     *string   `gorm:"type:json;column:audit_objects" json:"audit_objects"`
}

func (AuditEntry) TableName() string {
	return "db_audit"
}

// CreateAuditEntry creates a new audit entry in the database
func CreateAuditEntry(db *gorm.DB, entry *AuditEntry) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	return db.Create(entry).Error
}
