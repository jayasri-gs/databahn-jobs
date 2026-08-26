// Package secrets contains the credential-secret access layer used by the
// credential-expiry-notification job. Reads and writes only the columns needed
// to drive notification scheduling.
package secrets

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Secret mirrors the subset of the backend-service secrets table that the job needs.
type Secret struct {
	Id                   uuid.UUID      `gorm:"type:uuid;primary_key;column:id" json:"id"`
	Name                 string         `gorm:"column:name" json:"name"`
	TenantId             uuid.UUID      `gorm:"type:uuid;column:tenant_id" json:"tenant_id"`
	CustomerId           uuid.UUID      `gorm:"type:uuid;column:customer_id" json:"customer_id"`
	Scope                string         `gorm:"column:scope" json:"scope"`
	Vendor               string         `gorm:"column:vendor" json:"vendor"`
	Device               string         `gorm:"column:device" json:"device"`
	PullMechanism        string         `gorm:"column:pull_mechanism" json:"pull_mechanism"`
	DestinationType      string         `gorm:"column:destination_type" json:"destination_type"`
	ExpiryDate           *time.Time     `gorm:"column:expiry_date" json:"expiry_date"`
	NotificationTriggers datatypes.JSON `gorm:"type:jsonb;column:notification_triggers" json:"notification_triggers"`
}

func (Secret) TableName() string { return "secrets" }

// LookaheadDays defines the widest threshold + a small buffer. Rows whose expiry
// falls outside this window will not be picked up by the job.
const LookaheadDays = 16

// FetchDueCredentials returns credential secrets that:
//
//   - are scoped (scope IS NOT NULL) and have an expiry date,
//   - are linked to at least one log source or destination via configuration.secretId,
//   - have an expiry_date within LookaheadDays of today (uses idx_secrets_expiry_active),
//   - have a notification_triggers series present (seeded on create/update).
//
// Rows are locked with FOR UPDATE SKIP LOCKED so multiple job replicas can safely
// run concurrently and CRUD writers are not blocked by the job.
func FetchDueCredentials(ctx context.Context, db *gorm.DB) ([]Secret, error) {
	if db == nil {
		return nil, errors.New("nil db")
	}
	var results []Secret
	err := db.WithContext(ctx).
		Raw(`
			SELECT s.id, s.name, s.tenant_id, s.customer_id, s.scope,
			       s.vendor, s.device, s.pull_mechanism, s.destination_type,
			       s.expiry_date, s.notification_triggers
			FROM secrets s
			WHERE s.scope IS NOT NULL
			  AND s.expiry_date IS NOT NULL
			  AND s.notification_triggers IS NOT NULL
			  AND s.expiry_date <= ((now() AT TIME ZONE 'UTC')::date + ?)
			  AND (
			        EXISTS (
			          SELECT 1 FROM log_source ls
			          WHERE ls.tenant_id = s.tenant_id
			            AND (ls.customer_id = s.customer_id OR (ls.customer_id IS NULL AND s.customer_id IS NULL))
			            AND ls.configuration IS NOT NULL
			            AND json_extract_path_text(ls.configuration::json, 'secretId') = s.id::text
			        )
			     OR EXISTS (
			          SELECT 1 FROM destination d
			          WHERE d.tenant_id = s.tenant_id
			            AND (d.customer_id = s.customer_id OR (d.customer_id IS NULL AND s.customer_id IS NULL))
			            AND d.configuration IS NOT NULL
			            AND json_extract_path_text(d.configuration::json, 'secretId') = s.id::text
			        )
			  )
			FOR UPDATE OF s SKIP LOCKED
		`, LookaheadDays).
		Scan(&results).Error
	if err != nil {
		return nil, err
	}
	return results, nil
}

// UpdateNotificationTriggers overwrites the notification_triggers jsonb for a single secret.
// Callers should invoke this inside the same transaction that fetched the row with FOR UPDATE.
func UpdateNotificationTriggers(ctx context.Context, db *gorm.DB, secretId uuid.UUID, triggers datatypes.JSON) error {
	if db == nil {
		return errors.New("nil db")
	}
	return db.WithContext(ctx).
		Table("secrets").
		Where("id = ?", secretId).
		Update("notification_triggers", triggers).Error
}
