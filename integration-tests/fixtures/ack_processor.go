package fixtures

import (
	"context"
	"fmt"
	"time"

	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	AckProcessStatusPending    = "PENDING"
	AckProcessStatusProcessed  = "PROCESSED"
	AckProcessStatusSuppressed = "SUPPRESSED"
)

// ackProcessorSeedTimestamp is old enough for the 60s read grace period but young
// enough to survive ProcessAck cleanup (deletes PROCESSED rows older than ~1h).
func ackProcessorSeedTimestamp(now time.Time) time.Time {
	return now.Add(-90 * time.Second)
}

// AckProcessorEntity holds seeded rows for one entity under test.
type AckProcessorEntity struct {
	EntityID   string
	SourceID   uuid.UUID
	RequestID  string
	AckID      uuid.UUID
	LegacyReq  string
	LegacyAck  uuid.UUID
}

// AckProcessorFixture holds rows created for ack processor integration tests.
type AckProcessorFixture struct {
	TenantID uuid.UUID
	ActorID  uuid.UUID
	Entities []AckProcessorEntity
}

// SeedAckProcessorPaginationFixture seeds three sources with pending acks for cursor pagination.
func SeedAckProcessorPaginationFixture(ctx context.Context, db *gorm.DB) (*AckProcessorFixture, error) {
	tenantID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()
	ackTimestamp := ackProcessorSeedTimestamp(now)

	tenant := map[string]any{
		"id":     tenantID,
		"name":   fmt.Sprintf("ack-integration-%s", tenantID.String()[:8]),
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert ack tenant: %w", err)
	}

	fixture := &AckProcessorFixture{
		TenantID: tenantID,
		ActorID:  actorID,
		Entities: make([]AckProcessorEntity, 0, 3),
	}

	for i := range 3 {
		sourceID := uuid.New()
		requestID := fmt.Sprintf("req-%d-%s", i, sourceID.String()[:8])
		ackID := uuid.New()

		source := map[string]any{
			"id":            sourceID,
			"name":          fmt.Sprintf("ack-integration-source-%d", i),
			"tenant_id":     tenantID,
			"status":        "ERRORED",
			"created_by":    actorID,
			"updated_by":    actorID,
			"customer_id":   tenantID,
			"created_at":    now,
			"updated_at":    now,
		}
		if err := db.WithContext(ctx).Table("log_source").Create(source).Error; err != nil {
			return nil, fmt.Errorf("insert log_source %s: %w", sourceID, err)
		}

		cfRequest := map[string]any{
			"request_id":  requestID,
			"action":      "create",
			"entity_id":   sourceID.String(),
			"entity_type": utilConst.EntitySource,
			"is_processed": false,
			"tenant_id":   tenantID.String(),
			"timestamp":   ackTimestamp.Add(time.Duration(i) * time.Second),
		}
		if err := db.WithContext(ctx).Table("change_flag_requests").Create(cfRequest).Error; err != nil {
			return nil, fmt.Errorf("insert change_flag_request %s: %w", requestID, err)
		}

		ack := map[string]any{
			"id":             ackID,
			"request_id":     requestID,
			"entity_id":      sourceID.String(),
			"entity_type":    utilConst.EntitySource,
			"status":         ackPkg.StatusSuccess,
			"action":         "create",
			"tenant_id":      tenantID.String(),
			"timestamp":      ackTimestamp.Add(time.Duration(i) * time.Second),
			"process_status": AckProcessStatusPending,
		}
		if err := db.WithContext(ctx).Table("change_flag_acks").Create(ack).Error; err != nil {
			return nil, fmt.Errorf("insert change_flag_ack %s: %w", ackID, err)
		}

		fixture.Entities = append(fixture.Entities, AckProcessorEntity{
			EntityID:  sourceID.String(),
			SourceID:  sourceID,
			RequestID: requestID,
			AckID:     ackID,
		})
	}

	return fixture, nil
}

// SeedAckProcessorSuppressionFixture seeds one entity with an older and newer request.
func SeedAckProcessorSuppressionFixture(ctx context.Context, db *gorm.DB) (*AckProcessorFixture, error) {
	tenantID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()
	ackTimestamp := now.Add(-2 * time.Hour)
	sourceID := uuid.New()
	entityID := sourceID.String()

	tenant := map[string]any{
		"id":     tenantID,
		"name":   fmt.Sprintf("ack-suppress-%s", tenantID.String()[:8]),
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert ack tenant: %w", err)
	}

	source := map[string]any{
		"id":          sourceID,
		"name":        "ack-suppression-source",
		"tenant_id":   tenantID,
		"status":      "ERRORED",
		"created_by":  actorID,
		"updated_by":  actorID,
		"customer_id": tenantID,
		"created_at":  now,
		"updated_at":  now,
	}
	if err := db.WithContext(ctx).Table("log_source").Create(source).Error; err != nil {
		return nil, fmt.Errorf("insert log_source: %w", err)
	}

	oldRequestID := fmt.Sprintf("req-old-%s", sourceID.String()[:8])
	newRequestID := fmt.Sprintf("req-new-%s", sourceID.String()[:8])
	oldAckID := uuid.New()
	newAckID := uuid.New()

	requests := []map[string]any{
		{
			"request_id":  oldRequestID,
			"action":      "create",
			"entity_id":   entityID,
			"entity_type": utilConst.EntitySource,
			"is_processed": false,
			"tenant_id":   tenantID.String(),
			"timestamp":   ackTimestamp,
		},
		{
			"request_id":  newRequestID,
			"action":      "create",
			"entity_id":   entityID,
			"entity_type": utilConst.EntitySource,
			"is_processed": false,
			"tenant_id":   tenantID.String(),
			"timestamp":   ackTimestamp.Add(1 * time.Hour),
		},
	}
	for _, request := range requests {
		if err := db.WithContext(ctx).Table("change_flag_requests").Create(request).Error; err != nil {
			return nil, fmt.Errorf("insert change_flag_request: %w", err)
		}
	}

	acks := []map[string]any{
		{
			"id":             oldAckID,
			"request_id":     oldRequestID,
			"entity_id":      entityID,
			"entity_type":    utilConst.EntitySource,
			"status":         ackPkg.StatusSuccess,
			"action":         "create",
			"tenant_id":      tenantID.String(),
			"timestamp":      ackTimestamp,
			"process_status": AckProcessStatusPending,
		},
		{
			"id":             newAckID,
			"request_id":     newRequestID,
			"entity_id":      entityID,
			"entity_type":    utilConst.EntitySource,
			"status":         ackPkg.StatusSuccess,
			"action":         "create",
			"tenant_id":      tenantID.String(),
			"timestamp":      ackTimestamp.Add(1 * time.Hour),
			"process_status": AckProcessStatusPending,
		},
	}
	for _, ack := range acks {
		if err := db.WithContext(ctx).Table("change_flag_acks").Create(ack).Error; err != nil {
			return nil, fmt.Errorf("insert change_flag_ack: %w", err)
		}
	}

	return &AckProcessorFixture{
		TenantID: tenantID,
		ActorID:  actorID,
		Entities: []AckProcessorEntity{{
			EntityID:  entityID,
			SourceID:  sourceID,
			RequestID: newRequestID,
			AckID:     newAckID,
			LegacyReq: oldRequestID,
			LegacyAck: oldAckID,
		}},
	}, nil
}

// SeedAckProcessorFailureFixture seeds one source with a failure acknowledgement.
func SeedAckProcessorFailureFixture(ctx context.Context, db *gorm.DB) (*AckProcessorFixture, error) {
	tenantID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()
	ackTimestamp := ackProcessorSeedTimestamp(now)
	sourceID := uuid.New()
	requestID := fmt.Sprintf("req-failure-%s", sourceID.String()[:8])
	ackID := uuid.New()

	tenant := map[string]any{
		"id":     tenantID,
		"name":   fmt.Sprintf("ack-failure-%s", tenantID.String()[:8]),
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert ack tenant: %w", err)
	}

	source := map[string]any{
		"id":          sourceID,
		"name":        "ack-failure-source",
		"tenant_id":   tenantID,
		"status":      "ACTIVE",
		"created_by":  actorID,
		"updated_by":  actorID,
		"customer_id": tenantID,
		"created_at":  now,
		"updated_at":  now,
	}
	if err := db.WithContext(ctx).Table("log_source").Create(source).Error; err != nil {
		return nil, fmt.Errorf("insert log_source: %w", err)
	}

	cfRequest := map[string]any{
		"request_id":   requestID,
		"action":       "create",
		"entity_id":    sourceID.String(),
		"entity_type":  utilConst.EntitySource,
		"is_processed": false,
		"tenant_id":    tenantID.String(),
		"timestamp":    ackTimestamp,
	}
	if err := db.WithContext(ctx).Table("change_flag_requests").Create(cfRequest).Error; err != nil {
		return nil, fmt.Errorf("insert change_flag_request: %w", err)
	}

	ack := map[string]any{
		"id":             ackID,
		"request_id":     requestID,
		"entity_id":      sourceID.String(),
		"entity_type":    utilConst.EntitySource,
		"status":         ackPkg.StatusFailure,
		"action":         "create",
		"tenant_id":      tenantID.String(),
		"timestamp":      ackTimestamp,
		"process_status": AckProcessStatusPending,
		"error":          "deployment failed",
	}
	if err := db.WithContext(ctx).Table("change_flag_acks").Create(ack).Error; err != nil {
		return nil, fmt.Errorf("insert change_flag_ack: %w", err)
	}

	return &AckProcessorFixture{
		TenantID: tenantID,
		ActorID:  actorID,
		Entities: []AckProcessorEntity{{
			EntityID:  sourceID.String(),
			SourceID:  sourceID,
			RequestID: requestID,
			AckID:     ackID,
		}},
	}, nil
}

// CleanupAckProcessorFixture removes rows created for a fixture.
func CleanupAckProcessorFixture(ctx context.Context, db *gorm.DB, fixture *AckProcessorFixture) error {
	if fixture == nil {
		return nil
	}

	entityIDs := make([]string, 0, len(fixture.Entities))
	ackIDs := make([]uuid.UUID, 0, len(fixture.Entities)*2)
	requestIDs := make([]string, 0, len(fixture.Entities)*2)
	sourceIDs := make([]uuid.UUID, 0, len(fixture.Entities))

	for _, entity := range fixture.Entities {
		entityIDs = append(entityIDs, entity.EntityID)
		ackIDs = append(ackIDs, entity.AckID)
		requestIDs = append(requestIDs, entity.RequestID)
		sourceIDs = append(sourceIDs, entity.SourceID)
		if entity.LegacyAck != uuid.Nil {
			ackIDs = append(ackIDs, entity.LegacyAck)
		}
		if entity.LegacyReq != "" {
			requestIDs = append(requestIDs, entity.LegacyReq)
		}
	}

	if len(ackIDs) > 0 {
		if err := db.WithContext(ctx).Table("change_flag_acks").Where("id IN ?", ackIDs).Delete(nil).Error; err != nil {
			return fmt.Errorf("delete change_flag_acks: %w", err)
		}
	}
	if len(requestIDs) > 0 {
		if err := db.WithContext(ctx).Table("change_flag_requests").Where("request_id IN ?", requestIDs).Delete(nil).Error; err != nil {
			return fmt.Errorf("delete change_flag_requests: %w", err)
		}
	}
	if len(sourceIDs) > 0 {
		if err := db.WithContext(ctx).Table("log_source").Where("id IN ?", sourceIDs).Delete(nil).Error; err != nil {
			return fmt.Errorf("delete log_source: %w", err)
		}
	}
	if err := db.WithContext(ctx).Table("tenants").Where("id = ?", fixture.TenantID).Delete(nil).Error; err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	return nil
}
