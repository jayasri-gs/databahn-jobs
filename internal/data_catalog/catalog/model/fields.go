package model

import (
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// GroupFields returns common zap fields for catalog schema sync logging.
func GroupFields(destID, sourceID, tenantID uuid.UUID, dispenserType string) []zap.Field {
	return []zap.Field{
		zap.String("destination_id", destID.String()),
		zap.String("source_id", sourceID.String()),
		zap.String("tenant_id", tenantID.String()),
		zap.String("dispenser_type", dispenserType),
	}
}

// FieldNames returns the names from a slice of catalog fields.
func FieldNames(fields []Field) []string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

// FieldIDs returns the IDs from a slice of catalog fields.
func FieldIDs(fields []Field) []int64 {
	ids := make([]int64, len(fields))
	for i, f := range fields {
		ids[i] = f.ID
	}
	return ids
}
