package model

import (
	"errors"
	"fmt"
	"strings"

	athenastore "github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/google/uuid"
)

// TableNotFoundError indicates the Athena table or its configuration is missing.
type TableNotFoundError struct{ Msg string }

func (e *TableNotFoundError) Error() string { return e.Msg }

// NewTableNotFoundError creates a non-fatal table-not-found error.
func NewTableNotFoundError(format string, args ...interface{}) error {
	return &TableNotFoundError{Msg: fmt.Sprintf(format, args...)}
}

// IsTableNotFound reports whether err signals a missing Athena table or configuration.
func IsTableNotFound(err error) bool {
	if err == nil {
		return false
	}
	var tnf *TableNotFoundError
	if errors.As(err, &tnf) {
		return true
	}
	return athenastore.IsTableNotFound(err)
}

// DatabaseName returns the Athena database name for a tenant.
func DatabaseName(tenantID uuid.UUID) string {
	return "databahn_tenant_" + strings.ReplaceAll(tenantID.String(), "-", "_")
}

// BucketFromS3Location extracts the bucket name from an S3 URI like "s3://bucket/path/".
func BucketFromS3Location(s3Location string) string {
	trimmed := strings.TrimPrefix(s3Location, "s3://")
	if idx := strings.Index(trimmed, "/"); idx > 0 {
		return trimmed[:idx]
	}
	return trimmed
}

// GroupKey uniquely identifies a destination+source+tenant combination.
func GroupKey(destID, sourceID, tenantID uuid.UUID) string {
	return destID.String() + "|" + sourceID.String() + "|" + tenantID.String()
}

// AthenaType converts backend-service field types to Athena SQL types.
func AthenaType(fieldType string) string {
	switch fieldType {
	case "long":
		return "bigint"
	case "string":
		return "string"
	case "double":
		return "double"
	case "boolean":
		return "boolean"
	default:
		return "string"
	}
}
