package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"go.uber.org/zap"
)

// Athena's UNLOAD writes through the Hive page sink, whose timestamp precision is fixed at
// MILLISECONDS and is not settable per query or per workgroup. A result column typed
// timestamp(6) — which is what Iceberg-backed tables and AWS Security Lake's own OCSF
// catalog expose — fails the whole statement with:
//
//	NOT_SUPPORTED: Incorrect timestamp precision for timestamp(6);
//	the configured precision is MILLISECONDS; column name: <col>
//
// Reading such a column is fine, so interactive search is unaffected and only export breaks.
// The fix is to project the result columns explicitly, narrowing every timestamp to
// timestamp(3), before handing the query to UNLOAD.

// exportSubqueryAlias names the wrapped original query. The projection selects from it by
// column name, so the alias only has to be a valid identifier that cannot collide with a
// user table in the same statement.
const exportSubqueryAlias = "databahn_export_src"

// timestampSafeProjection is the SELECT list for cols with every timestamp narrowed to
// timestamp(3), and reports whether any narrowing was needed. When nothing needs casting the
// caller leaves the query untouched rather than rewriting it into an equivalent.
//
// Only top-level columns are narrowed. A timestamp(6) nested inside a struct or array —
// possible in OCSF — cannot be cast without rebuilding the whole value, so such a query
// still fails in Athena with the original error.
func timestampSafeProjection(cols []athenatypes.ColumnInfo) (string, bool) {
	if len(cols) == 0 {
		return "", false
	}
	items := make([]string, 0, len(cols))
	seen := make(map[string]struct{}, len(cols))
	needsCast := false
	for _, col := range cols {
		name := strings.TrimSpace(aws.ToString(col.Name))
		if name == "" || strings.Contains(name, `"`) {
			// A name that cannot be quoted safely makes the whole projection unsafe.
			return "", false
		}
		if _, dup := seen[name]; dup {
			// Duplicate output names (a join selecting two `id`s) cannot be referenced
			// unambiguously by name, so the projection would change which column is exported.
			return "", false
		}
		seen[name] = struct{}{}
		quoted := `"` + name + `"`
		if isNarrowableTimestamp(aws.ToString(col.Type)) {
			items = append(items, fmt.Sprintf("CAST(%s AS timestamp(3)) AS %s", quoted, quoted))
			needsCast = true
			continue
		}
		items = append(items, quoted)
	}
	return strings.Join(items, ", "), needsCast
}

// isNarrowableTimestamp reports whether a result column type is a plain timestamp that UNLOAD
// may reject. `timestamp` without a precision is included: Athena reports the type name
// without precision even when the underlying column is microsecond, so the reported precision
// cannot be trusted as the signal. Casting an already-millisecond column to timestamp(3) is a
// no-op, which makes casting every plain timestamp the safe choice.
//
// Time-zoned timestamps are excluded — narrowing one to timestamp(3) would silently drop its
// zone, which is a data change rather than a precision fix.
func isNarrowableTimestamp(columnType string) bool {
	t := strings.ToLower(strings.TrimSpace(columnType))
	if !strings.HasPrefix(t, "timestamp") {
		return false
	}
	if strings.Contains(t, "zone") || strings.HasPrefix(t, "timestamptz") {
		return false
	}
	// Reject compound types that merely start with the word, e.g. `timestamp_array`.
	rest := strings.TrimPrefix(t, "timestamp")
	return rest == "" || strings.HasPrefix(rest, "(")
}

// wrapWithProjection nests the original query so the projection can narrow its output columns
// without having to parse or rewrite the query itself. Any shape Athena accepts as a subquery
// works, including joins and explicit select lists.
func wrapWithProjection(query, projection string) string {
	return fmt.Sprintf("SELECT %s FROM (%s) AS %s", projection, query, exportSubqueryAlias)
}

// narrowTimestampsForUnload rewrites an export query so its timestamp columns cannot trip
// UNLOAD's millisecond writer.
//
// Column names and their order come from the result metadata of the query itself — the same
// source the CSV header is built from (GetQueryColumns) — so the header and the data cannot
// disagree. Reading the table schema instead would risk exactly that: Athena's `SELECT *`
// order is the catalog's data columns then its partition keys, but an Iceberg table's
// partitioning is hidden and its partition keys are not result columns at all.
//
// It is best effort by design: every failure to determine a safe projection returns the query
// unchanged, so a metadata problem degrades to today's behaviour instead of blocking an
// export that would otherwise have worked.
func (e *AthenaExecutor) narrowTimestampsForUnload(ctx context.Context, query, database string) string {
	if e.client == nil {
		return query
	}
	cols, err := e.queryColumnMetadata(ctx, query, database)
	if err != nil {
		e.log.Warn("Could not read result column metadata; exporting without timestamp narrowing",
			zap.Error(err))
		return query
	}
	projection, needsCast := timestampSafeProjection(cols)
	if !needsCast {
		return query
	}
	e.log.Info("Narrowed timestamp columns to timestamp(3) for UNLOAD",
		zap.Int("columnCount", len(cols)))
	return wrapWithProjection(query, projection)
}
