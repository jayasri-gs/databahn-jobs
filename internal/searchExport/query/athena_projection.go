package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"go.uber.org/zap"
)

// Athena's UNLOAD writer is the Hive page sink, whose timestamp precision is fixed at
// MILLISECONDS and is not settable per query or per workgroup. A result column typed
// timestamp(6) — which is what Iceberg-backed tables and AWS Security Lake's own OCSF
// catalog expose — fails the whole statement with:
//
//	NOT_SUPPORTED: Incorrect timestamp precision for timestamp(6);
//	the configured precision is MILLISECONDS; column name: <col>
//
// Reading such a column is fine, so interactive search is unaffected and only export
// breaks. The fix is to stop handing `SELECT *` to UNLOAD and instead project the columns
// explicitly, narrowing every timestamp to timestamp(3).
//
// athenaCatalog is the catalog GetTableMetadata reads the schema from. Athena queries the
// Glue Data Catalog under this fixed name.
const athenaCatalog = "AwsDataCatalog"

// selectStarPattern matches the generated export shape — `SELECT * FROM <table>` with an
// optional database qualifier and optional double quotes — capturing the table reference.
//
// The table must be followed by end of statement or a clause keyword. That trailing guard is
// what makes the rewrite safe: it rejects joins, comma joins and table aliases, where the
// result columns are not the single table's columns and projecting them would silently drop
// the other relation's columns from the export. Anything else — a self-contained user query,
// an explicit select list, a CTE — fails the anchor and is left alone.
var selectStarPattern = regexp.MustCompile(
	`(?is)^\s*SELECT\s+\*\s+FROM\s+("?[A-Za-z0-9_]+"?(?:\."?[A-Za-z0-9_]+"?)?)` +
		`(\s+(?:WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION)\b|\s*;?\s*$)`)

// timestampSafeProjection is the SELECT list for cols with every timestamp narrowed to
// timestamp(3), and reports whether any narrowing was needed. When nothing needs casting
// the caller should leave the query untouched rather than rewrite it to an equivalent.
//
// Only top-level columns are narrowed. A timestamp(6) nested inside a struct or array —
// possible in OCSF — cannot be cast without rebuilding the whole value, so such a table
// still fails in Athena with the original error.
func timestampSafeProjection(cols []athenatypes.Column) (string, bool) {
	if len(cols) == 0 {
		return "", false
	}
	items := make([]string, 0, len(cols))
	needsCast := false
	for _, col := range cols {
		name := strings.TrimSpace(aws.ToString(col.Name))
		if name == "" || strings.ContainsAny(name, `"`) {
			// A name that cannot be quoted safely makes the whole projection unsafe.
			return "", false
		}
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

// isNarrowableTimestamp reports whether a catalog type is a plain timestamp that UNLOAD may
// reject. `timestamp` itself is included: catalogs report the Hive type without precision
// even when Athena resolves it to timestamp(6), so the precision cannot be trusted as the
// signal. Casting an already-millisecond column to timestamp(3) is a no-op, which makes
// casting every plain timestamp the safe choice.
//
// Time-zoned timestamps are excluded — narrowing one to timestamp(3) would silently drop
// its zone, which is a data change rather than a precision fix.
func isNarrowableTimestamp(catalogType string) bool {
	t := strings.ToLower(strings.TrimSpace(catalogType))
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

// rewriteSelectStar substitutes projection for the `*` of a `SELECT * FROM <table>` query,
// preserving everything after the table reference (WHERE, ORDER BY, LIMIT). It reports
// false when the query is not that shape.
func rewriteSelectStar(query, projection string) (string, bool) {
	match := selectStarPattern.FindStringSubmatchIndex(query)
	if match == nil {
		return "", false
	}
	// Group 1 is the table reference; rebuild the head with the projection in place of `*`.
	tableStart, tableEnd := match[2], match[3]
	return "SELECT " + projection + " FROM " + query[tableStart:tableEnd] + query[tableEnd:], true
}

// selectStarTable returns the table reference of a `SELECT * FROM <table>` query, stripped
// of quotes and of any database qualifier, since GetTableMetadata takes them separately.
func selectStarTable(query string) (string, bool) {
	match := selectStarPattern.FindStringSubmatch(query)
	if match == nil {
		return "", false
	}
	ref := strings.ReplaceAll(match[1], `"`, "")
	if idx := strings.LastIndex(ref, "."); idx >= 0 {
		ref = ref[idx+1:]
	}
	if ref == "" {
		return "", false
	}
	return ref, true
}

// narrowTimestampsForUnload rewrites a `SELECT *` export query so its timestamp columns
// cannot trip UNLOAD's millisecond writer. It is best effort by design: every failure to
// determine a safe projection returns the query unchanged, so a schema lookup problem
// degrades to today's behaviour instead of blocking an export that would have worked.
func (e *AthenaExecutor) narrowTimestampsForUnload(ctx context.Context, query, database string) string {
	if e.client == nil || strings.TrimSpace(database) == "" {
		return query
	}
	table, ok := selectStarTable(query)
	if !ok {
		return query
	}
	meta, err := e.client.GetTableMetadata(ctx, &athena.GetTableMetadataInput{
		CatalogName:  aws.String(athenaCatalog),
		DatabaseName: aws.String(database),
		TableName:    aws.String(table),
	})
	if err != nil || meta.TableMetadata == nil {
		e.log.Warn("Could not read table metadata; exporting without timestamp narrowing",
			zap.String("database", database), zap.String("table", table), zap.Error(err))
		return query
	}
	// `SELECT *` yields data columns followed by partition keys, so the projection must
	// list them in that order to preserve the exported column order.
	cols := append(append([]athenatypes.Column{}, meta.TableMetadata.Columns...),
		meta.TableMetadata.PartitionKeys...)
	projection, needsCast := timestampSafeProjection(cols)
	if !needsCast {
		return query
	}
	rewritten, ok := rewriteSelectStar(query, projection)
	if !ok {
		return query
	}
	e.log.Info("Narrowed timestamp columns to timestamp(3) for UNLOAD",
		zap.String("database", database), zap.String("table", table))
	return rewritten
}
