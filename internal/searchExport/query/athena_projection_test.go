package query

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

func col(name, typ string) athenatypes.ColumnInfo {
	return athenatypes.ColumnInfo{Name: aws.String(name), Type: aws.String(typ)}
}

func TestIsNarrowableTimestamp(t *testing.T) {
	tests := map[string]bool{
		"timestamp":                true,
		"timestamp(6)":             true,
		"timestamp(3)":             true,
		" TIMESTAMP ":              true,
		"timestamp with time zone": false,
		"timestamptz":              false,
		"timestamp_array":          false,
		"string":                   false,
		"bigint":                   false,
		"array<timestamp>":         false,
		"struct<t:timestamp>":      false,
	}
	for typ, want := range tests {
		if got := isNarrowableTimestamp(typ); got != want {
			t.Fatalf("isNarrowableTimestamp(%q) = %v, want %v", typ, got, want)
		}
	}
}

func TestTimestampSafeProjection(t *testing.T) {
	got, needsCast := timestampSafeProjection([]athenatypes.ColumnInfo{
		col("time_dt", "timestamp"),
		col("src_endpoint", "row(ip varchar)"),
		col("severity_id", "integer"),
	})
	if !needsCast {
		t.Fatal("expected a cast to be required")
	}
	want := `CAST("time_dt" AS timestamp(3)) AS "time_dt", "src_endpoint", "severity_id"`
	if got != want {
		t.Fatalf("projection = %q, want %q", got, want)
	}
}

// Column order is the result order, because the header is built from the same metadata and
// the two must line up in the CSV.
func TestTimestampSafeProjectionPreservesColumnOrder(t *testing.T) {
	got, _ := timestampSafeProjection([]athenatypes.ColumnInfo{
		col("a", "varchar"), col("t", "timestamp"), col("z", "bigint"),
	})
	want := `"a", CAST("t" AS timestamp(3)) AS "t", "z"`
	if got != want {
		t.Fatalf("projection = %q, want %q", got, want)
	}
}

// Without a timestamp column there is nothing to fix, and the query must be left untouched
// rather than rewritten into an equivalent that only adds risk.
func TestTimestampSafeProjectionNoTimestampColumns(t *testing.T) {
	if _, needsCast := timestampSafeProjection([]athenatypes.ColumnInfo{col("a", "varchar")}); needsCast {
		t.Fatal("no timestamp column should mean no rewrite")
	}
	if _, needsCast := timestampSafeProjection(nil); needsCast {
		t.Fatal("empty column list should mean no rewrite")
	}
}

// A duplicate output name cannot be referenced unambiguously, so projecting by name would
// change which column is exported. Abandon the rewrite instead.
func TestTimestampSafeProjectionRejectsDuplicateNames(t *testing.T) {
	_, needsCast := timestampSafeProjection([]athenatypes.ColumnInfo{
		col("id", "timestamp"), col("id", "varchar"),
	})
	if needsCast {
		t.Fatal("duplicate output names must abandon the projection")
	}
}

// A name carrying a double quote cannot be quoted safely, so the projection is abandoned
// rather than emitting a statement the name could break out of.
func TestTimestampSafeProjectionRejectsUnquotableName(t *testing.T) {
	if _, needsCast := timestampSafeProjection([]athenatypes.ColumnInfo{col(`bad"name`, "timestamp")}); needsCast {
		t.Fatal("a column name containing a double quote must abandon the projection")
	}
	if _, needsCast := timestampSafeProjection([]athenatypes.ColumnInfo{col("  ", "timestamp")}); needsCast {
		t.Fatal("a blank column name must abandon the projection")
	}
}

// The query that failed in preprod: Security Lake OCSF, time_dt exposed as timestamp(6).
// The original query is nested verbatim, so its predicate and LIMIT keep their meaning.
func TestWrapWithProjectionNestsOriginalQuery(t *testing.T) {
	q := "SELECT * FROM amazon_security_lake_table_eu_north_1_route53_2_0 " +
		"WHERE time_dt >= from_unixtime(1772377558) LIMIT 100"
	got := wrapWithProjection(q, `CAST("time_dt" AS timestamp(3)) AS "time_dt", "srcaddr"`)
	want := `SELECT CAST("time_dt" AS timestamp(3)) AS "time_dt", "srcaddr" FROM (` + q +
		`) AS databahn_export_src`
	if got != want {
		t.Fatalf("wrapped = %q, want %q", got, want)
	}
}

// Wrapping is shape-agnostic: joins and explicit select lists nest just as well, which is why
// the projection is driven by result metadata rather than by parsing the query.
func TestWrapWithProjectionHandlesAnyShape(t *testing.T) {
	for _, q := range []string{
		"SELECT a.t, b.x FROM a JOIN b ON a.id = b.id",
		"WITH x AS (SELECT * FROM t) SELECT * FROM x",
		"SELECT count(*) AS c FROM tbl",
	} {
		got := wrapWithProjection(q, `"c"`)
		if got != `SELECT "c" FROM (`+q+`) AS databahn_export_src` {
			t.Fatalf("unexpected wrap for %q: %q", q, got)
		}
	}
}
