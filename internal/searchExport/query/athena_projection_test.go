package query

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

func col(name, typ string) athenatypes.Column {
	return athenatypes.Column{Name: aws.String(name), Type: aws.String(typ)}
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
	cols := []athenatypes.Column{
		col("time_dt", "timestamp"),
		col("src_endpoint", "struct<ip:string>"),
		col("severity_id", "int"),
	}
	got, needsCast := timestampSafeProjection(cols)
	if !needsCast {
		t.Fatal("expected a cast to be required")
	}
	want := `CAST("time_dt" AS timestamp(3)) AS "time_dt", "src_endpoint", "severity_id"`
	if got != want {
		t.Fatalf("projection = %q, want %q", got, want)
	}
}

// Without a timestamp column there is nothing to fix, and the query must be left byte-identical
// rather than rewritten into an equivalent that only adds risk.
func TestTimestampSafeProjectionNoTimestampColumns(t *testing.T) {
	_, needsCast := timestampSafeProjection([]athenatypes.Column{col("a", "string"), col("b", "int")})
	if needsCast {
		t.Fatal("no timestamp column should mean no rewrite")
	}
	if _, needsCast := timestampSafeProjection(nil); needsCast {
		t.Fatal("empty column list should mean no rewrite")
	}
}

// A column name carrying a double quote cannot be quoted safely, so the whole projection is
// abandoned rather than emitting a statement an attacker could shape.
func TestTimestampSafeProjectionRejectsUnquotableName(t *testing.T) {
	if _, needsCast := timestampSafeProjection([]athenatypes.Column{col(`bad"name`, "timestamp")}); needsCast {
		t.Fatal(`a column name containing a double quote must abandon the projection`)
	}
}

// The query that failed in preprod: Security Lake OCSF, time_dt exposed as timestamp(6).
func TestRewriteSelectStarPreservesPredicateAndLimit(t *testing.T) {
	q := "SELECT * FROM amazon_security_lake_table_eu_north_1_route53_2_0 " +
		"WHERE time_dt >= from_unixtime(1772377558) AND time_dt < from_unixtime(1788275159) LIMIT 100"
	got, ok := rewriteSelectStar(q, `CAST("time_dt" AS timestamp(3)) AS "time_dt", "srcaddr"`)
	if !ok {
		t.Fatal("expected the generated export shape to be rewritten")
	}
	want := `SELECT CAST("time_dt" AS timestamp(3)) AS "time_dt", "srcaddr" ` +
		"FROM amazon_security_lake_table_eu_north_1_route53_2_0 " +
		"WHERE time_dt >= from_unixtime(1772377558) AND time_dt < from_unixtime(1788275159) LIMIT 100"
	if got != want {
		t.Fatalf("rewritten = %q, want %q", got, want)
	}
}

func TestSelectStarTable(t *testing.T) {
	tests := map[string]string{
		"SELECT * FROM vpc_flow_search LIMIT 10": "vpc_flow_search",
		`SELECT * FROM "db"."tbl" WHERE x = 1`:   "tbl",
		"select  *  from db.tbl":                 "tbl",
		"SELECT * FROM tbl":                      "tbl",
	}
	for q, want := range tests {
		got, ok := selectStarTable(q)
		if !ok || got != want {
			t.Fatalf("selectStarTable(%q) = %q,%v want %q", q, got, ok, want)
		}
	}
}

// Anything other than a plain select-star over one table is left alone: the projection is
// built from the table's columns, which are only the result columns in that one shape.
// The join and alias cases matter most — there the rewrite would silently drop the other
// relation's columns from the export rather than fail loudly.
func TestSelectStarTableRejectsOtherShapes(t *testing.T) {
	for _, q := range []string{
		"SELECT time_dt, srcaddr FROM tbl",
		"SELECT * FROM a JOIN b ON a.id = b.id",
		"SELECT * FROM a, b WHERE a.id = b.id",
		"SELECT * FROM tbl AS t WHERE t.x = 1",
		"SELECT * FROM tbl t",
		"SELECT count(*) FROM tbl",
		"WITH x AS (SELECT * FROM t) SELECT * FROM x",
	} {
		if _, ok := selectStarTable(q); ok {
			t.Fatalf("query must not be rewritten: %q", q)
		}
	}
}

// The clause keywords that may legitimately follow the table are still rewritten.
func TestSelectStarTableAcceptsClauseKeywords(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM tbl",
		"SELECT * FROM tbl;",
		"SELECT * FROM tbl WHERE x = 1",
		"SELECT * FROM tbl ORDER BY x LIMIT 5",
		"SELECT * FROM tbl LIMIT 100",
	} {
		if _, ok := selectStarTable(q); !ok {
			t.Fatalf("query should be rewritten: %q", q)
		}
	}
}
