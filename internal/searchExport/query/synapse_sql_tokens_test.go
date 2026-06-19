package query

import "testing"

func TestHasOrderByClause_multiline(t *testing.T) {
	if !hasOrderByClause("SELECT * FROM t\nORDER BY col") {
		t.Fatal("expected ORDER BY detected")
	}
}

func TestOuterSelectInsertPos_cte(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"
	pos, ok := outerSelectInsertPos(sql)
	if !ok {
		t.Fatal("expected outer select")
	}
	if sql[pos:] != "* FROM cte" {
		t.Fatalf("pos=%d tail=%q", pos, sql[pos:])
	}
}
