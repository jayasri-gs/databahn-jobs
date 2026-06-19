package query

import (
	"regexp"
	"strings"
)

var sqlWhereClauseRe = regexp.MustCompile(`(?i)\bWHERE\b`)

// AddPartitionFilter appends a partition predicate without changing existing user filters.
func AddPartitionFilter(query, partitionFilter string) string {
	q := strings.TrimSpace(query)
	f := strings.TrimSpace(partitionFilter)
	if f == "" {
		return q
	}
	if sqlWhereClauseRe.MatchString(q) {
		return q + " AND (" + f + ")"
	}
	return q + " WHERE (" + f + ")"
}

// WrapWithPartitionFilter wraps innerSQL as a subquery and applies partitionFilter on the
// outside. Safe for queries that end with ORDER BY, LIMIT, OFFSET, or a semicolon, where
// appending AND/WHERE directly would produce invalid SQL.
func WrapWithPartitionFilter(innerSQL, partitionFilter string) string {
	q := strings.TrimRight(strings.TrimSpace(innerSQL), ";")
	f := strings.TrimSpace(partitionFilter)
	if f == "" {
		return q
	}
	return "SELECT * FROM (\n" + q + "\n) AS _q WHERE (" + f + ")"
}
