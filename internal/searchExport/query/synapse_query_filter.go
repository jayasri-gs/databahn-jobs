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
