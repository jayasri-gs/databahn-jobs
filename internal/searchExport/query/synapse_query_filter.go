package query

import (
	"strings"
)

// AddPartitionFilter appends a partition predicate without changing existing user filters.
func AddPartitionFilter(query, partitionFilter string) string {
	q := strings.TrimSpace(query)
	f := strings.TrimSpace(partitionFilter)
	if f == "" {
		return q
	}
	upper := strings.ToUpper(q)
	if strings.Contains(upper, " WHERE ") {
		return q + " AND (" + f + ")"
	}
	return q + " WHERE (" + f + ")"
}
