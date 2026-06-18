package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const synapseExportSortColumn = "db_edge_ts"

var synapseSelectTopRe = regexp.MustCompile(`(?is)(\bSELECT\s+(?:DISTINCT\s+)?)TOP\s*(?:\(\s*(\d+)\s*\)|(\d+))\s+`)

// BuildSubChunkQuery adds TOP/keyset pagination for large single-hour exports.
// When batchRows <= 0, only keyset continuation is applied when lastSortKey is set.
func BuildSubChunkQuery(hourSQL string, batchRows int64, lastSortKey string) string {
	sql, existingTop := stripTop(hourSQL)
	topN := effectiveTopLimit(batchRows, existingTop)
	if topN > 0 {
		sql = injectTop(sql, topN)
	}
	if lastSortKey != "" {
		sql = AddPartitionFilter(sql, fmt.Sprintf("%s > '%s'", synapseExportSortColumn, escapeSQLLiteral(lastSortKey)))
	}
	if topN > 0 || lastSortKey != "" {
		if !strings.Contains(strings.ToUpper(sql), " ORDER BY ") {
			sql += " ORDER BY " + synapseExportSortColumn
		}
	}
	return sql
}

func effectiveTopLimit(batchRows int64, existingTop int64) int64 {
	if batchRows <= 0 && existingTop <= 0 {
		return 0
	}
	if batchRows <= 0 {
		return existingTop
	}
	if existingTop <= 0 {
		return batchRows
	}
	if batchRows < existingTop {
		return batchRows
	}
	return existingTop
}

// stripTop removes an existing SELECT TOP clause and returns the prior limit (0 if none).
func stripTop(sql string) (string, int64) {
	loc := synapseSelectTopRe.FindStringSubmatchIndex(sql)
	if loc == nil {
		return sql, 0
	}
	groups := synapseSelectTopRe.FindStringSubmatch(sql)
	limit := int64(0)
	if groups[2] != "" {
		if n, err := strconv.ParseInt(groups[2], 10, 64); err == nil {
			limit = n
		}
	} else if groups[3] != "" {
		if n, err := strconv.ParseInt(groups[3], 10, 64); err == nil {
			limit = n
		}
	}
	stripped := sql[:loc[0]] + groups[1] + sql[loc[1]:]
	return stripped, limit
}

func injectTop(sql string, n int64) string {
	upper := strings.ToUpper(sql)
	top := fmt.Sprintf("TOP (%d) ", n)
	if i := strings.Index(upper, "SELECT DISTINCT "); i >= 0 {
		pos := i + len("SELECT DISTINCT ")
		return sql[:pos] + top + sql[pos:]
	}
	if i := strings.Index(upper, "SELECT "); i >= 0 {
		pos := i + len("SELECT ")
		return sql[:pos] + top + sql[pos:]
	}
	return sql
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
