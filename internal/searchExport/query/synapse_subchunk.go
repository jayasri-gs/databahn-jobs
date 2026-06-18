package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const synapseExportSortColumn = "db_edge_ts"

var synapseTopAfterSelectRe = regexp.MustCompile(`(?is)^\s*TOP\s*(?:\(\s*(\d+)\s*\)|(\d+))\s+`)

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
		if !hasOrderByClause(sql) {
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

// stripTop removes TOP from the outermost SELECT and returns the prior limit (0 if none).
func stripTop(sql string) (string, int64) {
	pos, ok := outerSelectInsertPos(sql)
	if !ok {
		return sql, 0
	}
	tail := sql[pos:]
	loc := synapseTopAfterSelectRe.FindStringSubmatchIndex(tail)
	if loc == nil {
		return sql, 0
	}
	groups := synapseTopAfterSelectRe.FindStringSubmatch(tail)
	limit := int64(0)
	if groups[1] != "" {
		if n, err := strconv.ParseInt(groups[1], 10, 64); err == nil {
			limit = n
		}
	} else if groups[2] != "" {
		if n, err := strconv.ParseInt(groups[2], 10, 64); err == nil {
			limit = n
		}
	}
	return sql[:pos] + tail[loc[1]:], limit
}

func injectTop(sql string, n int64) string {
	pos, ok := outerSelectInsertPos(sql)
	if !ok {
		return sql
	}
	return sql[:pos] + fmt.Sprintf("TOP (%d) ", n) + sql[pos:]
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
