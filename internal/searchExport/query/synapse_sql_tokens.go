package query

import (
	"regexp"
	"strings"
)

var sqlOrderByClauseRe = regexp.MustCompile(`(?i)\bORDER\s+BY\b`)

func hasOrderByClause(sql string) bool {
	return sqlOrderByClauseRe.MatchString(sql)
}

// outerSelectInsertPos returns the byte offset immediately after the outermost SELECT keyword.
func outerSelectInsertPos(sql string) (int, bool) {
	upper := strings.ToUpper(sql)
	depth := 0
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth != 0 {
				continue
			}
			if strings.HasPrefix(upper[i:], "SELECT DISTINCT ") {
				return i + len("SELECT DISTINCT "), true
			}
			if strings.HasPrefix(upper[i:], "SELECT ") {
				return i + len("SELECT "), true
			}
		}
	}
	return 0, false
}
