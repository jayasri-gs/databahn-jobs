package data_catalog

import (
	"sort"
	"strings"
)

// invalidField is a catalog field rejected by validation, tagged with why.
type invalidField struct {
	field  catalogField
	reason string // "leading_underscore" | "reserved_keyword" | "duplicate"
}

// athenaReservedKeywords is the lowercased union of Athena reserved DDL and
// reserved SELECT keywords. A column name matching any of these can break the
// ALTER TABLE or later queries.
// Ref: https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
var athenaReservedKeywords = map[string]struct{}{}

func init() {
	kw := []string{
		// Reserved DDL keywords
		"all", "alter", "and", "array", "as", "authorization", "between",
		"bigint", "binary", "boolean", "both", "by", "cache", "case", "cast",
		"char", "column", "conf", "constraint", "commit", "create", "cross",
		"cube", "current", "current_date", "current_timestamp", "cursor",
		"database", "date", "dayofweek", "decimal", "delete", "describe",
		"distinct", "double", "drop", "else", "end", "exchange", "exists",
		"extended", "external", "extract", "false", "fetch", "float", "floor",
		"following", "for", "foreign", "from", "full", "function", "grant",
		"group", "grouping", "having", "if", "import", "in", "inner", "insert",
		"int", "integer", "intersect", "interval", "into", "is", "join",
		"lateral", "left", "less", "like", "local", "macro", "map", "more",
		"none", "not", "null", "numeric", "of", "on", "only", "or", "order",
		"out", "outer", "over", "partialscan", "partition", "percent",
		"preceding", "precision", "preserve", "primary", "procedure", "range",
		"reads", "reduce", "regexp", "references", "revoke", "right", "rlike",
		"rollup", "row", "rows", "select", "set", "smallint", "start", "table",
		"tablesample", "then", "time", "timestamp", "to", "transform", "trigger",
		"true", "truncate", "unbounded", "union", "uniquejoin", "update", "user",
		"using", "utc_timestamp", "values", "varchar", "views", "when", "where",
		"window", "with",
		// Reserved SELECT-statement keywords not already listed above
		"current_path", "current_role", "current_time", "current_user",
		"deallocate", "escape", "except", "execute", "first",
		"last", "localtime", "localtimestamp", "natural", "normalize",
		"prepare", "recursive", "skip", "uescape", "unnest",
	}
	for _, k := range kw {
		athenaReservedKeywords[k] = struct{}{}
	}
}

// partitionCatalogFields splits unapplied fields into those safe to add and
// those to delete. existingApplied holds the lowercased names of columns already
// applied to the same table; they seed the duplicate "seen" set so an already-
// existing column is never re-added. Fields are evaluated in ascending id order
// so the oldest row survives a duplicate. Checks per field, first hit wins:
// leading underscore, then reserved keyword, then duplicate.
// The input slice is sorted in place by id; callers do not rely on its order.
func partitionCatalogFields(fields []catalogField, existingApplied []string) (valid []catalogField, invalid []invalidField) {
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].ID < fields[j].ID })

	seen := make(map[string]struct{}, len(existingApplied))
	for _, n := range existingApplied {
		seen[strings.ToLower(n)] = struct{}{}
	}

	for _, f := range fields {
		lower := strings.ToLower(f.Name)
		switch {
		case strings.HasPrefix(f.Name, "_"):
			invalid = append(invalid, invalidField{field: f, reason: "leading_underscore"})
		case isReservedKeyword(lower):
			invalid = append(invalid, invalidField{field: f, reason: "reserved_keyword"})
		case isSeen(seen, lower):
			invalid = append(invalid, invalidField{field: f, reason: "duplicate"})
		default:
			seen[lower] = struct{}{}
			valid = append(valid, f)
		}
	}
	return valid, invalid
}

func isReservedKeyword(lower string) bool {
	_, ok := athenaReservedKeywords[lower]
	return ok
}

func isSeen(seen map[string]struct{}, lower string) bool {
	_, ok := seen[lower]
	return ok
}

// lowerFieldNames returns the lowercased names of the given fields, used to
// scope the applied-column lookup to only names that could collide.
func lowerFieldNames(fields []catalogField) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = strings.ToLower(f.Name)
	}
	return out
}
