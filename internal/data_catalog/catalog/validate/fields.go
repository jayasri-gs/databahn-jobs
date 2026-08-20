package validate

import (
	"sort"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
)

// InvalidField is a catalog field rejected by validation, tagged with why.
type InvalidField struct {
	Field  model.Field
	Reason string // "leading_underscore" | "reserved_keyword" | "duplicate"
}

var athenaReservedKeywords = map[string]struct{}{}

func init() {
	kw := []string{
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
		"current_path", "current_role", "current_time", "current_user",
		"deallocate", "escape", "except", "execute", "first",
		"last", "localtime", "localtimestamp", "natural", "normalize",
		"prepare", "recursive", "skip", "uescape", "unnest",
	}
	for _, k := range kw {
		athenaReservedKeywords[k] = struct{}{}
	}
}

// PartitionCatalogFields splits unapplied fields into those safe to add and those to delete.
func PartitionCatalogFields(fields []model.Field, existingApplied []string) (valid []model.Field, invalid []InvalidField) {
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].ID < fields[j].ID })

	seen := make(map[string]struct{}, len(existingApplied))
	for _, n := range existingApplied {
		seen[strings.ToLower(n)] = struct{}{}
	}

	for _, f := range fields {
		lower := strings.ToLower(f.Name)
		switch {
		case strings.HasPrefix(f.Name, "_"):
			invalid = append(invalid, InvalidField{Field: f, Reason: "leading_underscore"})
		case isReservedKeyword(lower):
			invalid = append(invalid, InvalidField{Field: f, Reason: "reserved_keyword"})
		case isSeen(seen, lower):
			invalid = append(invalid, InvalidField{Field: f, Reason: "duplicate"})
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

func lowerFieldNames(fields []model.Field) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = strings.ToLower(f.Name)
	}
	return out
}
