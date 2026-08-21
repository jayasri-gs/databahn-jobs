package apply

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
)

var validSQLIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

func quoteSQLIdentifier(name string) (string, error) {
	if !validSQLIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %q", name)
	}
	return "`" + name + "`", nil
}

// BuildAddColumnsDDL builds an ALTER TABLE ADD COLUMNS statement for the given fields.
func BuildAddColumnsDDL(database, table string, fields []model.Field) (string, error) {
	quotedDB, err := quoteSQLIdentifier(database)
	if err != nil {
		return "", fmt.Errorf("invalid database name: %w", err)
	}
	quotedTable, err := quoteSQLIdentifier(table)
	if err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}
	var colDefs []string
	for _, f := range fields {
		quotedCol, err := quoteSQLIdentifier(f.Name)
		if err != nil {
			return "", fmt.Errorf("invalid column name %q: %w", f.Name, err)
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quotedCol, model.AthenaType(f.FieldType)))
	}
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMNS (%s)", quotedDB, quotedTable, strings.Join(colDefs, ", ")), nil
}
