package athena

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

var validSQLIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

const ddlPollInterval = 500 * time.Millisecond

// TableNotFoundError indicates the Athena table does not exist or DESCRIBE could not find it.
type TableNotFoundError struct{ Msg string }

func (e *TableNotFoundError) Error() string { return e.Msg }

// IsTableNotFound reports whether err signals a missing Athena table.
func IsTableNotFound(err error) bool {
	if err == nil {
		return false
	}
	var tnf *TableNotFoundError
	if errors.As(err, &tnf) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "table_not_found") ||
		strings.Contains(msg, "table not found") ||
		strings.Contains(msg, "does not exist")
}

func quoteSQLIdentifier(name string) (string, error) {
	if !validSQLIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %q", name)
	}
	return "`" + name + "`", nil
}

// buildDescribeQuery returns a DESCRIBE statement with quoted identifiers.
func buildDescribeQuery(database, table string) (string, error) {
	quotedDB, err := quoteSQLIdentifier(database)
	if err != nil {
		return "", fmt.Errorf("invalid database name: %w", err)
	}
	quotedTable, err := quoteSQLIdentifier(table)
	if err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}
	return fmt.Sprintf("DESCRIBE %s.%s", quotedDB, quotedTable), nil
}

// parseDescribeResultRows extracts lowercase column names from Athena DESCRIBE result rows.
// skipHeader should be true for the first page only.
func parseDescribeResultRows(rows []types.Row, skipHeader bool) map[string]struct{} {
	columns := make(map[string]struct{})
	start := 0
	if skipHeader && len(rows) > 0 {
		start = 1
	}
	for i := start; i < len(rows); i++ {
		row := rows[i]
		if len(row.Data) == 0 {
			continue
		}
		name := parseDescribeColumnName(datumString(row.Data[0]))
		if name == "" || strings.HasPrefix(name, "#") {
			continue
		}
		columns[name] = struct{}{}
	}
	return columns
}

func datumString(d types.Datum) string {
	if d.VarCharValue == nil {
		return ""
	}
	return *d.VarCharValue
}

// parseDescribeColumnName extracts the column name from a DESCRIBE result cell.
// Athena may return name and type in separate columns, or as one tab-separated value
// (e.g. "accountname         \tstring").
func parseDescribeColumnName(raw string) string {
	name := strings.TrimSpace(raw)
	if name == "" {
		return ""
	}
	if idx := strings.IndexByte(name, '\t'); idx >= 0 {
		name = strings.TrimSpace(name[:idx])
	}
	name = strings.Trim(name, "`")
	return strings.ToLower(name)
}

func mergeDescribeColumns(dest, src map[string]struct{}) {
	for k := range src {
		dest[k] = struct{}{}
	}
}

// GetTableColumnNames runs DESCRIBE against an Athena table and returns existing column names.
// The returned execution ID can be used for debug logging.
func GetTableColumnNames(ctx context.Context, client *athena.Client, database, table, outputLocation string) (map[string]struct{}, string, error) {
	query, err := buildDescribeQuery(database, table)
	if err != nil {
		return nil, "", err
	}

	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(database),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}
	result, err := client.StartQueryExecution(ctx, input)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start DESCRIBE query: %w", err)
	}
	executionID := aws.ToString(result.QueryExecutionId)

	if err := waitForQueryCompletion(ctx, client, executionID); err != nil {
		if IsTableNotFound(err) {
			return nil, executionID, &TableNotFoundError{Msg: err.Error()}
		}
		return nil, executionID, fmt.Errorf("DESCRIBE query failed: %w", err)
	}

	columns := make(map[string]struct{})
	var nextToken *string
	firstPage := true
	for {
		output, err := client.GetQueryResults(ctx, &athena.GetQueryResultsInput{
			QueryExecutionId: aws.String(executionID),
			NextToken:        nextToken,
			MaxResults:       aws.Int32(1000),
		})
		if err != nil {
			return nil, executionID, fmt.Errorf("failed to fetch DESCRIBE results: %w", err)
		}
		mergeDescribeColumns(columns, parseDescribeResultRows(output.ResultSet.Rows, firstPage))
		firstPage = false
		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}
	return columns, executionID, nil
}

// GetTableColumnNamesInRegion runs DESCRIBE using platform credentials in the given region.
func GetTableColumnNamesInRegion(ctx context.Context, region, database, table, outputLocation string) (map[string]struct{}, string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, "", fmt.Errorf("failed to load AWS config for region %s: %w", region, err)
	}
	client := athena.NewFromConfig(cfg)
	return GetTableColumnNames(ctx, client, database, table, outputLocation)
}

// RunDDL executes a DDL statement and waits for completion.
func RunDDL(ctx context.Context, client *athena.Client, query, outputLocation string) error {
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}
	result, err := client.StartQueryExecution(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return waitForQueryCompletionWithInterval(ctx, client, aws.ToString(result.QueryExecutionId), ddlPollInterval)
}

// RunDDLInRegion executes a DDL query in a specific AWS region with a custom output location.
func RunDDLInRegion(ctx context.Context, query, region, outputLocation string) error {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config for region %s: %w", region, err)
	}
	return RunDDL(ctx, athena.NewFromConfig(cfg), query, outputLocation)
}

func waitForQueryCompletionWithInterval(ctx context.Context, client *athena.Client, executionID string, pollInterval time.Duration) error {
	for {
		status, err := client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(executionID),
		})
		if err != nil {
			return fmt.Errorf("failed to get query status: %w", err)
		}
		state := status.QueryExecution.Status.State
		switch state {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed:
			reason := "unknown"
			if status.QueryExecution.Status.StateChangeReason != nil {
				reason = *status.QueryExecution.Status.StateChangeReason
			}
			return fmt.Errorf("query failed: %s", reason)
		case types.QueryExecutionStateCancelled:
			return fmt.Errorf("query was cancelled")
		case types.QueryExecutionStateQueued, types.QueryExecutionStateRunning:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
		}
	}
}
