package athena

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	commonAws "github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type FrequencyAggregation struct {
	Key1            string
	Key2            string
	SourceId        string
	DayEndTimestamp int64
	Count           float64
}

type AwsSearchSecret struct {
	Bucket string `json:"search.bucket"`
}

var (
	searchSecretOnce sync.Once
	searchSecret     *AwsSearchSecret
	searchSecretErr  error
)

// loadSearchSecret loads search bucket configuration from AWS Secrets Manager
func loadSearchSecret() error {
	searchSecretOnce.Do(func() {
		secretName := appConfig.GetAppConfiguration().GetString("search.secret_name")
		region := appConfig.GetAppConfiguration().GetString("region")

		logger.GetLogger().Info("attempting to load search secret from AWS Secrets Manager",
			zap.String("secret_name", secretName),
			zap.String("region", region))

		if secretName == "" {
			searchSecretErr = fmt.Errorf("search.secret_name not configured")
			logger.GetLogger().Error("search.secret_name not configured")
			return
		}

		data, err := commonAws.ReadSecretByName(secretName, region)
		if err != nil {
			searchSecretErr = fmt.Errorf("failed to read search secret from AWS Secrets Manager: %w", err)
			logger.GetLogger().Error("failed to read search secret",
				zap.String("secret_name", secretName),
				zap.String("region", region),
				zap.Error(err))
			return
		}

		searchSecret = &AwsSearchSecret{}
		err = json.Unmarshal([]byte(*data.SecretString), searchSecret)
		if err != nil {
			searchSecretErr = fmt.Errorf("failed to unmarshal search secret: %w", err)
			logger.GetLogger().Error("failed to unmarshal search secret", zap.Error(err))
			return
		}

		logger.GetLogger().Info("successfully loaded search bucket from AWS Secrets Manager",
			zap.String("bucket", searchSecret.Bucket))
	})

	return searchSecretErr
}

// getSearchBucket returns the search bucket from AWS Secrets Manager
func getSearchBucket() (string, error) {
	if err := loadSearchSecret(); err != nil {
		return "", err
	}

	if searchSecret.Bucket == "" {
		return "", fmt.Errorf("search.bucket not found in AWS Secrets Manager")
	}

	return searchSecret.Bucket, nil
}

// QueryFrequencyData queries frequency data from S3 via Athena
func QueryFrequencyData(ctx context.Context, tenantId string, startDate, endDate time.Time) ([]FrequencyAggregation, error) {
	athenaClient, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Athena client: %w", err)
	}

	// Build Athena query
	query := buildFrequencyQuery(tenantId, startDate, endDate)
	logger.GetLogger().Info("executing Athena query for frequency data",
		zap.String("tenant_id", tenantId),
		zap.String("query", query))

	// Execute query
	executionId, err := executeQuery(ctx, athenaClient, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Wait for query to complete
	err = waitForQueryCompletion(ctx, athenaClient, executionId)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Fetch and parse results
	results, err := fetchQueryResults(ctx, athenaClient, executionId)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch results: %w", err)
	}

	logger.GetLogger().Info("Athena query completed",
		zap.String("tenant_id", tenantId),
		zap.String("execution_id", executionId),
		zap.Int("result_count", len(results)))

	return results, nil
}

func buildFrequencyQuery(tenantId string, startDate, endDate time.Time) string {
	// Convert tenant ID from UUID format to underscore format
	// Example: 1be4494f-0251-4bf1-ad18-e09adc141aea -> 1be4494f_0251_4bf1_ad18_e09adc141aea
	tenantIdUnderscore := strings.ReplaceAll(tenantId, "-", "_")

	// Database name: databahn_tenant_{tenant_id_with_underscores}
	database := fmt.Sprintf("databahn_tenant_%s", tenantIdUnderscore)

	// Table name: tenant_{tenant_id_with_underscores}_sourcehostname
	table := fmt.Sprintf("tenant_%s_sourcehostname", tenantIdUnderscore)

	// Build date string for comparison (year, month, date are separate columns)
	// Calculate day_end_timestamp: end of day (23:59:59.999) for each date
	// Group by sourcehostname, source_id, and date to aggregate daily counts
	query := fmt.Sprintf(`
		SELECT 
			sourcehostname as key1,
			'' as key2,
			source_id,
			CAST(
				to_unixtime(
					date_parse(
						CAST(year AS VARCHAR) || '-' || 
						LPAD(CAST(month AS VARCHAR), 2, '0') || '-' || 
						LPAD(CAST(date AS VARCHAR), 2, '0') || ' 23:59:59.999',
						'%%Y-%%m-%%d %%H:%%i:%%s.%%f'
					)
				) * 1000 AS BIGINT
			) as day_end_timestamp,
			SUM(count) as total_count
		FROM %s.%s
		WHERE CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') || '-' || LPAD(CAST(date AS VARCHAR), 2, '0') 
			BETWEEN '%s' AND '%s'
		GROUP BY sourcehostname, source_id, year, month, date
		ORDER BY sourcehostname, source_id, year, month, date
	`, database, table,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"))

	return query
}

func executeQuery(ctx context.Context, client *athena.Client, query string) (string, error) {
	outputBucket, err := getSearchBucket()
	if err != nil {
		return "", fmt.Errorf("failed to get search bucket from AWS Secrets Manager: %w", err)
	}

	outputLocation := fmt.Sprintf("s3://%s/athena-results/", outputBucket)

	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}

	result, err := client.StartQueryExecution(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.QueryExecutionId, nil
}

func waitForQueryCompletion(ctx context.Context, client *athena.Client, executionId string) error {
	maxWaitTime := 5 * time.Minute
	pollInterval := 2 * time.Second
	deadline := time.Now().Add(maxWaitTime)

	for time.Now().Before(deadline) {
		input := &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(executionId),
		}

		result, err := client.GetQueryExecution(ctx, input)
		if err != nil {
			return err
		}

		status := result.QueryExecution.Status.State

		switch status {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed:
			reason := "unknown"
			if result.QueryExecution.Status.StateChangeReason != nil {
				reason = *result.QueryExecution.Status.StateChangeReason
			}
			return fmt.Errorf("query failed: %s", reason)
		case types.QueryExecutionStateCancelled:
			return fmt.Errorf("query was cancelled")
		case types.QueryExecutionStateQueued, types.QueryExecutionStateRunning:
			// Continue waiting
			time.Sleep(pollInterval)
		}
	}

	return fmt.Errorf("query execution timed out after %v", maxWaitTime)
}

func fetchQueryResults(ctx context.Context, client *athena.Client, executionId string) ([]FrequencyAggregation, error) {
	var results []FrequencyAggregation
	var nextToken *string

	for {
		input := &athena.GetQueryResultsInput{
			QueryExecutionId: aws.String(executionId),
			NextToken:        nextToken,
			MaxResults:       aws.Int32(1000),
		}

		output, err := client.GetQueryResults(ctx, input)
		if err != nil {
			return nil, err
		}

		// Skip header row on first page
		startIdx := 0
		if nextToken == nil && len(output.ResultSet.Rows) > 0 {
			startIdx = 1 // Skip header
		}

		for i := startIdx; i < len(output.ResultSet.Rows); i++ {
			row := output.ResultSet.Rows[i]
			if len(row.Data) < 5 {
				logger.GetLogger().Warn("skipping row with insufficient columns", zap.Int("column_count", len(row.Data)))
				continue
			}

			key1 := getStringValue(row.Data[0])
			key2 := getStringValue(row.Data[1])
			sourceId := getStringValue(row.Data[2])
			dayEndTimestamp, err := strconv.ParseInt(getStringValue(row.Data[3]), 10, 64)
			if err != nil {
				logger.GetLogger().Warn("failed to parse day_end_timestamp", zap.Error(err))
				continue
			}
			count, err := strconv.ParseFloat(getStringValue(row.Data[4]), 64)
			if err != nil {
				logger.GetLogger().Warn("failed to parse count", zap.Error(err))
				continue
			}

			results = append(results, FrequencyAggregation{
				Key1:            key1,
				Key2:            key2,
				SourceId:        sourceId,
				DayEndTimestamp: dayEndTimestamp,
				Count:           count,
			})
		}

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}

	return results, nil
}

func getStringValue(datum types.Datum) string {
	if datum.VarCharValue == nil {
		return ""
	}
	return *datum.VarCharValue
}
