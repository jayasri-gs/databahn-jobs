package synapse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// QueryFrequencyData queries frequency data from Azure Synapse (serverless SQL), returning the same aggregation shape as athena.QueryFrequencyData for insight health noise calculation.
func QueryFrequencyData(ctx context.Context, tenantId string, startDate, endDate time.Time) ([]athena.FrequencyAggregation, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Synapse client: %w", err)
	}

	query := buildFrequencyQuery(tenantId, startDate, endDate)
	logger.GetLogger().Info("executing Synapse query for frequency data",
		zap.String("tenant_id", tenantId),
		zap.String("query", query))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("Synapse query failed: %w", err)
	}
	defer rows.Close()

	var results []athena.FrequencyAggregation
	for rows.Next() {
		var key1, key2, sourceId string
		var year, month, date int
		var count float64
		if err := rows.Scan(&key1, &key2, &sourceId, &year, &month, &date, &count); err != nil {
			logger.GetLogger().Warn("skipping row on scan error", zap.Error(err))
			continue
		}
		// Compute end-of-day epoch ms in Go to avoid Synapse distributed-mode restrictions on DATETIME2/DATEDIFF_BIG.
		dayEnd := time.Date(year, time.Month(month), date, 23, 59, 59, 999*int(time.Millisecond), time.UTC)
		results = append(results, athena.FrequencyAggregation{
			Key1:            key1,
			Key2:            key2,
			SourceId:        sourceId,
			DayEndTimestamp: dayEnd.UnixMilli(),
			Count:           count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating Synapse results: %w", err)
	}

	logger.GetLogger().Info("Synapse query completed",
		zap.String("tenant_id", tenantId),
		zap.Int("result_count", len(results)))

	return results, nil
}

// buildFrequencyQuery returns a T-SQL query that returns key1, key2, source_id, year, month, date, total_count.
// day_end_timestamp is computed in Go from (year, month, date) to avoid Synapse serverless "not supported in
// distributed processing mode" errors from DATETIME2/DATEDIFF_BIG.
func buildFrequencyQuery(tenantId string, startDate, endDate time.Time) string {
	tenantIdUnderscore := strings.ReplaceAll(tenantId, "-", "_")
	database := fmt.Sprintf("databahn_tenant_%s", tenantIdUnderscore)
	table := fmt.Sprintf("tenant_%s_sourcehostname", tenantIdUnderscore)
	startYYYYMMDD := startDate.Year()*10000 + int(startDate.Month())*100 + startDate.Day()
	endYYYYMMDD := endDate.Year()*10000 + int(endDate.Month())*100 + endDate.Day()

	return fmt.Sprintf(`
		SELECT 
			sourcehostname AS key1,
			'' AS key2,
			source_id,
			CAST(year AS INT) AS year,
			CAST(month AS INT) AS month,
			CAST(date AS INT) AS date,
			SUM([count]) AS total_count
		FROM [%s].[dbo].[%s]
		WHERE CAST(CONCAT(RIGHT('0000' + CAST(year AS VARCHAR), 4), RIGHT('00' + CAST(month AS VARCHAR), 2), RIGHT('00' + CAST(date AS VARCHAR), 2)) AS INTEGER) BETWEEN %d AND %d
		GROUP BY sourcehostname, source_id, year, month, date
		ORDER BY sourcehostname, source_id, year, month, date
	`, database, table, startYYYYMMDD, endYYYYMMDD)
}
