package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type AthenaConfig struct {
	Region          string
	Workgroup       string
	OutputLocation  string
	QueryTimeout    time.Duration
	AuthType        string
	AccessKeyID     string
	SecretAccessKey string
	RoleArn         string
	ExternalID      string

	// NarrowTimestamps opts this executor into rewriting the export query so timestamp
	// columns cannot trip UNLOAD's millisecond writer. Only sources known to expose
	// microsecond timestamps set it -- see narrowTimestampsForUnload.
	NarrowTimestamps bool
}

type AthenaExecutor struct {
	cfg       AthenaConfig
	client    *athena.Client
	awsConfig aws.Config
	log       *zap.Logger

	// Column metadata costs a real Athena execution, and one export asks for it twice: once
	// to decide whether timestamps need narrowing, once to build the CSV header. An executor
	// serves a single export, so memoising the last answer collapses that to one query.
	colMetaMu     sync.Mutex
	colMetaKey    string
	colMeta       []athenatypes.ColumnInfo
	colMetaCached bool
}

func NewAthenaExecutor(cfg AthenaConfig) *AthenaExecutor {
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 30 * time.Minute
	}
	return &AthenaExecutor{cfg: cfg, log: logging.GetLogger()}
}

func (e *AthenaExecutor) SetLogger(log *zap.Logger) {
	if log != nil {
		e.log = log
	}
}

func (e *AthenaExecutor) Connect(ctx context.Context) error {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(e.cfg.Region))

	if e.cfg.AuthType == "role_based" {
		baseCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(e.cfg.Region))
		if err != nil {
			return fmt.Errorf("failed to load base AWS config: %w", err)
		}
		stsClient := sts.NewFromConfig(baseCfg)
		opts = append(opts, awsconfig.WithCredentialsProvider(
			stscreds.NewAssumeRoleProvider(stsClient, e.cfg.RoleArn, func(o *stscreds.AssumeRoleOptions) {
				if e.cfg.ExternalID != "" {
					o.ExternalID = aws.String(e.cfg.ExternalID)
				}
			}),
		))
	} else if e.cfg.AccessKeyID != "" && e.cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				e.cfg.AccessKeyID,
				e.cfg.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	e.awsConfig = awsCfg
	e.client = athena.NewFromConfig(awsCfg)
	e.log.Info("Connected to Athena",
		zap.String("region", e.cfg.Region),
		zap.String("workgroup", e.cfg.Workgroup),
		zap.String("outputLocation", e.cfg.OutputLocation),
		zap.String("authType", e.cfg.AuthType))
	return nil
}

func quoteAthenaUnloadURI(s3OutputPath string) (string, error) {
	if strings.ContainsFunc(s3OutputPath, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return "", fmt.Errorf("athena unload path contains control characters")
	}
	if strings.ContainsAny(s3OutputPath, `'"\`) {
		return "", fmt.Errorf("athena unload path contains quotes or escape characters")
	}
	if !strings.HasPrefix(s3OutputPath, "s3://") {
		return "", fmt.Errorf("athena unload path must be an s3 URI")
	}
	return "'" + s3OutputPath + "'", nil
}

func buildUnloadSQL(query, s3OutputPath string, opts UnloadOptions) (string, error) {
	location, err := quoteAthenaUnloadURI(s3OutputPath)
	if err != nil {
		return "", err
	}
	switch opts.Format {
	case "textfile":
		delim := opts.Delimiter
		if delim == "" {
			delim = ","
		}
		return fmt.Sprintf(
			"UNLOAD (%s) TO %s WITH (format = 'TEXTFILE', field_delimiter = '%s')",
			query, location, strings.ReplaceAll(delim, "'", "''"),
		), nil
	case "json":
		return fmt.Sprintf("UNLOAD (%s) TO %s WITH (format = 'JSON')", query, location), nil
	default:
		return fmt.Sprintf(
			"UNLOAD (%s) TO %s WITH (format = 'PARQUET', compression = 'SNAPPY')",
			query, location,
		), nil
	}
}

func (e *AthenaExecutor) Engine() string { return EngineAthena }

func (e *AthenaExecutor) NewStagingReader(tempDir string) (unload.StagingReader, error) {
	return unload.NewReader(e.awsConfig, tempDir)
}

func (e *AthenaExecutor) ExecuteUnloadAsync(ctx context.Context, query, database, s3OutputPath string, opts UnloadOptions) (string, error) {
	// UNLOAD writes through Athena's millisecond-only Hive sink, so a timestamp(6) result
	// column fails the statement outright. See narrowTimestampsForUnload.
	query = e.narrowTimestampsForUnload(ctx, query, database)
	unloadQuery, err := buildUnloadSQL(query, s3OutputPath, opts)
	if err != nil {
		return "", err
	}
	e.log.Info("Starting async UNLOAD query",
		zap.String("database", database),
		zap.String("outputPath", s3OutputPath),
		zap.String("unloadFormat", opts.Format))

	startInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(unloadQuery),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(database),
		},
		WorkGroup: aws.String(e.cfg.Workgroup),
	}
	if e.cfg.OutputLocation != "" {
		startInput.ResultConfiguration = &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(e.cfg.OutputLocation),
		}
	}
	output, err := e.client.StartQueryExecution(ctx, startInput)
	if err != nil {
		return "", fmt.Errorf("failed to start UNLOAD query: %w", err)
	}
	queryID := aws.ToString(output.QueryExecutionId)
	e.log.Info("Started async UNLOAD query", zap.String("athenaQueryExecutionId", queryID))
	return queryID, nil
}

func (e *AthenaExecutor) waitForCompletion(ctx context.Context, queryID string) error {
	deadline := time.Now().Add(e.cfg.QueryTimeout)
	started := time.Now()
	lastLog := started

	for {
		if time.Now().After(deadline) {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, _ = e.client.StopQueryExecution(stopCtx, &athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			cancel()
			e.log.Warn("Athena query timed out — execution cancelled",
				zap.String("athenaQueryExecutionId", queryID),
				zap.Duration("timeout", e.cfg.QueryTimeout))
			return fmt.Errorf("query timed out after %v (execution %s cancelled)", e.cfg.QueryTimeout, queryID)
		}

		output, err := e.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return fmt.Errorf("failed to get query status: %w", err)
		}

		state := output.QueryExecution.Status.State
		if time.Since(lastLog) >= 30*time.Second {
			fields := []zap.Field{
				zap.String("athenaQueryExecutionId", queryID),
				zap.String("state", string(state)),
				zap.Duration("elapsed", time.Since(started)),
			}
			if output.QueryExecution.Statistics != nil {
				stats := output.QueryExecution.Statistics
				if stats.EngineExecutionTimeInMillis != nil {
					fields = append(fields, zap.Int64("engineMs", *stats.EngineExecutionTimeInMillis))
				}
				if stats.DataScannedInBytes != nil {
					fields = append(fields, zap.Int64("bytesScanned", *stats.DataScannedInBytes))
				}
			}
			e.log.Info("Athena query in progress", fields...)
			lastLog = time.Now()
		}

		switch state {
		case athenatypes.QueryExecutionStateSucceeded:
			e.log.Info("Athena query succeeded",
				zap.String("athenaQueryExecutionId", queryID),
				zap.Duration("elapsed", time.Since(started)))
			return nil
		case athenatypes.QueryExecutionStateFailed:
			reason := ""
			if output.QueryExecution.Status.StateChangeReason != nil {
				reason = *output.QueryExecution.Status.StateChangeReason
			}
			return fmt.Errorf("query failed: %s", reason)
		case athenatypes.QueryExecutionStateCancelled:
			return fmt.Errorf("query was cancelled")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// GetQueryColumns runs a zero-row version of the query to read column names from result metadata.
func (e *AthenaExecutor) GetQueryColumns(ctx context.Context, query, database string) ([]string, error) {
	cols, err := e.queryColumnMetadata(ctx, query, database)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(cols))
	for _, col := range cols {
		if col.Name != nil {
			columns = append(columns, *col.Name)
		}
	}
	return columns, nil
}

// queryColumnMetadata runs a zero-row version of the query and returns its result column
// metadata: names, in output order, with their Athena types. Both the CSV header and the
// UNLOAD timestamp narrowing are derived from this one source so they cannot disagree.
//
// The result is memoised per (database, query): the two callers pass the same export query,
// so without this a Security Lake CSV export would run the probe twice. Failures are not
// cached, leaving a transient error retryable by the next caller.
func (e *AthenaExecutor) queryColumnMetadata(ctx context.Context, query, database string) ([]athenatypes.ColumnInfo, error) {
	cacheKey := columnMetadataCacheKey(query, database)
	if cols, ok := e.cachedColumnMetadata(cacheKey); ok {
		return cols, nil
	}

	metaQuery := fmt.Sprintf("SELECT * FROM (%s) AS export_src LIMIT 0", query)

	startInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(metaQuery),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(database),
		},
		WorkGroup: aws.String(e.cfg.Workgroup),
	}
	if e.cfg.OutputLocation != "" {
		startInput.ResultConfiguration = &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(e.cfg.OutputLocation),
		}
	}

	startOutput, err := e.client.StartQueryExecution(ctx, startInput)
	if err != nil {
		return nil, fmt.Errorf("failed to start column metadata query: %w", err)
	}

	queryID := *startOutput.QueryExecutionId
	if err := e.waitForCompletion(ctx, queryID); err != nil {
		return nil, fmt.Errorf("column metadata query failed: %w", err)
	}

	resultOutput, err := e.client.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(queryID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get column metadata: %w", err)
	}

	var cols []athenatypes.ColumnInfo
	if resultOutput.ResultSet != nil && resultOutput.ResultSet.ResultSetMetadata != nil {
		cols = resultOutput.ResultSet.ResultSetMetadata.ColumnInfo
	}
	e.storeColumnMetadata(cacheKey, cols)
	return cols, nil
}

// columnMetadataCacheKey pairs the query with its database. The NUL separator keeps a database
// name ending in query text from colliding with a different pair.
func columnMetadataCacheKey(query, database string) string {
	return database + "\x00" + query
}

// cachedColumnMetadata returns the memoised metadata for key, if it is the one held. The lock
// is not held across the Athena call, so a miss costs only the map-free comparison here.
func (e *AthenaExecutor) cachedColumnMetadata(key string) ([]athenatypes.ColumnInfo, bool) {
	e.colMetaMu.Lock()
	defer e.colMetaMu.Unlock()
	if e.colMetaCached && e.colMetaKey == key {
		return e.colMeta, true
	}
	return nil, false
}

func (e *AthenaExecutor) storeColumnMetadata(key string, cols []athenatypes.ColumnInfo) {
	e.colMetaMu.Lock()
	defer e.colMetaMu.Unlock()
	e.colMetaKey, e.colMeta, e.colMetaCached = key, cols, true
}

func (e *AthenaExecutor) GetAWSConfig() interface{} {
	return e.awsConfig
}

func (e *AthenaExecutor) GetOutputLocation() string {
	return e.cfg.OutputLocation
}

// CheckQueryStatus returns the Athena query state string (RUNNING, SUCCEEDED, FAILED, CANCELLED, QUEUED).
func (e *AthenaExecutor) CheckQueryStatus(ctx context.Context, executionID string) (string, error) {
	output, err := e.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(executionID),
	})
	if err != nil {
		return "", fmt.Errorf("get query execution status: %w", err)
	}
	return string(output.QueryExecution.Status.State), nil
}

// WaitForExecution reattaches to an already-started Athena query and waits for completion.
func (e *AthenaExecutor) WaitForExecution(ctx context.Context, executionID string) error {
	return e.waitForCompletion(ctx, executionID)
}

// GetExecutionResult fetches output metadata for a SUCCEEDED Athena execution.
func (e *AthenaExecutor) GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error) {
	output, err := e.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(executionID),
	})
	if err != nil {
		return nil, fmt.Errorf("get execution result: %w", err)
	}
	result := &UnloadResult{}
	if output.QueryExecution != nil && output.QueryExecution.Statistics != nil {
		stats := output.QueryExecution.Statistics
		if stats.DataManifestLocation != nil {
			result.ManifestLocation = *stats.DataManifestLocation
		}
		if stats.DataScannedInBytes != nil {
			result.BytesScanned = *stats.DataScannedInBytes
		}
	}
	return result, nil
}

// CancelQueryExecution stops a running Athena query execution.
func (e *AthenaExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := e.client.StopQueryExecution(stopCtx, &athena.StopQueryExecutionInput{
		QueryExecutionId: aws.String(executionID),
	})
	if err != nil {
		return fmt.Errorf("cancel query execution: %w", err)
	}
	return nil
}

func (e *AthenaExecutor) Close() error {
	return nil
}
