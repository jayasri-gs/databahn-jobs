package query

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	synapseParquetFileFormat = "DatabahnExportParquetFF"
	synapseCsvFileFormat     = "DatabahnExportCsvFF"
	synapseQueryTimeout      = 30 * time.Minute
	synapsePreflightTimeout  = 5 * time.Minute
	synapseProgressInterval  = 30 * time.Second
)

var validSynapseObjectName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type SynapseConfig struct {
	Workspace        string
	Database         string
	SqlUsername      string
	SqlPassword      string
	DataSourceName   string
	StagingContainer string
	StagingBlob      *destination.AzureBlobConfig
}

type SynapseExecutor struct {
	cfg           SynapseConfig
	db            *sql.DB
	log           *zap.Logger
	externalTable string
	stagingPrefix string
	cetasCancel   context.CancelFunc
	cetasMu       sync.Mutex
	cetasDone     bool
	cetasErr      error
}

type cetasParams struct {
	ExternalTable string
	DataSource    string
	FileFormat    string
	Location      string
	Query         string
}

func NewSynapseExecutor(cfg SynapseConfig) *SynapseExecutor {
	return &SynapseExecutor{cfg: cfg, log: logging.GetLogger()}
}

func (e *SynapseExecutor) SetLogger(log *zap.Logger) {
	if log != nil {
		e.log = log
	}
}

func (e *SynapseExecutor) Engine() string { return EngineSynapse }

func buildCetasSQL(p cetasParams) string {
	return fmt.Sprintf(`
CREATE EXTERNAL TABLE [dbo].[%s]
WITH (
    LOCATION = '%s',
    DATA_SOURCE = [%s],
    FILE_FORMAT = [%s]
)
AS
%s`, p.ExternalTable, p.Location, p.DataSource, p.FileFormat, p.Query)
}

func buildDropExternalTableSQL(table string) string {
	return fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.external_tables WHERE name = N'%s' AND schema_id = SCHEMA_ID('dbo'))
DROP EXTERNAL TABLE [dbo].[%s]`, table, table)
}

func mapCetasFileFormat(opts UnloadOptions) string {
	switch opts.Format {
	case "textfile":
		return synapseCsvFileFormat
	default:
		return synapseParquetFileFormat
	}
}

func sanitizeSynapseObjectName(name string) string {
	name = strings.ReplaceAll(name, "-", "_")
	if !validSynapseObjectName.MatchString(name) {
		return "DatabahnExport_" + strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				return r
			}
			return '_'
		}, name)
	}
	return name
}

func (e *SynapseExecutor) Connect(ctx context.Context) error {
	if e.cfg.Workspace == "" || e.cfg.Database == "" {
		return fmt.Errorf("synapse workspace and database are required")
	}
	if e.cfg.SqlUsername == "" || e.cfg.SqlPassword == "" {
		return fmt.Errorf("synapse SQL credentials are required")
	}
	if e.cfg.DataSourceName == "" {
		return fmt.Errorf("synapse data source name is required for CETAS")
	}

	timeoutSec := int(synapseQueryTimeout.Seconds())
	connString := fmt.Sprintf(
		"server=%s-ondemand.sql.azuresynapse.net;port=1433;database=%s;user id=%s;password=%s;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;connection timeout=%d",
		e.cfg.Workspace, e.cfg.Database, e.cfg.SqlUsername, e.cfg.SqlPassword, timeoutSec,
	)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return fmt.Errorf("failed to open synapse connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping synapse: %w", err)
	}
	e.db = db

	if err := e.ensureExportFileFormats(ctx); err != nil {
		db.Close()
		e.db = nil
		return err
	}
	return nil
}

func (e *SynapseExecutor) ensureExportFileFormats(ctx context.Context) error {
	q := fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = N'%s')
CREATE EXTERNAL FILE FORMAT [%s]
WITH (FORMAT_TYPE = PARQUET, DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec')`, synapseParquetFileFormat, synapseParquetFileFormat)
	if _, err := e.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("failed to ensure parquet export file format: %w", err)
	}
	return nil
}

func (e *SynapseExecutor) ensureCsvFileFormat(ctx context.Context, delimiter string) (string, error) {
	if delimiter == "" {
		delimiter = ","
	}
	formatName := synapseCsvFileFormat
	if delimiter != "," {
		formatName = sanitizeSynapseObjectName("DatabahnExportCsv_" + delimiter + "_FF")
	}
	escaped := strings.ReplaceAll(delimiter, "'", "''")
	q := fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = N'%s')
CREATE EXTERNAL FILE FORMAT [%s]
WITH (FORMAT_TYPE = DELIMITEDTEXT, FIELD_TERMINATOR = '%s', USE_TYPE_DEFAULT = true)`, formatName, formatName, escaped)
	if _, err := e.db.ExecContext(ctx, q); err != nil {
		return "", fmt.Errorf("failed to ensure csv export file format: %w", err)
	}
	return formatName, nil
}

func reportIDFromUnloadPath(outputPath string) string {
	if idx := strings.LastIndex(outputPath, "unload_"); idx >= 0 {
		rest := outputPath[idx+len("unload_"):]
		if end := strings.Index(rest, "_"); end > 0 {
			return rest[:end]
		}
		if end := strings.Index(rest, "/"); end > 0 {
			return rest[:end]
		}
		if rest != "" {
			return rest
		}
	}
	return strings.ReplaceAll(strings.Trim(outputPath, "/"), "/", "_")
}

func exportProbeQuery(query string) string {
	return fmt.Sprintf("SELECT TOP 1 * FROM (%s) AS databahn_export_probe", query)
}

// ValidateExportQuery runs a lightweight read against the export SQL before CETAS.
// Surfaces format/view/permission errors in seconds instead of after a full-range scan.
func (e *SynapseExecutor) ValidateExportQuery(ctx context.Context, query string) error {
	if e.db == nil {
		return fmt.Errorf("synapse not connected")
	}
	preflightCtx, cancel := context.WithTimeout(ctx, synapsePreflightTimeout)
	defer cancel()
	rows, err := e.db.QueryContext(preflightCtx, exportProbeQuery(query))
	if err != nil {
		return fmt.Errorf("synapse export preflight failed: %w", err)
	}
	defer rows.Close()
	return rows.Err()
}

func (e *SynapseExecutor) ExecuteUnloadAsync(ctx context.Context, query, database, outputPath string, opts UnloadOptions) (string, error) {
	if e.db == nil {
		return "", fmt.Errorf("synapse not connected")
	}

	tableName := sanitizeSynapseObjectName("DatabahnExport_" + reportIDFromUnloadPath(outputPath))
	e.externalTable = tableName
	e.stagingPrefix = strings.Trim(strings.TrimPrefix(outputPath, ".databahn_out/"), "/")

	fileFormat := mapCetasFileFormat(opts)
	if opts.Format == "textfile" {
		var err error
		fileFormat, err = e.ensureCsvFileFormat(ctx, opts.Delimiter)
		if err != nil {
			return "", err
		}
	}

	cetasSQL := buildCetasSQL(cetasParams{
		ExternalTable: tableName,
		DataSource:    e.cfg.DataSourceName,
		FileFormat:    fileFormat,
		Location:      e.stagingPrefix,
		Query:         query,
	})

	conn, err := e.db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("acquire synapse connection: %w", err)
	}
	var spid int
	if err := conn.QueryRowContext(ctx, "SELECT @@SPID").Scan(&spid); err != nil {
		conn.Close()
		return "", fmt.Errorf("failed to read synapse SPID: %w", err)
	}
	executionID := fmt.Sprintf("%d", spid)

	e.resetCetasResult()

	// CETAS runs on a dedicated connection; completion is tracked via connection result + DMV/blob polling.
	// WithoutCancel keeps short parent deadlines from aborting CETAS; cetasCancel stops it on shutdown.
	execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	e.cetasCancel = cancel
	go func() {
		defer cancel()
		defer conn.Close()
		_, err := conn.ExecContext(execCtx, cetasSQL)
		e.recordCetasResult(err)
		if err != nil {
			e.log.Warn("Synapse CETAS connection ended", zap.String("spid", executionID), zap.Error(err))
		}
	}()

	e.log.Info("Started Synapse CETAS", zap.String("spid", executionID), zap.String("externalTable", tableName))
	return executionID, nil
}

func (e *SynapseExecutor) resetCetasResult() {
	e.cetasMu.Lock()
	defer e.cetasMu.Unlock()
	e.cetasDone = false
	e.cetasErr = nil
}

func (e *SynapseExecutor) recordCetasResult(err error) {
	e.cetasMu.Lock()
	defer e.cetasMu.Unlock()
	e.cetasDone = true
	e.cetasErr = err
}

func (e *SynapseExecutor) cetasConnectionResult() (done bool, err error) {
	e.cetasMu.Lock()
	defer e.cetasMu.Unlock()
	return e.cetasDone, e.cetasErr
}

func (e *SynapseExecutor) stopCetas(executionID string) {
	if e.cetasCancel != nil {
		e.cetasCancel()
		e.cetasCancel = nil
	}
	if executionID == "" {
		return
	}
	killCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = e.CancelQueryExecution(killCtx, executionID)
}

func isDefinitiveCetasFailure(err error) bool {
	return err != nil && strings.Contains(err.Error(), "synapse CETAS failed:")
}

func (e *SynapseExecutor) WaitForExecution(ctx context.Context, executionID string) error {
	deadline := time.Now().Add(synapseQueryTimeout)
	started := time.Now()
	lastProgress := started
	for {
		status, err := e.CheckQueryStatus(ctx, executionID)
		if err != nil {
			if isDefinitiveCetasFailure(err) {
				e.stopCetas(executionID)
			}
			return err
		}
		switch status {
		case QueryStateSucceeded:
			return nil
		case QueryStateFailed, QueryStateCancelled:
			e.stopCetas(executionID)
			return fmt.Errorf("synapse CETAS failed")
		}
		now := time.Now()
		if now.Sub(lastProgress) >= synapseProgressInterval {
			e.log.Info("Synapse CETAS in progress",
				zap.String("spid", executionID),
				zap.String("status", status),
				zap.Duration("elapsed", now.Sub(started)))
			lastProgress = now
		}
		if now.After(deadline) {
			return fmt.Errorf("synapse CETAS timed out after %v", synapseQueryTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (e *SynapseExecutor) SetResumeStagingPrefix(outputPath string) {
	e.stagingPrefix = strings.Trim(strings.TrimPrefix(outputPath, ".databahn_out/"), "/")
}

func (e *SynapseExecutor) stagingHasOutput(ctx context.Context) (bool, error) {
	if e.stagingPrefix == "" || e.cfg.StagingBlob == nil {
		return false, nil
	}
	client, err := destination.NewAzureBlobClient(e.cfg.StagingBlob)
	if err != nil {
		return false, err
	}
	prefix := strings.TrimPrefix(e.stagingPrefix, "/")
	pager := client.NewListBlobsFlatPager(e.cfg.StagingContainer, &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("list staging blobs: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || strings.HasSuffix(*item.Name, "/") {
				continue
			}
			if item.Properties != nil && item.Properties.ContentLength != nil && *item.Properties.ContentLength > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (e *SynapseExecutor) sessionExists(ctx context.Context, spid int) (bool, error) {
	var id int
	err := e.db.QueryRowContext(ctx, `SELECT session_id FROM sys.dm_exec_sessions WHERE session_id = @p1`, spid).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *SynapseExecutor) CheckQueryStatus(ctx context.Context, executionID string) (string, error) {
	if done, err := e.cetasConnectionResult(); done {
		if err != nil {
			return QueryStateFailed, fmt.Errorf("synapse CETAS failed: %w", err)
		}
		hasFiles, checkErr := e.stagingHasOutput(ctx)
		if checkErr != nil {
			return "", checkErr
		}
		if hasFiles {
			return QueryStateSucceeded, nil
		}
		return QueryStateFailed, fmt.Errorf(
			"synapse CETAS failed: completed with no staging output at prefix %q", e.stagingPrefix)
	}
	return e.queryStatusFromDMV(ctx, executionID)
}

func (e *SynapseExecutor) queryStatusFromDMV(ctx context.Context, executionID string) (string, error) {
	if e.db == nil {
		return "", fmt.Errorf("synapse not connected")
	}
	var spid int
	if _, err := fmt.Sscanf(executionID, "%d", &spid); err != nil || spid <= 0 {
		return "", fmt.Errorf("invalid synapse execution id: %s", executionID)
	}

	var status string
	err := e.db.QueryRowContext(ctx, `
		SELECT TOP 1 status
		FROM sys.dm_exec_requests
		WHERE session_id = @p1`, spid).Scan(&status)
	if err == sql.ErrNoRows {
		hasFiles, checkErr := e.stagingHasOutput(ctx)
		if checkErr != nil {
			return "", checkErr
		}
		if hasFiles {
			return QueryStateSucceeded, nil
		}
		exists, sessErr := e.sessionExists(ctx, spid)
		if sessErr != nil {
			return "", sessErr
		}
		if !exists {
			return QueryStateFailed, nil
		}
		return QueryStateRunning, nil
	}
	if err != nil {
		return "", fmt.Errorf("poll synapse DMV: %w", err)
	}
	switch strings.ToLower(status) {
	case "running", "runnable", "suspended", "background":
		return QueryStateRunning, nil
	default:
		return QueryStateRunning, nil
	}
}

func (e *SynapseExecutor) GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error) {
	return &UnloadResult{OutputLocation: e.stagingPrefix}, nil
}

func (e *SynapseExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	if e.db == nil || executionID == "" {
		return nil
	}
	var spid int
	if _, err := fmt.Sscanf(executionID, "%d", &spid); err != nil || spid <= 0 {
		return fmt.Errorf("invalid synapse execution id: %s", executionID)
	}
	_, err := e.db.ExecContext(ctx, "DECLARE @killcmd NVARCHAR(32) = N'KILL ' + CONVERT(NVARCHAR(20), @p1); EXEC (@killcmd)", spid)
	return err
}

func (e *SynapseExecutor) GetQueryColumns(ctx context.Context, query, database string) ([]string, error) {
	if e.db == nil {
		return nil, fmt.Errorf("synapse not connected")
	}
	rows, err := e.db.QueryContext(ctx, `
		SELECT name
		FROM sys.dm_exec_describe_first_result_set(@p1, NULL, 0)
		WHERE name IS NOT NULL
		ORDER BY column_ordinal`, query)
	if err != nil {
		return nil, fmt.Errorf("describe synapse query columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan synapse column name: %w", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns returned for synapse query")
	}
	return columns, nil
}

func (e *SynapseExecutor) GetAWSConfig() interface{} { return nil }

func (e *SynapseExecutor) GetOutputLocation() string {
	if e.cfg.StagingContainer == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s", e.cfg.StagingContainer, ".databahn_out")
}

func (e *SynapseExecutor) NewStagingReader(tempDir string) (unload.StagingReader, error) {
	client, err := destination.NewAzureBlobClient(e.cfg.StagingBlob)
	if err != nil {
		return nil, err
	}
	return unload.NewBlobReader(client, e.cfg.StagingContainer, tempDir)
}

func (e *SynapseExecutor) Close() error {
	if e.db == nil {
		return nil
	}
	e.stopCetas("")
	if e.externalTable != "" {
		if _, err := e.db.Exec(buildDropExternalTableSQL(e.externalTable)); err != nil {
			e.log.Warn("Failed to drop synapse external table", zap.String("table", e.externalTable), zap.Error(err))
		}
	}
	return e.db.Close()
}
