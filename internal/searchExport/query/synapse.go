package query

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/databahn-ai/databahn-jobs/internal/store/datastore"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type SynapseConfig struct {
	Workspace      string
	Database       string
	SqlUsername    string
	SqlPassword    string
	DataSourceName string // optional metadata; unused by JDBC stream
}

type SynapseExecutor struct {
	cfg    SynapseConfig
	db     *sql.DB
	log    *zap.Logger
	spid   int
	spidMu sync.Mutex
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

func (e *SynapseExecutor) Connect(ctx context.Context) error {
	if e.cfg.Workspace == "" || e.cfg.Database == "" {
		return fmt.Errorf("synapse workspace and database are required")
	}
	if e.cfg.SqlUsername == "" || e.cfg.SqlPassword == "" {
		return fmt.Errorf("synapse SQL credentials are required")
	}
	db, err := datastore.OpenSynapseSQL(ctx, e.cfg.Workspace, e.cfg.Database, e.cfg.SqlUsername, e.cfg.SqlPassword)
	if err != nil {
		return fmt.Errorf("failed to ping synapse: %w", err)
	}
	e.db = db
	return nil
}

// ValidateExportQuery runs the export SQL with ROWCOUNT 1 on a dedicated connection.
func (e *SynapseExecutor) ValidateExportQuery(ctx context.Context, query string) error {
	if e.db == nil {
		return fmt.Errorf("synapse not connected")
	}
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("synapse export preflight connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET ROWCOUNT 1"); err != nil {
		return fmt.Errorf("synapse export preflight failed: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "SET ROWCOUNT 0") }()

	preflightCtx, cancel := context.WithTimeout(ctx, synapsePreflightTimeout)
	defer cancel()
	rows, err := conn.QueryContext(preflightCtx, query)
	if err != nil {
		return fmt.Errorf("synapse export preflight failed: %w", err)
	}
	defer rows.Close()
	return rows.Err()
}

func (e *SynapseExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	if e.db == nil {
		return nil
	}
	spid := 0
	if executionID != "" {
		_, _ = fmt.Sscanf(executionID, "%d", &spid)
	}
	if spid <= 0 {
		e.spidMu.Lock()
		spid = e.spid
		e.spidMu.Unlock()
	}
	if spid <= 0 {
		return nil
	}
	_, err := e.db.ExecContext(ctx, "DECLARE @killcmd NVARCHAR(32) = N'KILL ' + CONVERT(NVARCHAR(20), @p1); EXEC (@killcmd)", spid)
	return err
}

func (e *SynapseExecutor) GetQueryColumns(ctx context.Context, query, database string) ([]string, error) {
	_ = database
	if e.db == nil {
		return nil, fmt.Errorf("synapse not connected")
	}
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("synapse column discovery connection: %w", err)
	}
	defer conn.Close()

	colCtx, cancel := context.WithTimeout(ctx, synapsePreflightTimeout)
	defer cancel()

	if _, err := conn.ExecContext(colCtx, "SET ROWCOUNT 1"); err != nil {
		return nil, fmt.Errorf("describe synapse query columns: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "SET ROWCOUNT 0") }()

	rows, err := conn.QueryContext(colCtx, query)
	if err != nil {
		return nil, fmt.Errorf("describe synapse query columns: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("describe synapse query columns: %w", err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns returned for synapse query")
	}
	return columns, nil
}

func (e *SynapseExecutor) GetAWSConfig() interface{} { return nil }

func (e *SynapseExecutor) Close() error {
	if e.db == nil {
		return nil
	}
	_ = e.CancelQueryExecution(context.Background(), "")
	return e.db.Close()
}

func (e *SynapseExecutor) setSPID(spid int) {
	e.spidMu.Lock()
	e.spid = spid
	e.spidMu.Unlock()
}
