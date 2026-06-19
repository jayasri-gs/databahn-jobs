package query

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

func normalizeCellValue(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func checkCellSize(v interface{}) error {
	switch x := v.(type) {
	case []byte:
		if len(x) > maxLOBBytes {
			return fmt.Errorf("synapse cell exceeds 1MB LOB limit (%d bytes)", len(x))
		}
	case string:
		if len(x) > maxLOBBytes {
			return fmt.Errorf("synapse cell exceeds 1MB LOB limit (%d bytes)", len(x))
		}
	}
	return nil
}

func (e *SynapseExecutor) StreamRows(
	ctx context.Context,
	query string,
	opts StreamRowsOptions,
	fn func(row []interface{}) error,
) (int64, error) {
	if e.db == nil {
		return 0, fmt.Errorf("synapse not connected")
	}
	if opts.ProgressEvery <= 0 {
		opts.ProgressEvery = synapseProgressInterval
	}
	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = time.Duration(defaultSynapseQueryTimeoutMin) * time.Minute
	}

	streamCtx, cancel := context.WithTimeout(ctx, opts.QueryTimeout)
	defer cancel()

	conn, err := e.db.Conn(streamCtx)
	if err != nil {
		return 0, fmt.Errorf("synapse stream connection: %w", err)
	}
	defer conn.Close()

	var spid int
	if err := conn.QueryRowContext(streamCtx, "SELECT @@SPID").Scan(&spid); err != nil {
		return 0, fmt.Errorf("synapse SPID: %w", err)
	}
	e.setSPID(spid)
	defer e.setSPID(0)

	if opts.ServerRowCap > 0 {
		if _, err := conn.ExecContext(streamCtx, fmt.Sprintf("SET ROWCOUNT %d", opts.ServerRowCap)); err != nil {
			return 0, fmt.Errorf("synapse set rowcount: %w", err)
		}
		defer func() { _, _ = conn.ExecContext(context.Background(), "SET ROWCOUNT 0") }()
		e.log.Info("Synapse JDBC stream server row cap enabled",
			zap.Int("spid", spid),
			zap.Int64("serverRowCap", opts.ServerRowCap))
	}

	e.log.Info("Synapse JDBC stream query started", zap.Int("spid", spid))

	rows, err := conn.QueryContext(streamCtx, query)
	if err != nil {
		return 0, fmt.Errorf("synapse query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if opts.OnColumns != nil {
		if err := opts.OnColumns(cols); err != nil {
			return 0, err
		}
	}
	dest := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	var count int64
	started := time.Now()
	lastProgress := started

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return count, fmt.Errorf("synapse scan: %w", err)
		}
		row := make([]interface{}, len(cols))
		for i, v := range dest {
			norm := normalizeCellValue(v)
			if err := checkCellSize(norm); err != nil {
				return count, err
			}
			row[i] = norm
		}
		count++
		if count == 1 {
			e.log.Info("Synapse JDBC stream first row received",
				zap.Int("spid", spid),
				zap.Duration("elapsed", time.Since(started)))
		}
		if opts.ServerRowCap == 0 && opts.MaxRows > 0 && count > opts.MaxRows {
			return count, fmt.Errorf("export row cap exceeded (%d)", opts.MaxRows)
		}
		if err := fn(row); err != nil {
			return count, err
		}
		now := time.Now()
		if opts.ProgressEveryRows > 0 && count%opts.ProgressEveryRows == 0 {
			e.log.Info("Synapse JDBC stream batch progress",
				zap.Int("spid", spid),
				zap.Int64("rowsStreamed", count),
				zap.Int64("batchSize", opts.ProgressEveryRows),
				zap.Duration("elapsed", now.Sub(started)))
			lastProgress = now
		} else if now.Sub(lastProgress) >= opts.ProgressEvery {
			e.log.Info("Synapse JDBC stream in progress",
				zap.Int("spid", spid),
				zap.Int64("rowsStreamed", count),
				zap.Duration("elapsed", now.Sub(started)))
			lastProgress = now
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("synapse rows: %w", err)
	}
	e.log.Info("Synapse JDBC stream completed",
		zap.Int("spid", spid),
		zap.Int64("rowsStreamed", count),
		zap.Duration("elapsed", time.Since(started)))
	return count, nil
}
