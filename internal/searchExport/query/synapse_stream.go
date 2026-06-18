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

	rows, err := conn.QueryContext(streamCtx, query)
	if err != nil {
		return 0, fmt.Errorf("synapse query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
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
		if opts.MaxRows > 0 && count > opts.MaxRows {
			return count, fmt.Errorf("export row cap exceeded (%d)", opts.MaxRows)
		}
		if err := fn(row); err != nil {
			return count, err
		}
		now := time.Now()
		if now.Sub(lastProgress) >= opts.ProgressEvery {
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
	return count, nil
}
