package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"go.uber.org/zap"
)

// cetasShortID derives the 8-char DDL object suffix from a report ID.
func cetasShortID(reportID string) string {
	shortID := strings.ReplaceAll(reportID, "-", "")
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	return shortID
}

// cetasObjectNames returns the per-report credential and data source names.
func cetasObjectNames(shortID string) (credName, dsName string) {
	return "DatabahnStagingCred_" + shortID, "DatabahnStagingDS_" + shortID
}

// prepareCETASStaging reports whether the CETAS query must run. CETAS creates the
// external table only after the statement fully succeeds, so an existing table means
// the staged output is complete and reusable. When the table is missing, leftover
// blobs from a crashed attempt are deleted — CETAS requires an empty target prefix.
func prepareCETASStaging(ctx context.Context, cetasExec query.CETASExecutor, stagingReader unload.StagingReader, tableName, stagingPrefix string, log *zap.Logger) (bool, error) {
	exists, err := cetasExec.ExternalTableExists(ctx, tableName)
	if err != nil {
		return false, fmt.Errorf("check CETAS table: %w", err)
	}
	if exists {
		log.Info("CETAS table exists from previous attempt — reusing staged query output",
			zap.String("table", tableName))
		return false, nil
	}
	leftovers, err := stagingReader.ListFiles(ctx, stagingPrefix)
	if err != nil {
		return false, fmt.Errorf("list staging leftovers: %w", err)
	}
	if len(leftovers) > 0 {
		log.Info("Deleting leftover staged blobs from previous attempt",
			zap.Int("count", len(leftovers)),
			zap.String("prefix", stagingPrefix))
		if err := stagingReader.DeleteFiles(ctx, leftovers); err != nil {
			return false, fmt.Errorf("delete staging leftovers: %w", err)
		}
	}
	return true, nil
}

// CleanupCETASArtifacts best-effort drops the report's CETAS DDL objects and deletes
// staged blobs. Called after a successful upload and on permanent failure — never on
// a retryable failure, so the next attempt can reuse the completed table. Order
// matters: the external table must be dropped before the data source it references.
func CleanupCETASArtifacts(ctx context.Context, cetasExec query.CETASExecutor, stagingReader unload.StagingReader, reportID string, log *zap.Logger) {
	shortID := cetasShortID(reportID)
	credName, dsName := cetasObjectNames(shortID)
	for _, ddl := range []string{
		query.CETASTableDropIfExistsDDL(query.CETASTableName(shortID)),
		query.CETASDataSourceDropIfExistsDDL(dsName),
		query.CETASCredentialDropIfExistsDDL(credName),
		query.CETASFileFormatDropIfExistsDDL(query.CETASFileFormatName("csv", shortID)),
		query.CETASFileFormatDropIfExistsDDL(query.CETASFileFormatName("json", shortID)),
	} {
		if err := cetasExec.ExecDDL(ctx, ddl); err != nil {
			log.Warn("CETAS cleanup DDL failed", zap.Error(err))
		}
	}
	prefix := query.CETASStagingPrefix(reportID)
	if files, err := stagingReader.ListFiles(ctx, prefix); err == nil && len(files) > 0 {
		if err := stagingReader.DeleteFiles(ctx, files); err != nil {
			log.Warn("CETAS cleanup staging delete failed", zap.Error(err))
		}
	}
}

func (p *Pipeline) runSynapseCETASExport(
	ctx context.Context,
	destBucket string,
	cetasExec query.CETASExecutor,
	stagingCfg *destination.AzureBlobConfig,
) (*PipelineResult, error) {
	p.log.Info("Running Synapse CETAS export")

	exportFormat := normalizedFormat(p.request.Format)
	shortID := cetasShortID(p.reportID)
	credName, dsName := cetasObjectNames(shortID)
	fmtName := query.CETASFileFormatName(exportFormat, shortID)
	tableName := query.CETASTableName(shortID)
	stagingPrefix := query.CETASStagingPrefix(p.reportID)

	blobClient, err := destination.NewAzureBlobClient(stagingCfg)
	if err != nil {
		return nil, fmt.Errorf("CETAS staging blob client: %w", err)
	}
	stagingReader, err := unload.NewBlobReader(blobClient, stagingCfg.Container, p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("CETAS staging reader: %w", err)
	}

	runQuery, err := prepareCETASStaging(ctx, cetasExec, stagingReader, tableName, stagingPrefix, p.log)
	if err != nil {
		return nil, err
	}

	if runQuery {
		creds, err := query.StagingCredsFromBlobConfig(stagingCfg)
		if err != nil {
			return nil, fmt.Errorf("CETAS staging credentials: %w", err)
		}

		// Recreate DDL objects idempotently. The table is known not to exist here, so
		// the data source and credential are not referenced and can be dropped safely.
		for _, ddl := range []string{
			query.CETASCredentialDropIfExistsDDL(credName),
			query.CETASCredentialDDL(credName, creds.Identity, creds.Secret),
			query.CETASDataSourceDropIfExistsDDL(dsName),
			query.CETASDataSourceDDL(dsName, credName, creds.DataSrcURL),
			query.CETASFileFormatDropIfExistsDDL(fmtName),
			query.CETASFileFormatDDL(fmtName, exportFormat, p.request.Delimiter),
		} {
			if err := cetasExec.ExecDDL(ctx, ddl); err != nil {
				return nil, fmt.Errorf("CETAS staging DDL: %w", err)
			}
		}

		partCols := query.PartitionColumnsForStoreType(p.request.DataStoreType)
		rangeFilter := query.RangePartitionFilter(p.request.StartTime, p.request.EndTime, partCols)
		if rangeFilter == "" {
			return nil, fmt.Errorf("invalid export time range %d-%d", p.request.StartTime, p.request.EndTime)
		}
		fullSQL := query.WrapWithPartitionFilter(p.request.Query, rangeFilter)

		p.log.Info("CETAS export query started",
			zap.Int64("rangeStartMs", p.request.StartTime),
			zap.Int64("rangeEndMs", p.request.EndTime),
			zap.String("table", tableName))
		if err := cetasExec.ExecDDL(ctx, query.CETASTableDDL(tableName, dsName, stagingPrefix, fmtName, fullSQL)); err != nil {
			return nil, fmt.Errorf("CETAS: %w", err)
		}
	}

	allFiles, err := stagingReader.ListFiles(ctx, stagingPrefix)
	if err != nil {
		return nil, fmt.Errorf("list staging files: %w", err)
	}
	if len(allFiles) == 0 {
		CleanupCETASArtifacts(ctx, cetasExec, stagingReader, p.reportID, p.log)
		return &PipelineResult{TotalRows: 0, TotalBytes: 0}, nil
	}

	ext, contentType := formatMeta(exportFormat)
	_, outputKey := p.buildExportObjectKey(ext)
	p.abortOrphanedMultipartUploads(ctx, destBucket)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("init uploader: %w", err)
	}

	var totalRows, totalBytes int64
	if exportFormat == "csv" {
		// CETAS DELIMITEDTEXT output: raw bytes — stream directly, prepend header if requested.
		var header []byte
		if p.request.IncludeHeader {
			cols, colErr := cetasExec.GetQueryColumns(ctx, p.request.Query, p.request.Database)
			if colErr == nil && len(cols) > 0 {
				delim := p.request.Delimiter
				if delim == "" {
					delim = ","
				}
				header = unload.BuildCSVHeader(cols, delim)
			}
		}
		delim := p.request.Delimiter
		if delim == "" {
			delim = ","
		}
		totalRows, totalBytes, err = stagingReader.StreamToUploader(
			ctx, p.uploader, allFiles, header, exportFormat, delim, p.log,
		)
	} else {
		// CETAS PARQUET output: decode Parquet files, encode to JSON/Excel.
		totalRows, totalBytes, err = p.processUnloadToFinal(ctx, stagingReader, allFiles)
	}
	if err != nil {
		// Retryable failure: abort the destination upload but LEAVE the CETAS table and
		// staged blobs so the next attempt reuses the completed query output.
		_ = p.uploader.Abort(ctx)
		return nil, fmt.Errorf("CETAS upload: %w", err)
	}

	CleanupCETASArtifacts(ctx, cetasExec, stagingReader, p.reportID, p.log)

	if totalBytes == 0 {
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
	}

	presignedURL, _ := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}
