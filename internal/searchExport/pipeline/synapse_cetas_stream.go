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

func (p *Pipeline) runSynapseCETASExport(
	ctx context.Context,
	destBucket string,
	cetasExec query.CETASExecutor,
	stagingCfg *destination.AzureBlobConfig,
) (*PipelineResult, error) {
	p.log.Info("Running Synapse CETAS export")

	exportFormat := normalizedFormat(p.request.Format)

	// Derive Synapse CREDENTIAL parameters from blob config (SAS token or SP identity).
	creds, err := query.StagingCredsFromBlobConfig(stagingCfg)
	if err != nil {
		return nil, fmt.Errorf("CETAS staging credentials: %w", err)
	}

	// DDL object names are unique per report (8 hex chars, no hyphens).
	shortID := strings.ReplaceAll(p.reportID, "-", "")
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	credName := "DatabahnStagingCred_" + shortID
	dsName := "DatabahnStagingDS_" + shortID
	fmtName := query.CETASFileFormatName(exportFormat, shortID)

	// Create staging credential (drop first for idempotency on retry).
	for _, ddl := range []string{
		query.CETASCredentialDropIfExistsDDL(credName),
		query.CETASCredentialDDL(credName, creds.Identity, creds.Secret),
	} {
		if err := cetasExec.ExecDDL(ctx, ddl); err != nil {
			return nil, fmt.Errorf("CETAS credential: %w", err)
		}
	}
	defer cetasExec.ExecDDL(context.Background(), query.CETASCredentialDropIfExistsDDL(credName)) //nolint:errcheck

	// Create staging data source.
	for _, ddl := range []string{
		query.CETASDataSourceDropIfExistsDDL(dsName),
		query.CETASDataSourceDDL(dsName, credName, creds.DataSrcURL),
	} {
		if err := cetasExec.ExecDDL(ctx, ddl); err != nil {
			return nil, fmt.Errorf("CETAS data source: %w", err)
		}
	}
	defer cetasExec.ExecDDL(context.Background(), query.CETASDataSourceDropIfExistsDDL(dsName)) //nolint:errcheck

	// Create file format (drop-if-exists first for idempotency on retry).
	for _, ddl := range []string{
		query.CETASFileFormatDropIfExistsDDL(fmtName),
		query.CETASFileFormatDDL(fmtName, exportFormat, p.request.Delimiter),
	} {
		if err := cetasExec.ExecDDL(ctx, ddl); err != nil {
			return nil, fmt.Errorf("CETAS file format: %w", err)
		}
	}
	defer cetasExec.ExecDDL(context.Background(), query.CETASFileFormatDropDDL(fmtName)) //nolint:errcheck

	// Plan hour chunks — each gets one CETAS DDL with equality partition filter.
	partCols := query.PartitionColumnsForStoreType(p.request.DataStoreType)
	hours := query.PlanHourChunks(p.request.StartTime, p.request.EndTime)
	if len(hours) == 0 {
		return nil, fmt.Errorf("no hour chunks for time range %d-%d", p.request.StartTime, p.request.EndTime)
	}
	p.log.Info("CETAS export hour plan",
		zap.Int("totalHours", len(hours)),
		zap.Int64("rangeStartMs", p.request.StartTime),
		zap.Int64("rangeEndMs", p.request.EndTime))

	// Create staging blob reader.
	blobClient, err := destination.NewAzureBlobClient(stagingCfg)
	if err != nil {
		return nil, fmt.Errorf("CETAS staging blob client: %w", err)
	}
	stagingReader, err := unload.NewBlobReader(blobClient, stagingCfg.Container, p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("CETAS staging reader: %w", err)
	}

	// Execute CETAS per hour; collect all staging file paths before uploading.
	var allFiles []string
	var tableNames []string
	for hi, hour := range hours {
		tableName := query.CETASTableName(shortID, hi)
		tableNames = append(tableNames, tableName)
		stagingPrefix := query.CETASStagingPrefix(p.reportID, hi)

		hourFilter := query.HourChunkFilter(hour.StartMs, p.request.StartTime, p.request.EndTime, partCols)
		hourSQL := query.WrapWithPartitionFilter(p.request.Query, hourFilter)

		p.log.Info("CETAS export hour",
			zap.Int("hourIndex", hi+1),
			zap.Int("totalHours", len(hours)),
			zap.String("filter", hourFilter))

		// Drop any leftover table from a previous partial run.
		_ = cetasExec.ExecDDL(ctx, query.CETASTableDropIfExistsDDL(tableName))
		if err := cetasExec.ExecDDL(ctx, query.CETASTableDDL(tableName, dsName, stagingPrefix, fmtName, hourSQL)); err != nil {
			return nil, fmt.Errorf("CETAS hour %d: %w", hi, err)
		}

		files, err := stagingReader.ListFiles(ctx, stagingPrefix)
		if err != nil {
			return nil, fmt.Errorf("list staging files hour %d: %w", hi, err)
		}
		allFiles = append(allFiles, files...)
	}
	defer func() {
		if len(allFiles) > 0 {
			_ = stagingReader.DeleteFiles(context.Background(), allFiles)
		}
		for _, t := range tableNames {
			_ = cetasExec.ExecDDL(context.Background(), query.CETASTableDropDDL(t))
		}
	}()

	if len(allFiles) == 0 {
		p.cleanupCheckpoint()
		return &PipelineResult{TotalRows: 0, TotalBytes: 0}, nil
	}

	// Init multipart upload for final output file.
	ext, contentType := formatMeta(exportFormat)
	_, outputKey := p.buildExportObjectKey(ext)
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
			ctx, p.uploader, allFiles, header, exportFormat, delim, p.log, unload.StreamOptions{},
		)
	} else {
		// CETAS PARQUET output: decode Parquet files, encode to JSON/Excel.
		totalRows, totalBytes, err = p.processUnloadToFinal(ctx, stagingReader, allFiles)
	}
	if err != nil {
		_ = p.uploader.Abort(ctx)
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("CETAS upload: %w", err)
	}

	if totalBytes == 0 {
		p.cleanupCheckpoint()
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
	}

	presignedURL, _ := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	p.cleanupCheckpoint()
	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}
