package pipeline

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/format"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/segment"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type PipelineConfig struct {
	TempDir          string
	MaxSegmentSizeMB int
	PresignExpiry    time.Duration
	LifecycleTag     string
}

type PipelineResult struct {
	TotalRows    int64
	TotalBytes   int64
	Location     string
	PresignedURL string
	Expiry       time.Time
}

type Pipeline struct {
	config   PipelineConfig
	request  *models.SearchExportConfig
	reportID string
	executor query.QueryExecutor
	uploader upload.CloudUploader
}

func New(cfg PipelineConfig, reportID string, req *models.SearchExportConfig, executor query.QueryExecutor) *Pipeline {
	return &Pipeline{
		config:   cfg,
		request:  req,
		reportID: reportID,
		executor: executor,
	}
}

func (p *Pipeline) Run(ctx context.Context, destBucket string) (*PipelineResult, error) {
	logging.GetLogger().Info("Starting export pipeline",
		zap.String("reportId", p.reportID),
		zap.String("format", p.request.Format))

	if err := p.executor.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.executor.Close()

	awsCfgInterface := p.executor.GetAWSConfig()
	awsCfg, ok := awsCfgInterface.(aws.Config)
	if !ok {
		return nil, fmt.Errorf("failed to get AWS config")
	}

	athenaOutputLoc := p.executor.GetOutputLocation()
	if athenaOutputLoc == "" {
		return nil, fmt.Errorf("athena output_location required for UNLOAD")
	}

	tempPath := fmt.Sprintf("unload_%s_%d/", p.reportID, time.Now().Unix())
	tempS3Path := trimSuffix(athenaOutputLoc, "/") + "/" + tempPath

	logging.GetLogger().Info("Executing UNLOAD",
		zap.String("outputPath", tempS3Path))

	unloadResult, err := p.executor.ExecuteUnload(ctx, p.request.Query, p.request.Database, tempS3Path)
	if err != nil {
		return nil, fmt.Errorf("UNLOAD failed: %w", err)
	}

	unloadReader := unload.NewReader(awsCfg, p.config.TempDir)

	var outputFiles []string
	if unloadResult.ManifestLocation != "" {
		outputFiles, err = unloadReader.ParseManifest(ctx, unloadResult.ManifestLocation)
	} else {
		outputFiles, err = unloadReader.ListFiles(ctx, tempS3Path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list UNLOAD output: %w", err)
	}

	if len(outputFiles) == 0 {
		logging.GetLogger().Warn("UNLOAD produced no files")
		return &PipelineResult{TotalRows: 0}, nil
	}

	ext := "csv"
	contentType := "text/csv"
	switch p.request.Format {
	case "json":
		ext = "json"
		contentType = "application/x-ndjson"
	case "xlsx", "excel":
		ext = "xlsx"
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	}

	outputKey := fmt.Sprintf("exports/%s/export.%s", p.reportID, ext)

	p.uploader = upload.NewS3Uploader(awsCfg, p.config.LifecycleTag)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	totalRows, totalBytes, err := p.processUnloadToFinal(ctx, unloadReader, outputFiles)
	if err != nil {
		p.uploader.Abort(ctx)
		unloadReader.DeleteS3Files(ctx, outputFiles)
		return nil, err
	}

	unloadReader.DeleteS3Files(ctx, outputFiles)
	if unloadResult.ManifestLocation != "" {
		unloadReader.DeleteS3Files(ctx, []string{unloadResult.ManifestLocation})
	}

	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		logging.GetLogger().Warn("Failed to generate presigned URL", zap.Error(err))
	}

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}

func (p *Pipeline) processUnloadToFinal(ctx context.Context, ur *unload.Reader, files []string) (int64, int64, error) {
	var totalRows int64
	var totalBytes int64
	var parts []upload.PartInfo
	partNum := 1

	maxSegSizeMB := p.config.MaxSegmentSizeMB
	if maxSegSizeMB == 0 {
		maxSegSizeMB = 10
	}

	segMgr, err := segment.NewManager(p.reportID+"_unload", p.config.TempDir, maxSegSizeMB)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create segment manager: %w", err)
	}
	defer segMgr.Cleanup()

	var currentSeg *segment.SegmentWriter
	var columns []string

	uploadSegment := func() error {
		if currentSeg == nil {
			return nil
		}

		segName := currentSeg.Name()
		if err := currentSeg.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}

		segPath := segMgr.GetSegmentPath(segName)
		segData, err := os.ReadFile(segPath)
		if err != nil {
			return fmt.Errorf("failed to read segment: %w", err)
		}

		if partNum > 1 && p.request.Format == "csv" {
			segData, err = readSegmentWithoutHeader(segPath)
			if err != nil {
				return fmt.Errorf("failed to strip header: %w", err)
			}
		}

		if len(segData) == 0 {
			currentSeg = nil
			return nil
		}

		size := int64(len(segData))
		part, err := p.uploader.UploadPart(ctx, partNum, bytes.NewReader(segData), size)
		if err != nil {
			return fmt.Errorf("failed to upload part %d: %w", partNum, err)
		}

		parts = append(parts, *part)
		totalBytes += size
		partNum++
		currentSeg = nil
		return nil
	}

	err = ur.StreamRows(ctx, files, func(row []interface{}) error {
		if columns == nil {
			columns = ur.Columns()
		}

		if currentSeg == nil {
			currentSeg, err = segMgr.NewSegmentWriter(columns)
			if err != nil {
				return fmt.Errorf("failed to create segment: %w", err)
			}
		}

		strRow := make([]string, len(row))
		for i, v := range row {
			strRow[i] = format.FormatValue(v)
		}

		if err := currentSeg.WriteRow(strRow); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		totalRows++

		if currentSeg.IsFull() {
			if err := uploadSegment(); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return totalRows, 0, fmt.Errorf("failed to process Parquet: %w", err)
	}

	if currentSeg != nil {
		if err := uploadSegment(); err != nil {
			return totalRows, 0, err
		}
	}

	if len(parts) == 0 {
		return 0, 0, nil
	}

	if err := p.uploader.Complete(ctx, parts); err != nil {
		return totalRows, 0, fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	logging.GetLogger().Info("Export completed",
		zap.Int64("totalRows", totalRows),
		zap.Int64("totalBytes", totalBytes))

	return totalRows, totalBytes, nil
}

func readSegmentWithoutHeader(segPath string) ([]byte, error) {
	file, err := os.Open(segPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return []byte{}, nil
		}
		return nil, err
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}
