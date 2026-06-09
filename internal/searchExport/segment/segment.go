package segment

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type Manager struct {
	jobID       string
	tempDir     string
	maxSegBytes int64
	mu          sync.Mutex
	segments    []string
	segCount    int
}

func NewManager(jobID, tempDir string, maxSegmentSizeMB int) (*Manager, error) {
	dir := filepath.Join(tempDir, jobID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	maxBytes := int64(maxSegmentSizeMB) * 1024 * 1024
	if maxBytes < 5*1024*1024 {
		maxBytes = 5 * 1024 * 1024
	}

	return &Manager{
		jobID:       jobID,
		tempDir:     dir,
		maxSegBytes: maxBytes,
		segments:    make([]string, 0),
	}, nil
}

func (m *Manager) TempDir() string {
	return m.tempDir
}

func (m *Manager) NewSegmentWriter(columns []string) (*SegmentWriter, error) {
	m.mu.Lock()
	m.segCount++
	segNum := m.segCount
	m.mu.Unlock()

	segName := fmt.Sprintf("seg_%05d", segNum)
	segPath := filepath.Join(m.tempDir, segName+".csv")

	file, err := os.Create(segPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	writer := csv.NewWriter(file)
	if err := writer.Write(columns); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	m.mu.Lock()
	m.segments = append(m.segments, segName)
	m.mu.Unlock()

	logging.GetLogger().Debug("Created segment", zap.String("segment", segName))

	return &SegmentWriter{
		manager:  m,
		name:     segName,
		path:     segPath,
		file:     file,
		writer:   writer,
		maxBytes: m.maxSegBytes,
	}, nil
}

func (m *Manager) GetSegmentsInOrder() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]string, len(m.segments))
	copy(result, m.segments)
	sort.Strings(result)
	return result
}

func (m *Manager) GetSegmentPath(segName string) string {
	return filepath.Join(m.tempDir, segName+".csv")
}

func (m *Manager) Cleanup() error {
	return os.RemoveAll(m.tempDir)
}

type SegmentWriter struct {
	manager  *Manager
	name     string
	path     string
	file     *os.File
	writer   *csv.Writer
	maxBytes int64
	rowCount int64
	bytes    int64
}

func (w *SegmentWriter) Name() string {
	return w.name
}

func (w *SegmentWriter) Bytes() int64 {
	return w.bytes
}

func (w *SegmentWriter) IsFull() bool {
	return w.bytes >= w.maxBytes
}

func (w *SegmentWriter) WriteRow(row []string) error {
	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.rowCount++
	for _, v := range row {
		w.bytes += int64(len(v))
	}
	return nil
}

func (w *SegmentWriter) Close() error {
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		w.file.Close()
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	logging.GetLogger().Debug("Closed segment",
		zap.String("segment", w.name),
		zap.Int64("rows", w.rowCount),
		zap.Int64("bytes", w.bytes))

	return nil
}
