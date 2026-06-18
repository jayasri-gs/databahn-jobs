package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	StageQuerying  = "querying"
	StageStreaming = "streaming"
	StageUploading = "uploading"
)

type Checkpoint struct {
	Stage                string    `json:"stage"`
	AthenaExecutionID    string    `json:"athena_execution_id"`
	QueryExecutionID     string    `json:"query_execution_id"`
	UploadID             string    `json:"upload_id"`
	Bucket               string    `json:"bucket"`
	Key                  string    `json:"key"`
	UnloadFiles          []string  `json:"unload_files"`
	ManifestLocation     string    `json:"manifest_location"`
	ProcessedFileIndex   int       `json:"processed_file_index"`
	LastUploadedPart     int       `json:"last_uploaded_part"`
	RowsProcessed        int64     `json:"rows_processed"`
	BytesProcessed       int64     `json:"bytes_processed"`
	TempOutputPath       string    `json:"temp_output_path"`
	SynapseHourIndex     int       `json:"synapse_hour_index,omitempty"`
	SynapseSubChunkIndex int       `json:"synapse_sub_chunk_index,omitempty"`
	SynapseLastSortKey   string    `json:"synapse_last_sort_key,omitempty"`
	TotalHourChunks      int       `json:"total_hour_chunks,omitempty"`
	UploadBlockIDs       []string  `json:"upload_block_ids,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func dir(mountPath, reportID string) string {
	return filepath.Join(mountPath, reportID)
}

func filePath(mountPath, reportID string) string {
	return filepath.Join(dir(mountPath, reportID), "checkpoint.json")
}

func Write(mountPath, reportID string, cp Checkpoint) error {
	cp.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	d := dir(mountPath, reportID)
	if err := os.MkdirAll(d, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmpFile, err := os.CreateTemp(d, "checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create checkpoint tmp: %w", err)
	}
	tmp := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmp)
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmp, filePath(mountPath, reportID)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
}

func Read(mountPath, reportID string) (*Checkpoint, error) {
	data, err := os.ReadFile(filePath(mountPath, reportID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	return &cp, nil
}

func Delete(mountPath, reportID string) error {
	path := filePath(mountPath, reportID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete checkpoint file: %w", err)
	}
	_ = os.Remove(dir(mountPath, reportID))
	return nil
}
