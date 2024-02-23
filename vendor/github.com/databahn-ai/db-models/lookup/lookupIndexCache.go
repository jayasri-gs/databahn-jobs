package lookup

import (
	"github.com/google/uuid"
	"time"
)

type LookupIndexCacheRequest struct {
	Id            uuid.UUID `json:"id"`
	LookupId      uuid.UUID `json:"lookup_id"`
	Lookup        Lookup    `gorm:"foreignKey:id;references:lookup_id"`
	FilePath      string    `json:"file_path"`
	FileName      string    `json:"file_name"`
	RequestStatus string    `json:"request_status"`
	RequestAction string    `json:"request_action"`
	IndexName     string    `json:"index_name"`
	Operation     string    `json:"operation"`
	Type          string    `json:"type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
