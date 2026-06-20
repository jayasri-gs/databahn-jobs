package agent

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/objstore"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// FetchAgentDiscoveredChannelSubscriptions fetches and processes agent discovered channel subscriptions
func FetchAgentDiscoveredChannelSubscriptions(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	logger.GetLoggerWithContext(ctx).Info("starting agent discovered channel subscriptions fetch")

	// Get database connection

	db := config.GetDB()
	if db == nil {
		err := fmt.Errorf("failed to get database connection")
		logger.GetLoggerWithContext(ctx).Error("database connection error", zap.Error(err))
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("DATABASE_CONNECTION_ERROR: %s", err.Error())})
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Fetch tenant WEC certificates from database
	var tenantCerts []TenantWecCertificate
	if err := db.WithContext(ctx).Find(&tenantCerts).Error; err != nil {
		logger.GetLoggerWithContext(ctx).Error("failed to fetch tenant WEC certificates", zap.Error(err))
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("DATABASE_QUERY_ERROR: %s", err.Error())})
		return common.NewJobResultFromErrors(jobErrors)
	}

	logger.GetLoggerWithContext(ctx).Info("fetched tenant certificates", zap.Int("count", len(tenantCerts)))

	// Check if no tenants are configured
	if len(tenantCerts) == 0 {
		logger.GetLoggerWithContext(ctx).Warn("no tenant WEC certificates found in database - no processing will occur")
		return common.NewJobResultSuccess() // Still success, but with clear logging
	}

	// Process each tenant
	for _, cert := range tenantCerts {
		if cert.SubscriptionStatusS3Path == "" {
			logger.GetLoggerWithContext(ctx).Warn("empty subscription status S3 path for tenant",
				zap.String("tenant_id", cert.TenantID))
			continue
		}

		err := processSubscriptionStatusForTenant(ctx, cert.TenantID, cert.SubscriptionStatusS3Path)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("failed to process tenant subscription status",
				zap.String("tenant_id", cert.TenantID),
				zap.String("s3_path", cert.SubscriptionStatusS3Path),
				zap.Error(err))
			jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("TENANT_PROCESSING_ERROR: tenant %s: %s", cert.TenantID, err.Error())})
		}
	}

	logger.GetLoggerWithContext(ctx).Info("completed agent discovered channel subscriptions fetch",
		zap.Int("total_tenants", len(tenantCerts)),
		zap.Int("errors", len(jobErrors)))

	if len(jobErrors) > 0 {
		return common.NewJobResultFromErrors(jobErrors)
	}
	return common.NewJobResultSuccess()
}

// processSubscriptionStatusForTenant processes the subscription status for a specific tenant
func processSubscriptionStatusForTenant(ctx context.Context, tenantID, s3Path string) error {
	logger.GetLoggerWithContext(ctx).Info("processing subscription status for tenant",
		zap.String("tenant_id", tenantID),
		zap.String("s3_path", s3Path))

	bucketName := objstore.GetBucket(objstore.BucketArtifacts)
	if bucketName == "" {
		return fmt.Errorf("artifacts bucket not configured")
	}

	dataMap, err := parseCSVFromObjectStore(ctx, bucketName, s3Path)
	if err != nil {
		return fmt.Errorf("failed to parse CSV from object store: %w", err)
	}

	if dataMap == nil {
		logger.GetLoggerWithContext(ctx).Info("no CSV data to process - skipping subscription update",
			zap.String("tenant_id", tenantID))
		return nil
	}

	return updateSubscription(ctx, dataMap, tenantID)
}

// parseCSVFromObjectStore retrieves and parses the most recent CSV file from the object store
func parseCSVFromObjectStore(ctx context.Context, bucket, prefix string) (map[string]map[string]map[string]string, error) {
	objects, err := objstore.GetClient().List(ctx, bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("unable to list items in bucket %q with prefix %q: %w", bucket, prefix, err)
	}

	if len(objects) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no CSV files found - customer may not be onboarded yet",
			zap.String("bucket", bucket),
			zap.String("prefix", prefix))
		return nil, nil
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].LastModified.After(objects[j].LastModified)
	})

	mostRecentKey := objects[0].Key
	logger.GetLoggerWithContext(ctx).Info("processing most recent CSV file",
		zap.String("key", mostRecentKey),
		zap.Time("last_modified", objects[0].LastModified))

	data, err := objstore.GetClient().Get(ctx, bucket, mostRecentKey)
	if err != nil {
		return nil, fmt.Errorf("unable to get object %q from bucket %q: %w", mostRecentKey, bucket, err)
	}

	dataMap := make(map[string]map[string]map[string]string)
	r := csv.NewReader(bytes.NewReader(data))

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV data: %w", err)
	}

	// CSV structure: "Computer Name","LogName","ChannelAccess","Data"
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) == 4 {
			computerName := record[0]
			logName := record[1]
			channelAccess := record[2]
			csvData := record[3]

			if _, ok := dataMap[computerName]; !ok {
				dataMap[computerName] = make(map[string]map[string]string)
			}
			if _, ok := dataMap[computerName][logName]; !ok {
				dataMap[computerName][logName] = make(map[string]string)
			}
			dataMap[computerName][logName]["ChannelAccess"] = channelAccess
			dataMap[computerName][logName]["Data"] = csvData
		}
	}

	logger.GetLoggerWithContext(ctx).Info("parsed CSV data",
		zap.Int("total_records", len(records)-1),
		zap.Int("data_entries", len(dataMap)))

	return dataMap, nil
}

// updateSubscription updates subscription data in agent_discovered_nodes table
func updateSubscription(ctx context.Context, dataMap map[string]map[string]map[string]string, tenantID string) error {
	logger.GetLoggerWithContext(ctx).Info("updating subscription data",
		zap.String("tenant_id", tenantID),
		zap.Int("data_entries", len(dataMap)))

	db := config.GetDB()
	if db == nil {
		return fmt.Errorf("failed to get database connection")
	}

	// Process each computer (node) in the data
	for computerName, logEntries := range dataMap {
		logger.GetLoggerWithContext(ctx).Debug("processing computer",
			zap.String("computer_name", computerName),
			zap.String("tenant_id", tenantID),
			zap.Int("log_entries", len(logEntries)))

		// Convert log entries to JSON for storage in channel_subscriptions column
		channelSubscriptionsJSON, err := convertLogEntriesToJSON(logEntries)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("failed to convert log entries to JSON",
				zap.String("computer_name", computerName),
				zap.Error(err))
			return fmt.Errorf("failed to convert log entries to JSON for %s: %w", computerName, err)
		}

		// Update agent_discovered_nodes where node_name matches computerName
		result := db.WithContext(ctx).
			Table("agent_discovered_nodes").
			Where("tenant_id = ? AND node_name = ?", tenantID, computerName).
			Updates(map[string]interface{}{
				"channel_subscriptions": channelSubscriptionsJSON,
				"updated_at":            gorm.Expr("NOW()"),
			})

		if result.Error != nil {
			logger.GetLoggerWithContext(ctx).Error("failed to update agent discovered node",
				zap.String("tenant_id", tenantID),
				zap.String("node_name", computerName),
				zap.Error(result.Error))
			return fmt.Errorf("failed to update agent discovered node for %s: %w", computerName, result.Error)
		}

		if result.RowsAffected == 0 {
			logger.GetLoggerWithContext(ctx).Warn("no rows updated for node",
				zap.String("tenant_id", tenantID),
				zap.String("node_name", computerName))
		} else {
			logger.GetLoggerWithContext(ctx).Debug("updated agent discovered node",
				zap.String("tenant_id", tenantID),
				zap.String("node_name", computerName),
				zap.Int64("rows_affected", result.RowsAffected))
		}
	}

	logger.GetLoggerWithContext(ctx).Info("completed updating subscription data",
		zap.String("tenant_id", tenantID))

	return nil
}

// convertLogEntriesToJSON converts log entries map to JSON string for storage
func convertLogEntriesToJSON(logEntries map[string]map[string]string) (string, error) {
	// Create a structured format for the JSON
	type LogEntry struct {
		LogName       string `json:"log_name"`
		ChannelAccess string `json:"channel_access"`
		Data          string `json:"data"`
	}

	var entries []LogEntry
	for logName, details := range logEntries {
		entries = append(entries, LogEntry{
			LogName:       logName,
			ChannelAccess: details["ChannelAccess"],
			Data:          details["Data"],
		})
	}

	jsonBytes, err := json.Marshal(entries)
	if err != nil {
		return "", fmt.Errorf("failed to marshal log entries to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// TenantWecCertificate represents the tenant_wec_certificates table structure
type TenantWecCertificate struct {
	TenantID                 string `json:"tenant_id" gorm:"column:tenant_id;primaryKey;type:uuid"`
	CNName                   string `json:"cn_name" gorm:"column:cn_name;type:varchar(255)"`
	CertificateBundleS3URI   string `json:"certificate_bundle_s3_uri" gorm:"column:certificate_bundle_s3_uri;type:varchar(1024)"`
	SubscriptionStatusS3Path string `json:"subscription_status_s3_path" gorm:"column:subscription_status_s3_path;type:varchar(1024)"`
	TagID                    string `json:"tag_id" gorm:"column:tag_id;type:uuid;not null"`
}

// TableName specifies the table name for GORM
func (TenantWecCertificate) TableName() string {
	return "tenant_wec_certificates"
}
