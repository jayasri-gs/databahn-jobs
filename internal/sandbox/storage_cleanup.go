package sandbox

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/objectstore"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/dataplane"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const defaultSandboxCleanupRetentionHours = 4

// sandboxCleanupDeleteAllEnv is the env var name; when set to true, the job deletes every object
// under each valid hour-partition path (year=/month=/day=/hour=) within tenant/status prefixes,
// ignoring retention. Intended for one-time cleanup after layout or format migrations (e.g. JSON → Parquet).
const sandboxCleanupDeleteAllEnv = "SANDBOX_CLEANUP_DELETE_ALL"

var sandboxStatuses = []string{"published", "dropped"}

type hourFolderInfo struct {
	folderTime time.Time
	keys       []string
}

// CleanupSandboxStorage removes old data from sandbox storage across all data planes.
// Supports both S3 and Azure Blob backends based on per-dataplane configuration.
func CleanupSandboxStorage(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	dataplaneConfigs, err := loadDataPlaneSandboxConfigs(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error loading data plane sandbox configurations: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error loading data plane sandbox configurations", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	if len(dataplaneConfigs) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no data planes with sandbox storage configuration found")
		return common.NewJobResultSuccess()
	}

	tenantIDs, err := loadTenantIDs(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error loading tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error loading tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	deleteAll := utils.GetEnvBool(sandboxCleanupDeleteAllEnv, false)
	retentionHours := utils.GetEnvInt("SANDBOX_CLEANUP_RETENTION_HOURS", defaultSandboxCleanupRetentionHours)
	now := time.Now().UTC()
	cutoffTime := now.Add(-time.Duration(retentionHours) * time.Hour).Truncate(time.Hour)

	log := logger.GetLoggerWithContext(ctx)
	if deleteAll {
		log.Warn("sandbox storage cleanup running in delete-all mode; all objects under hour-partition paths (year=/month=/day=/hour=) will be removed for each tenant/status prefix",
			zap.String("env_flag", sandboxCleanupDeleteAllEnv),
			zap.Bool("value", true))
	}

	log.Info("starting sandbox storage cleanup",
		zap.Int("dataplane_count", len(dataplaneConfigs)),
		zap.Int("tenant_count", len(tenantIDs)),
		zap.Time("current_time_utc", now),
		zap.Time("cutoff_time_utc", cutoffTime),
		zap.Int("retention_hours", retentionHours),
		zap.Bool("delete_all_objects", deleteAll))

	totalDeleted := 0
	for dataplaneID, sandboxConfig := range dataplaneConfigs {
		deleted, errs := cleanupDataPlane(ctx, dataplaneID, sandboxConfig, tenantIDs, cutoffTime, deleteAll)
		totalDeleted += deleted
		jobErrors = append(jobErrors, errs...)
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed sandbox storage cleanup",
			zap.Int("total_objects_deleted", totalDeleted),
			zap.Int("dataplanes_processed", len(dataplaneConfigs)))
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("sandbox storage cleanup completed with errors",
		zap.Int("error_count", len(jobErrors)),
		zap.Int("total_objects_deleted", totalDeleted),
		zap.Int("dataplanes_processed", len(dataplaneConfigs)))
	return common.NewJobResultFromErrors(jobErrors)
}

func loadTenantIDs(ctx context.Context) ([]string, error) {
	tenants, err := tenant.GetTenants(ctx, config.GetDB())
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(tenants))
	for i, t := range tenants {
		ids[i] = t.Id.String()
	}
	return ids, nil
}

// cleanupDataPlane processes cleanup for a single data plane across all tenants and statuses.
func cleanupDataPlane(ctx context.Context, dataplaneID uuid.UUID, sandboxConfig *dataplane.SandboxConfig, tenantIDs []string, cutoffTime time.Time, deleteAll bool) (int, []common.JobError) {
	var jobErrors []common.JobError
	collectionName := sandboxConfig.CollectionName()

	logger.GetLoggerWithContext(ctx).Info("processing sandbox storage cleanup for data plane",
		zap.String("dataplane_id", dataplaneID.String()),
		zap.String("collection", collectionName),
		zap.String("backend", sandboxConfig.Backend))

	store, err := createObjectStore(ctx, sandboxConfig)
	if err != nil {
		errorMsg := fmt.Sprintf("error creating object store for data plane %s: %v", dataplaneID.String(), err)
		logger.GetLoggerWithContext(ctx).Error("error creating object store",
			zap.String("dataplane_id", dataplaneID.String()),
			zap.Error(err))
		return 0, []common.JobError{{Message: errorMsg}}
	}

	dataplaneDeleted := 0
	for _, tenantID := range tenantIDs {
		for _, status := range sandboxStatuses {
			prefix := fmt.Sprintf("databahn-sandbox/status=%s/tenant_id=%s/", status, tenantID)
			deleted, err := cleanupPrefix(ctx, store, collectionName, prefix, cutoffTime, deleteAll)
			if err != nil {
				errorMsg := fmt.Sprintf("error cleaning up prefix %s in collection %s for data plane %s: %v",
					prefix, collectionName, dataplaneID.String(), err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("error cleaning up prefix",
					zap.String("dataplane_id", dataplaneID.String()),
					zap.String("tenant_id", tenantID),
					zap.String("collection", collectionName),
					zap.String("prefix", prefix),
					zap.Error(err))
				continue
			}
			dataplaneDeleted += deleted
		}
	}

	logger.GetLoggerWithContext(ctx).Info("completed cleanup for data plane",
		zap.String("dataplane_id", dataplaneID.String()),
		zap.String("collection", collectionName),
		zap.Int("objects_deleted", dataplaneDeleted))

	return dataplaneDeleted, jobErrors
}

// loadDataPlaneSandboxConfigs retrieves all data planes and builds a map of their sandbox storage configurations.
func loadDataPlaneSandboxConfigs(ctx context.Context) (map[uuid.UUID]*dataplane.SandboxConfig, error) {
	db := config.GetDB()
	dataPlanes, err := dataplane.GetAllDataPlanes(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error getting data planes: %w", err)
	}

	dataplaneConfigs := make(map[uuid.UUID]*dataplane.SandboxConfig)
	for _, dp := range dataPlanes {
		cfg, err := dp.ParseBackupConfiguration()
		if err != nil {
			logger.GetLoggerWithContext(ctx).Warn("error parsing backup configuration for data plane",
				zap.String("dataplane_id", dp.ID.String()),
				zap.String("dataplane_name", dp.Name),
				zap.Error(err))
			continue
		}

		if cfg == nil {
			logger.GetLoggerWithContext(ctx).Debug("no backup configuration found for data plane",
				zap.String("dataplane_id", dp.ID.String()),
				zap.String("dataplane_name", dp.Name))
			continue
		}

		if !cfg.SandboxConfiguration.Enabled {
			logger.GetLoggerWithContext(ctx).Debug("sandbox storage cleanup is disabled for data plane",
				zap.String("dataplane_id", dp.ID.String()),
				zap.String("dataplane_name", dp.Name))
			continue
		}

		sc := &cfg.SandboxConfiguration
		collectionName := sc.CollectionName()
		if collectionName == "" {
			logger.GetLoggerWithContext(ctx).Debug("no storage collection configured for data plane",
				zap.String("dataplane_id", dp.ID.String()),
				zap.String("dataplane_name", dp.Name))
			continue
		}

		dataplaneConfigs[dp.ID] = sc
		logger.GetLoggerWithContext(ctx).Debug("loaded sandbox storage configuration for data plane",
			zap.String("dataplane_id", dp.ID.String()),
			zap.String("dataplane_name", dp.Name),
			zap.String("backend", sc.Backend),
			zap.String("collection", collectionName))
	}

	return dataplaneConfigs, nil
}

// createObjectStore builds the appropriate ObjectStore backend from sandbox config.
func createObjectStore(ctx context.Context, cfg *dataplane.SandboxConfig) (objectstore.ObjectStore, error) {
	if cfg.IsBlobBackend() {
		accountName := cfg.AzureConfiguration.AccountName
		if accountName == "" {
			return nil, fmt.Errorf("azure account name is required for blob backend")
		}
		return objectstore.NewBlobBackend(ctx, "", accountName, "")
	}

	region := cfg.AWSConfiguration.Region
	if region == "" {
		region = "us-east-1"
	}
	return objectstore.NewS3Backend(ctx, region, "", "", "", false)
}

// cleanupPrefix lists objects under the prefix, groups them by hour folder
// (paths containing .../year=.../month=.../day=.../hour=.../), and deletes objects in those folders.
// When deleteAll is false, only hour folders older than cutoffTime are removed.
// When deleteAll is true, every object under matching hour folders is removed (retention ignored);
// keys that do not match that layout are left unchanged.
func cleanupPrefix(ctx context.Context, store objectstore.ObjectStore, collection, prefix string, cutoffTime time.Time, deleteAll bool) (int, error) {
	objects, err := store.List(ctx, collection, prefix)
	if err != nil {
		return 0, fmt.Errorf("error listing objects: %w", err)
	}

	hourFolders := groupObjectsByHourFolder(ctx, objects, cutoffTime, deleteAll)

	foldersToDelete := 0
	for _, info := range hourFolders {
		if info != nil {
			foldersToDelete++
		}
	}

	logger.GetLoggerWithContext(ctx).Info("identified hour folders to delete",
		zap.String("prefix", prefix),
		zap.Int("total_scanned", len(objects)),
		zap.Int("hour_folders_to_delete", foldersToDelete),
		zap.Bool("delete_all_hour_folders", deleteAll))

	totalDeleted := deleteExpiredHourFolders(ctx, store, collection, hourFolders)

	logger.GetLoggerWithContext(ctx).Info("completed deleting hour folders",
		zap.String("prefix", prefix),
		zap.Int("total_deleted", totalDeleted))

	return totalDeleted, nil
}

// groupObjectsByHourFolder groups object keys by their hour folder path.
// Returns a map where nil values indicate folders that should be skipped (unparseable or, in retention mode, not expired).
// deleteAllHourFolders: if true, every object under a valid hour-partition path is grouped for deletion (cutoff ignored).
func groupObjectsByHourFolder(ctx context.Context, objects []objectstore.ObjectInfo, cutoffTime time.Time, deleteAllHourFolders bool) map[string]*hourFolderInfo {
	hourFolders := make(map[string]*hourFolderInfo)

	for _, obj := range objects {
		hourFolder := extractHourFolder(obj.Key)
		if hourFolder == "" {
			continue
		}

		if info, exists := hourFolders[hourFolder]; exists {
			if info != nil {
				info.keys = append(info.keys, obj.Key)
			}
			continue
		}

		folderTime, err := parseHourFolderPath(hourFolder)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Debug("skipping object with unparseable path",
				zap.String("key", obj.Key),
				zap.Error(err))
			hourFolders[hourFolder] = nil
			continue
		}

		if deleteAllHourFolders || folderTime.Before(cutoffTime) {
			hourFolders[hourFolder] = &hourFolderInfo{
				folderTime: folderTime,
				keys:       []string{obj.Key},
			}
		} else {
			hourFolders[hourFolder] = nil
		}
	}

	return hourFolders
}

// deleteExpiredHourFolders deletes all objects in the given expired hour folders using batch delete.
func deleteExpiredHourFolders(ctx context.Context, store objectstore.ObjectStore, collection string, hourFolders map[string]*hourFolderInfo) int {
	totalDeleted := 0

	for hourFolder, info := range hourFolders {
		if info == nil {
			continue
		}
		if err := store.DeleteBatch(ctx, collection, info.keys); err != nil {
			logger.GetLoggerWithContext(ctx).Error("error batch deleting hour folder",
				zap.String("hour_folder", hourFolder),
				zap.Int("key_count", len(info.keys)),
				zap.Error(err))
			continue
		}
		totalDeleted += len(info.keys)
		logger.GetLoggerWithContext(ctx).Info("deleted hour folder",
			zap.String("hour_folder", hourFolder),
			zap.Time("folder_time", info.folderTime),
			zap.Int("objects_deleted", len(info.keys)))
	}

	return totalDeleted
}

// extractHourFolder extracts the hour folder path from an object key.
// Example: tenant_id=xxx/source_id=yyy/year=2025/month=12/day=17/hour=11/part-000.parquet
// Returns: tenant_id=xxx/source_id=yyy/year=2025/month=12/day=17/hour=11/
// The trailing slash prevents prefix matching issues (hour=1 matching hour=10..hour=19).
func extractHourFolder(key string) string {
	hourIdx := strings.LastIndex(key, "/hour=")
	if hourIdx == -1 {
		return ""
	}

	remaining := key[hourIdx+1:]
	nextSlash := strings.Index(remaining, "/")
	if nextSlash == -1 {
		return key + "/"
	}

	return key[:hourIdx+1+nextSlash] + "/"
}

// parseHourFolderPath parses the folder path to extract timestamp in UTC.
// Folder structure: .../year=2025/month=12/day=17/hour=11/...
func parseHourFolderPath(folderPath string) (time.Time, error) {
	pattern := regexp.MustCompile(`year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour=(\d{1,2})`)
	matches := pattern.FindStringSubmatch(folderPath)

	if len(matches) != 5 {
		return time.Time{}, fmt.Errorf("invalid folder path format: %s", folderPath)
	}

	year, err := strconv.Atoi(matches[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid year value in folder path %s: %w", folderPath, err)
	}

	month, err := strconv.Atoi(matches[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month value in folder path %s: %w", folderPath, err)
	}

	day, err := strconv.Atoi(matches[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day value in folder path %s: %w", folderPath, err)
	}

	hour, err := strconv.Atoi(matches[4])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid hour value in folder path %s: %w", folderPath, err)
	}

	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), nil
}
