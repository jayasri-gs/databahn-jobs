package jobs

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	defaultSandboxCleanupRetentionHours = 3
	batchDeleteSize                     = 1000 // S3 DeleteObjects supports up to 1000 keys per request
)

// CleanupSandboxStorage removes old data from sandbox storage S3 bucket
func CleanupSandboxStorage(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	bucketName := config.GetAppConfiguration().GetString(configuration.BackupEventsS3Bucket)
	if bucketName == "" {
		errorMsg := "backup events S3 bucket not configured"
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error(errorMsg)
		return common.NewJobResultFromErrors(jobErrors)
	}

	s3Client, err := createS3ClientForCleanup(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error creating S3 client: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("error creating S3 client", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	// Read retention hours from environment variable or use default
	retentionHours := utils.GetEnvInt("SANDBOX_CLEANUP_RETENTION_HOURS", defaultSandboxCleanupRetentionHours)

	// Use UTC timezone to match S3 folder structure (hour=HH is in UTC)
	now := time.Now().UTC()
	cutoffTime := now.Add(-time.Duration(retentionHours) * time.Hour)

	logger.GetLoggerWithContext(ctx).Info("starting sandbox storage cleanup",
		zap.String("bucket", bucketName),
		zap.Time("current_time_utc", now),
		zap.Time("cutoff_time_utc", cutoffTime),
		zap.Int("retention_hours", retentionHours),
		zap.String("timezone", "UTC"))

	// Process both published and dropped prefixes
	prefixes := []string{"databahn-sandbox-storage/published/", "databahn-sandbox-storage/dropped/"}
	totalDeleted := 0

	for _, prefix := range prefixes {
		deleted, err := cleanupPrefix(ctx, s3Client, bucketName, prefix, cutoffTime)
		if err != nil {
			errorMsg := fmt.Sprintf("error cleaning up prefix %s: %v", prefix, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("error cleaning up prefix",
				zap.String("prefix", prefix),
				zap.Error(err))
			// Continue with other prefixes even if one fails
			continue
		}
		totalDeleted += deleted
		logger.GetLoggerWithContext(ctx).Info("cleaned up prefix",
			zap.String("prefix", prefix),
			zap.Int("objects_deleted", deleted))
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed sandbox storage cleanup",
			zap.Int("total_objects_deleted", totalDeleted))
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("sandbox storage cleanup completed with errors",
		zap.Int("error_count", len(jobErrors)),
		zap.Int("total_objects_deleted", totalDeleted))
	return common.NewJobResultFromErrors(jobErrors)
}

func createS3ClientForCleanup(ctx context.Context) (*s3.Client, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(config.GetAppConfiguration().GetString(configuration.Region)))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return s3.NewFromConfig(cfg), nil
}

// cleanupPrefix scans and deletes old hour folders under a specific prefix
func cleanupPrefix(ctx context.Context, s3Client *s3.Client, bucket, prefix string, cutoffTime time.Time) (int, error) {
	totalScanned := 0
	totalDeleted := 0

	// Step 1: Identify all unique hour folders and their timestamps
	hourFoldersToDelete := make(map[string]time.Time) // map[hourFolderPrefix]timestamp

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return totalDeleted, fmt.Errorf("error listing objects: %w", err)
		}

		for _, obj := range page.Contents {
			totalScanned++
			key := aws.ToString(obj.Key)

			// Extract the hour folder from this object key
			hourFolder := extractHourFolder(key)
			if hourFolder == "" {
				continue // Not an hour folder structure
			}

			// Skip if we've already identified this hour folder for deletion
			if _, exists := hourFoldersToDelete[hourFolder]; exists {
				continue
			}

			// Parse the hour folder path to get timestamp (in UTC)
			folderTime, err := parseHourFolderPath(hourFolder)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Debug("skipping object with unparseable path",
					zap.String("key", key),
					zap.Error(err))
				continue
			}

			// Check if folder is older than cutoff (both times are in UTC)
			if folderTime.Before(cutoffTime) {
				hourFoldersToDelete[hourFolder] = folderTime
			}
		}
	}

	logger.GetLoggerWithContext(ctx).Info("identified hour folders to delete",
		zap.String("prefix", prefix),
		zap.Int("total_scanned", totalScanned),
		zap.Int("hour_folders_to_delete", len(hourFoldersToDelete)))

	// Step 2: Delete all objects in each identified hour folder
	for hourFolderPrefix, folderTime := range hourFoldersToDelete {
		deleted, err := deleteHourFolder(ctx, s3Client, bucket, hourFolderPrefix, folderTime)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error deleting hour folder",
				zap.String("hour_folder", hourFolderPrefix),
				zap.Error(err))
			// Continue with other folders even if one fails
			continue
		}
		totalDeleted += deleted
	}

	logger.GetLoggerWithContext(ctx).Info("completed deleting hour folders",
		zap.String("prefix", prefix),
		zap.Int("total_deleted", totalDeleted))

	return totalDeleted, nil
}

// deleteHourFolder deletes all objects within a specific hour folder
func deleteHourFolder(ctx context.Context, s3Client *s3.Client, bucket, hourFolderPrefix string, folderTime time.Time) (int, error) {
	var keysToDelete []string
	totalDeleted := 0

	// List all objects in this hour folder
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(hourFolderPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return totalDeleted, fmt.Errorf("error listing objects in hour folder: %w", err)
		}

		for _, obj := range page.Contents {
			keysToDelete = append(keysToDelete, aws.ToString(obj.Key))

			// Batch delete when we reach the limit
			if len(keysToDelete) >= batchDeleteSize {
				if err := deleteObjects(ctx, s3Client, bucket, keysToDelete); err != nil {
					return totalDeleted, err
				}
				totalDeleted += len(keysToDelete)
				keysToDelete = []string{} // Reset for next batch
			}
		}
	}

	// Delete remaining objects
	if len(keysToDelete) > 0 {
		if err := deleteObjects(ctx, s3Client, bucket, keysToDelete); err != nil {
			return totalDeleted, err
		}
		totalDeleted += len(keysToDelete)
	}

	logger.GetLoggerWithContext(ctx).Info("deleted hour folder",
		zap.String("hour_folder", hourFolderPrefix),
		zap.Time("folder_time", folderTime),
		zap.Int("objects_deleted", totalDeleted))

	return totalDeleted, nil
}

// extractHourFolder extracts the hour folder path from an object key
// Example: published/tenant_id=xxx/source_id=yyy/year=2025/month=12/day=17/hour=11/file.json
// Returns: published/tenant_id=xxx/source_id=yyy/year=2025/month=12/day=17/hour=11/
// Note: The trailing slash is CRITICAL to prevent prefix matching issues in S3.
// Without it, hour=1 would match hour=10, hour=11, through hour=19, causing unintended data deletion.
func extractHourFolder(key string) string {
	// Find the last occurrence of "hour="
	hourIdx := strings.LastIndex(key, "/hour=")
	if hourIdx == -1 {
		return ""
	}

	// Find the next slash after hour= or end of string
	remaining := key[hourIdx+1:]
	nextSlash := strings.Index(remaining, "/")
	if nextSlash == -1 {
		// The key ends with hour=XX, this is the folder itself
		// Add trailing slash to ensure exact prefix matching in S3
		return key + "/"
	}

	// Return up to and including the hour folder with trailing slash
	return key[:hourIdx+1+nextSlash] + "/"
}

// parseHourFolderPath parses the folder path to extract timestamp in UTC
// S3 folder structure uses UTC timezone (hour=HH is in UTC)
// Example: published/tenant_id=xxx/source_id=yyy/year=2025/month=12/day=17/hour=11
func parseHourFolderPath(folderPath string) (time.Time, error) {
	// Extract year, month, day, hour using regex
	pattern := regexp.MustCompile(`year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour=(\d{1,2})`)
	matches := pattern.FindStringSubmatch(folderPath)

	if len(matches) != 5 {
		return time.Time{}, fmt.Errorf("invalid folder path format: %s", folderPath)
	}

	year, _ := strconv.Atoi(matches[1])
	month, _ := strconv.Atoi(matches[2])
	day, _ := strconv.Atoi(matches[3])
	hour, _ := strconv.Atoi(matches[4])

	// Return time in UTC to match S3 folder timezone
	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), nil
}

// deleteObjects deletes a batch of objects from S3
func deleteObjects(ctx context.Context, s3Client *s3.Client, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// Build delete request
	var objectIdentifiers []types.ObjectIdentifier
	for _, key := range keys {
		objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectIdentifiers,
			Quiet:   aws.Bool(true), // Don't return success for each object
		},
	}

	output, err := s3Client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		return fmt.Errorf("error deleting objects: %w", err)
	}

	// Check for errors in the response
	if len(output.Errors) > 0 {
		errorMessages := make([]string, 0, len(output.Errors))
		for _, delErr := range output.Errors {
			errorMessages = append(errorMessages, fmt.Sprintf("key=%s: %s", aws.ToString(delErr.Key), aws.ToString(delErr.Message)))
		}
		return fmt.Errorf("errors deleting some objects: %s", strings.Join(errorMessages, "; "))
	}

	logger.GetLogger().Info("deleted objects from S3",
		zap.String("bucket", bucket),
		zap.Int("count", len(keys)))

	return nil
}
