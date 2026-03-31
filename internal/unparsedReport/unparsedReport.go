package unparsedReport

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	cn "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	cp_jobs "github.com/databahn-ai/databahn-jobs/internal/cp_alerts/jobs"
	awsemail "github.com/databahn-ai/databahn-jobs/internal/healthchecker/aws"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	osstore "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type UnparsedEventReport struct {
	TenantName          string
	SourceName          string
	TotalEvents         int64
	TotalUnparsedEvents int64
	UnparsedPercentage  float64
}

type TenantSummary struct {
	TenantName   string
	TotalSources int
}

// Memory-efficient streaming approach
type ReportStreamer struct {
	ctx          context.Context
	db           *gorm.DB
	startTime    time.Time
	endTime      time.Time
	emailBuilder *strings.Builder
	tenantCount  int
	totalSources int
}

func NewReportStreamer(ctx context.Context, db *gorm.DB, startTime, endTime time.Time) *ReportStreamer {
	return &ReportStreamer{
		ctx:          ctx,
		db:           db,
		startTime:    startTime,
		endTime:      endTime,
		emailBuilder: &strings.Builder{},
	}
}

func SendUnparsedEventsReport(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	startTime := time.Now()
	db := config.GetDB()

	logging.GetLoggerWithContext(ctx).Info("Starting unparsed events report generation")

	notificationMgr, err := notification.NewNotificationManager(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Failed to initialize notification manager", zap.Error(err))
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to initialize notification manager: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}
	defer notificationMgr.Close(ctx)

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Failed to get tenants from database", zap.Error(err))
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to get tenants: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}

	logging.GetLoggerWithContext(ctx).Info("Found tenants", zap.Int("count", len(tenants)))

	endTime := time.Now().UTC()
	startTimeRange := endTime.Add(-24 * time.Hour)

	streamer := NewReportStreamer(ctx, db, startTimeRange, endTime)

	batchSize := 5
	for i := 0; i < len(tenants); i += batchSize {
		end := i + batchSize
		if end > len(tenants) {
			end = len(tenants)
		}

		batch := tenants[i:end]
		if err := streamer.processTenantBatch(batch); err != nil {
			logging.GetLoggerWithContext(ctx).Error("Error processing tenant batch", zap.Error(err), zap.Int("batchStart", i), zap.Int("batchEnd", end))
			continue
		}

		logging.GetLoggerWithContext(ctx).Info("Processed tenant batch",
			zap.Int("batchStart", i),
			zap.Int("batchEnd", end),
			zap.Int("totalTenants", len(tenants)))
	}

	processingTime := time.Since(startTime)

	logging.GetLoggerWithContext(ctx).Info("Unparsed events report data collected successfully",
		zap.Int("totalTenants", streamer.tenantCount),
		zap.Int("sourcesWithUnparsedEvents", streamer.totalSources),
		zap.Duration("processingTime", processingTime))

	completeHTML := createCompleteHTML(streamer.emailBuilder.String(), streamer.tenantCount, streamer.totalSources, processingTime)

	err = sendUnparsedEventsReportEmail(ctx, notificationMgr, completeHTML)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Failed to send email report", zap.Error(err))
		jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to send email report: %v", err)})
		return common.NewJobResultFromErrors(jobErrors)
	}

	return common.NewJobResultSuccess()
}

func (rs *ReportStreamer) processTenantBatch(tenants []tenant.Tenant) error {
	for _, t := range tenants {
		tenantId := t.Id.String()
		tenantName := t.Name
		if tenantName == "" {
			tenantName = "Unknown Tenant"
		}

		statsAlias := osstore.StatisticsIndexAlias(tenantId)

		logging.GetLoggerWithContext(rs.ctx).Info("Processing tenant", zap.String("tenantId", tenantId), zap.String("tenantName", tenantName))

		// Get unparsed events for tenant first
		unparsedAgg, err := cp_jobs.GetUnparsedEventsForTenant(rs.ctx, strconv.Itoa(int(rs.startTime.UnixMilli())), strconv.Itoa(int(rs.endTime.UnixMilli())), statsAlias)
		if err != nil {
			logging.GetLoggerWithContext(rs.ctx).Error("Error getting unparsed events", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		// Skip tenant if no unparsed events
		if len(unparsedAgg.Agg) == 0 {
			logging.GetLoggerWithContext(rs.ctx).Info("Skipping tenant - no unparsed events", zap.String("tenantId", tenantId), zap.String("tenantName", tenantName))
			continue
		}

		// Get total events for tenant (only if we have unparsed events)
		totalEventsAgg, err := cp_jobs.GetTotalEventsForTenant(rs.ctx, strconv.Itoa(int(rs.startTime.UnixMilli())), strconv.Itoa(int(rs.endTime.UnixMilli())), statsAlias)
		if err != nil {
			logging.GetLoggerWithContext(rs.ctx).Error("Error getting total events", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		// Only fetch sources that have unparsed events
		sourceIds := getSourceIdsWithUnparsedEvents(unparsedAgg)
		if len(sourceIds) == 0 {
			logging.GetLoggerWithContext(rs.ctx).Info("Skipping tenant - no valid source IDs", zap.String("tenantId", tenantId))
			continue
		}

		// Get only the sources that have unparsed events with complete information
		sources, err := getSourcesByIds(rs.db, sourceIds)
		if err != nil {
			logging.GetLoggerWithContext(rs.ctx).Error("Error getting sources by IDs", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		// Validate that we have all source names
		if len(sources) != len(sourceIds) {
			logging.GetLoggerWithContext(rs.ctx).Warn("Some sources not found in database",
				zap.String("tenantId", tenantId),
				zap.Int("expectedSources", len(sourceIds)),
				zap.Int("foundSources", len(sources)))
		}

		// Create source ID to source name mapping with validation
		sourceIdToName := make(map[string]string)
		for _, s := range sources {
			if s.Name != "" {
				sourceIdToName[s.ID.String()] = s.Name
			} else {
				logging.GetLoggerWithContext(rs.ctx).Warn("Source has empty name",
					zap.String("sourceId", s.ID.String()),
					zap.String("tenantId", tenantId))
				// Use source ID as fallback if name is empty
				sourceIdToName[s.ID.String()] = fmt.Sprintf("Source-%s", s.ID.String()[:8])
			}
		}

		// Skip if we don't have any valid source names
		if len(sourceIdToName) == 0 {
			logging.GetLoggerWithContext(rs.ctx).Warn("No valid source names found for tenant",
				zap.String("tenantId", tenantId),
				zap.String("tenantName", tenantName))
			continue
		}

		// Log source information for debugging
		logging.GetLoggerWithContext(rs.ctx).Info("Processing tenant sources",
			zap.String("tenantId", tenantId),
			zap.String("tenantName", tenantName),
			zap.Int("totalSources", len(sourceIdToName)),
			zap.Strings("sourceNames", getSourceNames(sourceIdToName)))

		// Process data for this tenant and stream to email builder
		tenantSummary, reports := rs.processTenantData(tenantName, unparsedAgg, totalEventsAgg, sourceIdToName)

		// Add tenant section to email builder (streaming approach)
		rs.addTenantSectionToEmail(tenantSummary, reports)

		// Update global counters
		rs.tenantCount++
		rs.totalSources += tenantSummary.TotalSources

	}

	return nil
}

func (rs *ReportStreamer) processTenantData(tenantName string, unparsedAgg, totalEventsAgg statistics.AggregateResponse, sourceIdToName map[string]string) (TenantSummary, []UnparsedEventReport) {
	var reports []UnparsedEventReport

	// Create maps for easy lookup
	sourceIdToUnparsedCount := make(map[string]float64)
	sourceIdToTotalCount := make(map[string]float64)

	// Process total events
	for sourceId, count := range totalEventsAgg.Agg {
		if v, ok := count.(float64); ok && v > 0 {
			sourceIdToTotalCount[sourceId] = v
		}
	}

	// Process only sources that have unparsed events
	for sourceId, unparsedCount := range unparsedAgg.Agg {
		if v, ok := unparsedCount.(float64); ok && v > 0 {
			sourceIdToUnparsedCount[sourceId] = v

			totalCount := sourceIdToTotalCount[sourceId]
			if totalCount > 0 {
				percentage := (v / totalCount) * 100
				sourceName := sourceIdToName[sourceId]

				// Additional safety check - if sourceName is still empty, skip this source
				if sourceName == "" {
					logging.GetLoggerWithContext(rs.ctx).Warn("Skipping source with empty name",
						zap.String("sourceId", sourceId),
						zap.String("tenantName", tenantName))
					continue
				}

				report := UnparsedEventReport{
					TenantName:          tenantName,
					SourceName:          sourceName,
					TotalEvents:         int64(totalCount),
					TotalUnparsedEvents: int64(v),
					UnparsedPercentage:  percentage,
				}
				reports = append(reports, report)
			}
		}
	}

	// Return summary for global counters and reports for email
	return TenantSummary{
		TenantName:   tenantName,
		TotalSources: len(reports),
	}, reports
}

func (rs *ReportStreamer) addTenantSectionToEmail(tenantSummary TenantSummary, reports []UnparsedEventReport) {
	// Only add section if there are sources with unparsed events
	if tenantSummary.TotalSources == 0 {
		return
	}

	// Build tenant section HTML and add to builder
	tenantSection := fmt.Sprintf(`
        <div class="tenant-section">
            <div class="tenant-header">
                %s
            </div>
                        <div class="tenant-summary">
                <strong>Tenant Summary:</strong> %d sources with unparsed events
            </div>
            <div class="table-container">
            <table>
                <thead>
                <tr>
                    <th>Source Name</th>
                    <th>Total Events</th>
                    <th>Unparsed Events</th>
                    <th>Unparsed Percentage</th>
                </tr>
                </thead>
                <tbody>`,
		tenantSummary.TenantName,
		tenantSummary.TotalSources)

	rs.emailBuilder.WriteString(tenantSection)

	// Add individual source rows
	for _, report := range reports {
		sourceRow := fmt.Sprintf(`
                <tr>
                    <td>%s</td>
                    <td>%d</td>
                    <td>%d</td>
                    <td>%.2f%%</td>
                </tr>`,
			report.SourceName,
			report.TotalEvents,
			report.TotalUnparsedEvents,
			report.UnparsedPercentage)

		rs.emailBuilder.WriteString(sourceRow)
	}

	rs.emailBuilder.WriteString(`
                </tbody>
            </table>
            </div>
        </div>`)
}

// Helper function to get source IDs that have unparsed events
func getSourceIdsWithUnparsedEvents(unparsedAgg statistics.AggregateResponse) []string {
	var sourceIds []string
	for sourceId := range unparsedAgg.Agg {
		sourceIds = append(sourceIds, sourceId)
	}
	return sourceIds
}

// Helper function to extract source names from the mapping
func getSourceNames(sourceIdToName map[string]string) []string {
	var names []string
	for _, name := range sourceIdToName {
		names = append(names, name)
	}
	return names
}

// parseEmailRecipients parses comma-separated email addresses
func parseEmailRecipients(emailConfig string) []string {
	if emailConfig == "" {
		return []string{}
	}

	// Split by comma and clean up whitespace
	emails := strings.Split(emailConfig, ",")
	var cleanEmails []string

	for _, email := range emails {
		// Trim whitespace and add if not empty
		cleanEmail := strings.TrimSpace(email)
		if cleanEmail != "" {
			cleanEmails = append(cleanEmails, cleanEmail)
		}
	}

	logging.GetLogger().Info("Email recipients parsed",
		zap.Int("totalFound", len(emails)),
		zap.Int("cleanEmails", len(cleanEmails)))

	return cleanEmails
}

// Get sources by IDs instead of by tenant (more efficient)
func getSourcesByIds(db *gorm.DB, sourceIds []string) ([]source.Source, error) {
	var sources []source.Source
	if len(sourceIds) == 0 {
		return sources, nil
	}

	// Select specific fields to ensure we get complete source information
	result := db.Select("id, name, tenant_id, status").Where("id IN ? AND status = 'ACTIVE'", sourceIds).Find(&sources)

	if result.Error != nil {
		return sources, result.Error
	}

	// Log if we didn't find all expected sources
	if len(sources) != len(sourceIds) {
		// Find which source IDs are missing
		foundIds := make(map[string]bool)
		for _, s := range sources {
			foundIds[s.ID.String()] = true
		}

		var missingIds []string
		for _, id := range sourceIds {
			if !foundIds[id] {
				missingIds = append(missingIds, id)
			}
		}

		if len(missingIds) > 0 {
			logging.GetLogger().Warn("Some source IDs not found in database",
				zap.Strings("missingSourceIds", missingIds))
		}
	}

	return sources, nil
}

func sendUnparsedEventsReportEmail(ctx context.Context, notificationMgr *notification.NotificationManager, emailBody string) error {
	emailConfig := config.GetAppConfiguration().GetString(awsemail.UnparsedReport)

	emailTo := parseEmailRecipients(emailConfig)

	if len(emailTo) == 0 {
		logging.GetLoggerWithContext(ctx).Error("No email recipients found in configuration")
		return fmt.Errorf("no email recipients configured")
	}

	logging.GetLoggerWithContext(ctx).Info("Publishing email notification to kafka",
		zap.Strings("recipients", emailTo),
		zap.Int("count", len(emailTo)))

	emailRequest := cn.EmailNotificationRequest{
		Recipients: &cn.EmailRecipients{
			To: emailTo,
		},
		Body:    emailBody,
		Subject: "DataBahn Unparsed Events Alert Report - All Tenants",
	}

	err := notificationMgr.SendEmailNotification(emailRequest)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Failed to publish email notification to kafka", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("Unparsed events report email notification published to kafka successfully")
	return nil
}

func createCompleteHTML(tenantSections string, tenantCount, totalSources int, processingTime time.Duration) string {
	htmlBody := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f4f4f4;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
        }
        .logo {
            max-width: 300px;
            margin-bottom: 20px;
        }
        .summary {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .tenant-section {
            margin: 30px 0;
            border: 1px solid #ddd;
            border-radius: 8px;
            overflow: hidden;
        }
        .tenant-header {
            background-color: #007bff;
            color: white;
            padding: 15px;
            font-size: 18px;
            font-weight: bold;
        }
        .tenant-summary {
            background-color: #e7f3ff;
            padding: 15px;
            border-bottom: 1px solid #ddd;
        }
        table {
            width: 100%%;
            border-collapse: collapse;
            margin-top: 0;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
            font-size: 14px;
        }
        th {
            background-color: #f4f4f4;
            color: #333;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .footer {
            margin-top: 30px;
            text-align: center;
            color: #666;
        }
        .table-container {
            margin-top: 0;
            overflow-x: auto;
        }
        .data-note {
            background-color: #e7f3ff;
            border-left: 4px solid #007bff;
            padding: 10px;
            margin: 20px 0;
            font-size: 14px;
        }
        .performance-info {
            background-color: #d4edda;
            border-left: 4px solid #28a745;
            padding: 10px;
            margin: 20px 0;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="https://databahn.ai/wp-content/uploads/2024/02/DB-logo-reversed-final-1024x237-1-1.webp" alt="DataBahn Inc" class="logo">
            <h1>Unparsed Events Alert Report - All Tenants</h1>
            <p>Generated on: %s</p>
        </div>
        
        <div class="summary">
            <h2>Overall Summary</h2>
            <p><strong>Total Tenants:</strong> %d</p>
            <p><strong>Sources with Unparsed Events:</strong> %d</p>
        </div>

        <div class="performance-info">
            <strong>Performance:</strong> Report generated in %s
        </div>

        <div class="data-note">
            <strong>Note:</strong> This report shows only sources with unparsed events for the last 24 hours across all tenants. Sources with 0%% unparsed events are excluded to focus on actionable items.
        </div>

        <!-- Tenant sections -->
        %s
        
        <div class="footer">
            <p>This report was automatically generated by DataBahn.</p>
            <p>Please investigate sources with high unparsed event percentages.</p>
            <p>Regards,<br>DataBahn Team</p>
        </div>
    </div>
</body>
</html>`,
		time.Now().Format("2006-01-02 15:04:05 UTC"),
		tenantCount,
		totalSources,
		processingTime.String(),
		tenantSections)

	return htmlBody
}
