package transformationCheckerUtility

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/data_model"
	"github.com/databahn-ai/databahn-jobs/internal/store/data_transformation"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// ComparisonReport represents a single field comparison between data model and rename config
type ComparisonReport struct {
	DatabahnAttribute     string `json:"databahn_attribute"`
	DataModelName         string `json:"data_model_name"`
	DataModelLogAttribute string `json:"data_model_log_attribute"`
	RenameConfigLogAttr   string `json:"rename_config_log_attribute"`
}

func ValidateTransformations(ctx context.Context) error {

	tenantId := utils.GetEnvOrDefault("TRANSFORMATION_CHECKER_TENANT_ID", "")
	tenantUUID := utils.UUIDFromStringOrNil(tenantId)
	logger.GetLogger().Info("Validating transformations for tenant", zap.Any("tenant_id", tenantUUID))

	transformations, err := data_transformation.GetDataTransformationsByTypeAndFunctionTypeByTenant(ctx, config.GetDB(), tenantUUID)
	if err != nil {
		logger.GetLogger().Error("Error fetching transformations", zap.Error(err), zap.Any("tenant_id", tenantUUID))
		return err
	}

	var allComparisonReports []ComparisonReport
	var allTransformations []string

	for _, tr := range transformations {

		allTransformations = append(allTransformations, tr.Name)

		// Get source ID from transformation
		pipelineID := tr.PipelineID
		if pipelineID == nil {
			logger.GetLogger().Warn("Transformation has no pipeline ID, skipping", zap.String("transformation_id", tr.ID.String()))
			continue
		}

		// Get pipeline log source mappings to find sources
		pipelineLogSources, err := pipeline.GetPipelineLogSources(ctx, config.GetDB(), *pipelineID)
		if err != nil {
			logger.GetLogger().Error("Error fetching pipeline log sources", zap.Error(err), zap.String("pipeline_id", pipelineID.String()))
			continue
		}

		// Process each source mapping
		for _, pls := range pipelineLogSources {
			// Get source details by ID
			sourceDetails, err := source.GetSourceByID(ctx, config.GetDB(), pls.LogSourceID)
			if err != nil {
				logger.GetLogger().Error("Error fetching source details", zap.Error(err), zap.String("source_id", pls.LogSourceID.String()))
				continue
			}

			// Get device, vendor, and log type from source
			device := sourceDetails.Device
			vendor := sourceDetails.Vendor
			logType := sourceDetails.LogType

			// Fetch the correct data model based on device, vendor, log type
			dataModels, err := data_model.GetDataModelByDeviceVendorLogType(ctx, config.GetDB(), device, vendor, logType)
			if err != nil {
				logger.GetLogger().Error("Error fetching data model", zap.Error(err),
					zap.String("device", device),
					zap.String("vendor", vendor),
					zap.String("log_type", logType))
				continue
			}

			if len(dataModels) == 0 {
				logger.GetLogger().Warn("No data models found",
					zap.String("device", device),
					zap.String("vendor", vendor),
					zap.String("log_type", logType))
				continue
			}

			// Parse transformation rename config
			var renameConfig data_transformation.TransformationRenameConfig
			if tr.TransformationFunction != nil && tr.TransformationFunction.RenameConfig != nil {
				err = json.Unmarshal(tr.TransformationFunction.RenameConfig, &renameConfig)
				if err != nil {
					logger.GetLogger().Error("Error parsing rename config", zap.Error(err))
					continue
				}
			}

			if len(renameConfig.RenameFields) == 0 {
				logger.GetLogger().Warn("No rename fields found in transformation config")
				continue
			}

			// Compare data model fields with rename config to find extra fields
			for _, dataModel := range dataModels {

				// Check if this data model field exists in rename config
				foundInRenameConfig := false
				for _, renameField := range renameConfig.RenameFields {
					if renameField.DatabahnAttribute == dataModel.Name {
						foundInRenameConfig = true
						break
					}
				}

				// If not found in rename config, it's extra in data model
				if !foundInRenameConfig {
					report := ComparisonReport{
						DatabahnAttribute:     dataModel.Name,
						DataModelName:         tr.Name,
						DataModelLogAttribute: dataModel.LogAttribute,
						RenameConfigLogAttr:   "MISSING",
					}
					allComparisonReports = append(allComparisonReports, report)
				}
			}

			// Check for extra fields in rename config that don't exist in data model
			for _, renameField := range renameConfig.RenameFields {
				foundInDataModel := false
				for _, dataModel := range dataModels {
					if dataModel.Name == renameField.DatabahnAttribute {
						foundInDataModel = true
						break
					}
				}

				// If not found in data model, it's extra in rename config
				if !foundInDataModel {
					report := ComparisonReport{
						DatabahnAttribute:     renameField.DatabahnAttribute,
						DataModelName:         tr.Name,
						DataModelLogAttribute: "MISSING",
						RenameConfigLogAttr:   renameField.LogAttribute,
					}
					allComparisonReports = append(allComparisonReports, report)
				}
			}
		}
	}

	// Generate simple tabular report
	tabularReport := generateSimpleTabularReport(allComparisonReports)

	// Print the report to console
	fmt.Println(tabularReport)

	logger.GetLogger().Info("Transformation validation completed and report printed to console",
		zap.Int("total_transformations", len(transformations)),
		zap.Int("total_comparisons", len(allComparisonReports)))

	return nil
}

// generateSimpleTabularReport creates a simple, easy-to-understand report showing extra fields
func generateSimpleTabularReport(reports []ComparisonReport) string {
	var builder strings.Builder

	// Group reports by transformation
	transformationGroups := make(map[string][]ComparisonReport)
	for _, r := range reports {
		transformationGroups[r.DataModelName] = append(transformationGroups[r.DataModelName], r)
	}

	// Generate report for each transformation separately
	for transformationName, transformationReports := range transformationGroups {
		builder.WriteString(strings.Repeat("-", 80) + "\n")
		builder.WriteString(fmt.Sprintf("transformation name - %s\n", transformationName))

		// Collect extra fields in data model
		var extraInDataModel []string
		var extraInRenameConfig []string

		for _, r := range transformationReports {
			if r.RenameConfigLogAttr == "MISSING" {
				extraInDataModel = append(extraInDataModel, r.DatabahnAttribute)
			} else if r.DataModelLogAttribute == "MISSING" {
				extraInRenameConfig = append(extraInRenameConfig, r.DatabahnAttribute)
			}
		}

		// Output extra fields in data model
		if len(extraInDataModel) > 0 {
			builder.WriteString(fmt.Sprintf("extra fields in data model - %s\n", strings.Join(extraInDataModel, ", ")))
		} else {
			builder.WriteString("extra fields in data model - none\n")
		}

		// Output extra fields in rename config
		if len(extraInRenameConfig) > 0 {
			builder.WriteString(fmt.Sprintf("extra fields in transformation object - %s\n", strings.Join(extraInRenameConfig, ", ")))
		} else {
			builder.WriteString("extra fields in transformation object - none\n")
		}

		builder.WriteString(strings.Repeat("-", 80) + "\n\n")
	}

	return builder.String()
}
