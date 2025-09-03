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

func ValidateTransformations(ctx context.Context) error {

	tenantId := utils.GetEnvOrDefault("TRANSFORMATION_CHECKER_TENANT_ID", "")
	tenantUUID := utils.UUIDFromStringOrNil(tenantId)
	logger.GetLogger().Info("Validating transformations for tenant", zap.Any("tenant_id", tenantUUID))

	transformations, err := data_transformation.GetDataTransformationsByTypeAndFunctionTypeByTenant(ctx, config.GetDB(), tenantUUID)
	if err != nil {
		logger.GetLogger().Error("Error fetching transformations", zap.Error(err), zap.Any("tenant_id", tenantUUID))
		return err
	}

	var builder strings.Builder

	for _, tr := range transformations {
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

			// Count extra fields directly
			var extraInDataModel []string
			var extraInRenameConfig []string

			// Check for extra fields in data model
			for _, dataModel := range dataModels {

				// Check if this data model field exists in rename config
				foundInRenameConfig := false
				for _, renameField := range renameConfig.RenameFields {
					if renameField.DatabahnAttribute == dataModel.Name {
						foundInRenameConfig = true
						break
					}
				}

				if !foundInRenameConfig {
					extraInDataModel = append(extraInDataModel, dataModel.Name)
				}
			}

			// Check for extra fields in rename config
			for _, renameField := range renameConfig.RenameFields {
				foundInDataModel := false
				for _, dataModel := range dataModels {
					if dataModel.Name == renameField.DatabahnAttribute {
						foundInDataModel = true
						break
					}
				}

				if !foundInDataModel {
					extraInRenameConfig = append(extraInRenameConfig, renameField.DatabahnAttribute)
				}
			}

			// Generate report directly
			builder.WriteString(strings.Repeat("-", 80) + "\n")
			builder.WriteString(fmt.Sprintf("transformation name - %s\n", tr.Name))

			if len(extraInDataModel) > 0 {
				builder.WriteString(fmt.Sprintf("extra fields in data model - %s\n", strings.Join(extraInDataModel, ", ")))
			} else {
				builder.WriteString("extra fields in data model - none\n")
			}

			if len(extraInRenameConfig) > 0 {
				builder.WriteString(fmt.Sprintf("extra fields in transformation object - %s\n", strings.Join(extraInRenameConfig, ", ")))
			} else {
				builder.WriteString("extra fields in transformation object - none\n")
			}

			builder.WriteString(strings.Repeat("-", 80) + "\n\n")
		}
	}

	// Print the report to console
	fmt.Println(builder.String())

	logger.GetLogger().Info("Transformation validation completed and report printed to console",
		zap.Int("total_transformations", len(transformations)))

	return nil
}
