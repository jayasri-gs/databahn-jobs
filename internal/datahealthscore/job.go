package datahealthscore

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/datahealthscore/constants"
	"github.com/databahn-ai/databahn-jobs/internal/datahealthscore/models"
	"github.com/databahn-ai/databahn-jobs/internal/datahealthscore/utils"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"time"
)

func CalculateDataHealthScore(ctx context.Context) error {

	logging.GetLogger().Info("calculating data health scores")

	violations, functionalitiesToConsider := utils.ReadViolationsFromConfig("/home/databahn/service/config.yaml")

	var logSources []source.Source
	if err := config.GetDB().Model(&source.Source{}).Where("replay_source != ?", true).Scan(&logSources).Error; err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting log sources", zap.Error(err))
		return err
	}

	alerts, err := utils.GetAllAlertsFromOpenSearch(ctx, functionalitiesToConsider)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting alerts from OpenSearch", zap.Error(err))
		return err
	}

	sourceToAlertsMap := getSourceToAlertMap(alerts)
	dbDataHealthScores, dbDataHealthScoreRecords := calculateScores(ctx, logSources, sourceToAlertsMap, violations)

	if err := saveDataHealthScores(ctx, dbDataHealthScores, dbDataHealthScoreRecords); err != nil {
		logging.GetLoggerWithContext(ctx).Error("transaction failed", zap.Error(err))
		return err
	}
	logging.GetLogger().Info("data health scores calculation completed", zap.Time("time", time.Now()), zap.Int("count", len(dbDataHealthScores)), zap.Int("records", len(dbDataHealthScoreRecords)))
	return nil
}

func getSourceToAlertMap(alerts []statistics.AlertDocument) map[string][]statistics.AlertDocument {
	sourceToAlertsMap := make(map[string][]statistics.AlertDocument)
	for _, alert := range alerts {
		sourceId := alert.FunctionalityEntityId
		sourceToAlertsMap[sourceId] = append(sourceToAlertsMap[sourceId], alert)
	}
	return sourceToAlertsMap
}

func calculateScores(ctx context.Context, logSources []source.Source, sourceToAlertsMap map[string][]statistics.AlertDocument, violations map[string]models.Violation) ([]models.DataHealthScore, []models.DataHealthScoreRecord) {
	var dbDataHealthScores []models.DataHealthScore
	var dbDataHealthScoreRecords []models.DataHealthScoreRecord

	for _, ls := range logSources {
		score := 100
		dataHealthScore := models.DataHealthScore{
			ID:         uuid.New(),
			TenantID:   ls.TenantID,
			SourceID:   ls.ID,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
			EntityType: constants.ENTITY_TYPE_SOURCE,
		}

		if sourceAlerts, ok := sourceToAlertsMap[ls.ID.String()]; ok {
			for _, alert := range sourceAlerts {
				if violation, ok := violations[alert.FunctionalityType]; ok {
					percentageInt, err := strconv.Atoi(violation.PercentageReduction)
					if err != nil {
						logging.GetLoggerWithContext(ctx).Error("error while parsing percentage reduction", zap.Error(err))
						continue
					}

					if !alert.Dismissed {
						score -= percentageInt * score / 100
					}

					healthScoreRecord := models.DataHealthScoreRecord{
						ID:                  uuid.New(),
						DataHealthScoreID:   dataHealthScore.ID,
						TenantID:            ls.TenantID,
						CreatedAt:           time.Now(),
						ViolationType:       violation.Functionality,
						ViolationSubtype:    violation.FunctionalityType,
						Message:             fmt.Sprintf(violation.Message, ls.Name),
						PercentageReduction: float32(percentageInt),
						ResolutionStatus:    constants.STATUS_OPEN,
						Details:             map[string]interface{}{},
					}
					dbDataHealthScoreRecords = append(dbDataHealthScoreRecords, healthScoreRecord)
				}
			}
		}
		dataHealthScore.HealthScore = float32(score)
		dbDataHealthScores = append(dbDataHealthScores, dataHealthScore)
	}

	return dbDataHealthScores, dbDataHealthScoreRecords
}

func saveDataHealthScores(ctx context.Context, dbDataHealthScores []models.DataHealthScore, dbDataHealthScoreRecords []models.DataHealthScoreRecord) error {
	return config.GetDB().Transaction(func(tx *gorm.DB) error {
		if len(dbDataHealthScores) > 0 {
			if err := tx.Create(&dbDataHealthScores).Error; err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while creating data health scores", zap.Error(err))
				return err
			}
		}
		if len(dbDataHealthScoreRecords) > 0 {
			if err := tx.Create(&dbDataHealthScoreRecords).Error; err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while creating data health score records", zap.Error(err))
				return err
			}
		}
		logging.GetLogger().Info("data health scores and records created successfully")
		return nil
	})
}
