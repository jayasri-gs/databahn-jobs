package datahealthscore

import (
	"context"
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

func main() {

	ctx := context.Background()

	violations := utils.ReadViolationsFromConfig("config.yaml")

	var logSources []source.Source
	err := config.GetDB().Model(&source.Source{}).Scan(&logSources).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting log sources", zap.Error(err))
	}

	alerts, err := utils.GetAllAlertsFromOpenSearch(ctx)
	sourceToAlertsMap := getSourceToAlertMap(alerts)

	var dbDataHealthScores []models.DataHealthScore
	var dbDataHealthScoreRecords []models.DataHealthScoreRecord
	for _, ls := range logSources {
		score := 100
		dataHealthScore := models.DataHealthScore{
			ID:        uuid.New(),
			TenantID:  ls.TenantID,
			SourceID:  ls.ID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		var healthScoreRecords []models.DataHealthScoreRecord
		if sourceAlerts, ok := sourceToAlertsMap[ls.ID.String()]; ok {
			for _, alert := range sourceAlerts {
				if violation, ok := violations[alert.FunctionalityType]; ok {
					percentageInt, err := strconv.Atoi(violation.PercentageReduction)
					if err != nil {
						logging.GetLoggerWithContext(ctx).Error("error while parsing percentage reduction", zap.Error(err))
					}

					if alert.Dismissed {
						score = score - (percentageInt*score/100)/2
					} else {
						score = score - (percentageInt * score / 100)
					}

					healthScoreRecord := models.DataHealthScoreRecord{
						ID:                  uuid.New(),
						DataHealthScoreID:   dataHealthScore.ID,
						TenantID:            ls.TenantID,
						CreatedAt:           time.Now(),
						ViolationType:       violation.Functionality,
						ViolationSubtype:    violation.FunctionalityType,
						Message:             violation.Message,
						PercentageReduction: float32(percentageInt),
						ResolutionStatus:    constants.STATUS_OPEN,
						Details:             map[string]interface{}{},
					}
					healthScoreRecords = append(healthScoreRecords, healthScoreRecord)
				}
			}
		}
		dataHealthScore.HealthScore = float32(score)

		dbDataHealthScores = append(dbDataHealthScores, dataHealthScore)
		dbDataHealthScoreRecords = append(dbDataHealthScoreRecords, healthScoreRecords...)
	}

	err = config.GetDB().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&dbDataHealthScoreRecords).Error; err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while creating data health score records", zap.Error(err))
			return err
		}
		if err := tx.Create(&dbDataHealthScores).Error; err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while creating data health scores", zap.Error(err))
			return err
		}
		return nil
	})

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("transaction failed", zap.Error(err))
	}

}

func getSourceToAlertMap(alerts []statistics.AlertDocument) map[string][]statistics.AlertDocument {
	sourceToAlertsMap := make(map[string][]statistics.AlertDocument)
	for _, alert := range alerts {
		sourceId := alert.FunctionalityEntityId
		if _, ok := sourceToAlertsMap[sourceId]; !ok {
			sourceToAlertsMap[sourceId] = []statistics.AlertDocument{}
		}
		sourceToAlertsMap[sourceId] = append(sourceToAlertsMap[sourceId], alert)
	}
	return sourceToAlertsMap
}
