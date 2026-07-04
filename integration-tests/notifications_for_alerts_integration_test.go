//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dbkafka "github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
)

const (
	notificationJobTimeout  = 2 * time.Minute
	notificationJobPollRate = 2 * time.Second

	reminderSkipSameWindowReason = "reminder already sent for current window"
	reminderSkipDurationReason   = "reminder duration has elapsed"

	defaultReminderDuration = 7 * 24 * time.Hour
)

func TestNotificationsForAlertsWhenObservedSinceLastCheckpoint(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		tenantName = "notifications-integration-tenant"
		entityName = "integration-test-log-source"
	)
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "notifications-integration-email-target",
		TargetEmail: "notifications-integration@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	entityID := uuid.NewString()
	alert, err := BuildExternalAlert(fixture.TenantID.String(), entityID, entityName)
	if err != nil {
		t.Fatalf("build alert: %v", err)
	}
	IndexExternalAlert(ctx, t, job.GetOpenSearch(t), alert)

	expectedOpsGenieSubject := fmt.Sprintf(
		"configuration_processing_failure:%s:%s",
		tenantName,
		entityName,
	)
	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		tenantName,
	)

	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)

	opsgenieMessage := WaitForKafkaMessage(t, kafka, OpsgenieNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, opsgenieMessage, "$.subject", expectedOpsGenieSubject)

	emailMessage := WaitForKafkaMessage(t, kafka, EmailNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, emailMessage, "$.subject", expectedEmailSubject)
	AssertKafkaMessageJSONPath(t, emailMessage, "$.targets[0].targetId", fixture.TargetID.String())
	AssertKafkaMessageJSONPath(t, emailMessage, "$.targets[0].tenantId", fixture.TenantID.String())

	notificationSentMessage := WaitForKafkaMessage(t, kafka, AlertIndexingTopic, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alert.Id)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageHeader(t, notificationSentMessage, "action", "notification_sent")
	AssertKafkaMessageJSONPath(t, notificationSentMessage, "$.id", alert.Id)
}

func TestNotificationsForAlertsInternalAlertOnlyOpsGenie(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		databahnTenantName = "Databahn Engineering"
		entityName         = "internal-alert-ops-only-entity"
	)

	entityID := uuid.NewString()
	alert, err := BuildInternalAlert(entityID, entityName)
	if err != nil {
		t.Fatalf("build internal alert: %v", err)
	}
	IndexInternalAlert(ctx, t, job.GetOpenSearch(t), alert)

	expectedOpsGenieSubject := fmt.Sprintf(
		"configuration_processing_failure:%s:%s:internal",
		databahnTenantName,
		entityName,
	)
	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		databahnTenantName,
	)

	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)

	opsgenieMessage := WaitForKafkaMessage(t, kafka, OpsgenieNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, opsgenieMessage, "$.subject", expectedOpsGenieSubject)

	AssertKafkaMessageCount(t, kafka, EmailNotificationTopic, 0, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	})
	AssertKafkaMessageCount(t, kafka, AlertIndexingTopic, 0, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alert.Id)
	})
}

func TestNotificationsForAlertsBackfillsActivationTimeFromLastObservedAt(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		tenantName = "activation-backfill-tenant"
		entityName = "activation-backfill-entity"
	)
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "activation-backfill-email-target",
		TargetEmail: "activation-backfill@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	now := time.Now().UTC()
	observedAt := now.Add(-5 * time.Minute)
	firstObservedAt := now.Add(-2 * time.Hour)

	entityID := uuid.NewString()
	alert, err := BuildExternalAlertWithTitle(
		fixture.TenantID.String(),
		entityID,
		entityName,
		"legacy alert missing activation time",
	)
	if err != nil {
		t.Fatalf("build alert: %v", err)
	}
	IndexExternalAlertWithState(ctx, t, job.GetOpenSearch(t), alert, observedAt, ExternalAlertNotificationState{
		FirstObservedAt: firstObservedAt.UnixMilli(),
	})

	jobStartedAt := time.Now().UTC().Add(-1 * time.Minute).UnixMilli()
	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)
	notificationSentMessage := WaitForKafkaMessage(t, kafka, AlertIndexingTopic, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alert.Id)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertNotificationSentBackfillsActivationTime(
		t,
		notificationSentMessage,
		alert.Id,
		1,
		observedAt.UnixMilli(),
		jobStartedAt,
	)
}

func TestNotificationsForAlertsSkipsWhenLastObservedBeforeCheckpoint(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		tenantName = "checkpoint-skip-tenant"
		entityName = "checkpoint-skip-entity"
	)
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "checkpoint-skip-email-target",
		TargetEmail: "checkpoint-skip@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	now := time.Now().UTC()
	checkpointAt := now.Add(-3 * time.Minute)
	alertObservedAt := now.Add(-10 * time.Minute)
	if err := fixtures.SeedExternalAlertCheckpoint(ctx, GormDB(), fixture.TenantID, checkpointAt.UnixMilli()); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	entityID := uuid.NewString()
	alert, err := BuildExternalAlert(fixture.TenantID.String(), entityID, entityName)
	if err != nil {
		t.Fatalf("build alert: %v", err)
	}
	IndexExternalAlertAt(ctx, t, job.GetOpenSearch(t), alert, alertObservedAt)

	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)
	time.Sleep(3 * time.Second)

	expectedOpsGenieSubject := fmt.Sprintf(
		"configuration_processing_failure:%s:%s",
		tenantName,
		entityName,
	)
	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		tenantName,
	)

	AssertKafkaMessageCount(t, kafka, OpsgenieNotificationTopic, 0, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubject)
	})
	AssertKafkaMessageCount(t, kafka, EmailNotificationTopic, 0, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	})
	AssertKafkaMessageCount(t, kafka, AlertIndexingTopic, 0, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alert.Id)
	})
}

func TestNotificationsForAlertsMultipleAlertsSameModule(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		tenantName = "multi-alert-tenant"
		entityOne  = "multi-alert-entity-one"
		entityTwo  = "multi-alert-entity-two"
	)
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "multi-alert-email-target",
		TargetEmail: "multi-alert@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	entityOneID := uuid.NewString()
	entityTwoID := uuid.NewString()
	alertOne, err := BuildExternalAlertWithTitle(fixture.TenantID.String(), entityOneID, entityOne, "multi alert one")
	if err != nil {
		t.Fatalf("build alert one: %v", err)
	}
	alertTwo, err := BuildExternalAlertWithTitle(fixture.TenantID.String(), entityTwoID, entityTwo, "multi alert two")
	if err != nil {
		t.Fatalf("build alert two: %v", err)
	}

	openSearch := job.GetOpenSearch(t)
	IndexExternalAlert(ctx, t, openSearch, alertOne)
	IndexExternalAlert(ctx, t, openSearch, alertTwo)

	expectedOpsGenieSubjectOne := fmt.Sprintf(
		"configuration_processing_failure:%s:%s",
		tenantName,
		entityOne,
	)
	expectedOpsGenieSubjectTwo := fmt.Sprintf(
		"configuration_processing_failure:%s:%s",
		tenantName,
		entityTwo,
	)
	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		tenantName,
	)

	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)

	opsgenieOne := WaitForKafkaMessage(t, kafka, OpsgenieNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubjectOne)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, opsgenieOne, "$.subject", expectedOpsGenieSubjectOne)

	opsgenieTwo := WaitForKafkaMessage(t, kafka, OpsgenieNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubjectTwo)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, opsgenieTwo, "$.subject", expectedOpsGenieSubjectTwo)

	emailMessage := WaitForKafkaMessage(t, kafka, EmailNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, emailMessage, "$.subject", expectedEmailSubject)

	notificationSentOne := WaitForKafkaMessage(t, kafka, AlertIndexingTopic, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alertOne.Id)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, notificationSentOne, "$.id", alertOne.Id)

	notificationSentTwo := WaitForKafkaMessage(t, kafka, AlertIndexingTopic, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alertTwo.Id)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, notificationSentTwo, "$.id", alertTwo.Id)

	AssertKafkaMessageCount(t, kafka, OpsgenieNotificationTopic, 1, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubjectOne)
	})
	AssertKafkaMessageCount(t, kafka, OpsgenieNotificationTopic, 1, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedOpsGenieSubjectTwo)
	})
	AssertKafkaMessageCount(t, kafka, EmailNotificationTopic, 1, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	})
	AssertKafkaMessageCount(t, kafka, AlertIndexingTopic, 1, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alertOne.Id)
	})
	AssertKafkaMessageCount(t, kafka, AlertIndexingTopic, 1, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alertTwo.Id)
	})
}

func TestNotificationsForAlertsEmailBodyNewAndReminderSections(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const (
		tenantName     = "email-sections-tenant"
		newEntity      = "email-kafka-new-entity"
		reminderEntity = "email-kafka-reminder-entity"
	)
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "email-sections-email-target",
		TargetEmail: "email-sections@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	now := time.Now().UTC()
	observedAt := now.Add(-5 * time.Minute)
	activationInWindow := now.Add(-50 * time.Hour)
	lastNotificationPreviousWindow := now.Add(-26 * time.Hour)

	tenantID := fixture.TenantID.String()
	openSearch := job.GetOpenSearch(t)

	newAlert, err := BuildExternalAlertWithTitle(tenantID, uuid.NewString(), newEntity, "email kafka new alert")
	if err != nil {
		t.Fatalf("build new alert: %v", err)
	}
	reminderAlert, err := BuildExternalAlertWithTitle(tenantID, uuid.NewString(), reminderEntity, "email kafka reminder alert")
	if err != nil {
		t.Fatalf("build reminder alert: %v", err)
	}

	IndexExternalAlertWithState(ctx, t, openSearch, newAlert, observedAt, ExternalAlertNotificationState{
		NotificationCount: 0,
	})
	IndexExternalAlertWithState(ctx, t, openSearch, reminderAlert, observedAt, ExternalAlertNotificationState{
		NotificationCount:    2,
		LastActivationTime:   activationInWindow.UnixMilli(),
		LastNotificationTime: lastNotificationPreviousWindow.UnixMilli(),
	})

	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		tenantName,
	)

	runNotificationsForAlertsJob(t, ctx, tg, job)

	kafka := job.GetKafka(t)
	emailMessage := WaitForKafkaMessage(t, kafka, EmailNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, emailMessage, "$.subject", expectedEmailSubject)
	AssertKafkaEmailBodySections(t, emailMessage, newEntity, reminderEntity)
}

func TestNotificationsForAlertsReminderScenarios(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	const tenantName = "reminder-scenarios-tenant"
	fixture := SeedNewNotificationTenant(t, fixtures.NotificationFixtureOptions{
		TenantName:  tenantName,
		TargetName:  "reminder-scenarios-email-target",
		TargetEmail: "reminder-scenarios@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE"},
	})

	now := time.Now().UTC()
	observedAt := now.Add(-5 * time.Minute)
	activationInWindow := now.Add(-50 * time.Hour)
	activationThreeHoursAgo := now.Add(-3 * time.Hour)
	activationExpired := now.Add(-defaultReminderDuration).Add(-24 * time.Hour)
	lastNotificationPreviousWindow := now.Add(-26 * time.Hour)
	lastNotificationCurrentWindow := now.Add(-1 * time.Hour)
	lastNotificationDurationEnded := now.Add(-defaultReminderDuration).Add(-2 * time.Hour)

	tenantID := fixture.TenantID.String()
	openSearch := job.GetOpenSearch(t)

	type reminderScenario struct {
		entityName string
		title      string
		state      ExternalAlertNotificationState
		wantCount  int
		skipReason string
	}

	scenarios := []reminderScenario{
		{
			entityName: "reminder-new-notification",
			title:      "reminder scenario new notification",
			state:      ExternalAlertNotificationState{NotificationCount: 0},
			wantCount:  1,
		},
		{
			entityName: "reminder-count-one",
			title:      "reminder scenario count one",
			state:      ExternalAlertNotificationState{NotificationCount: 1},
			wantCount:  2,
		},
		{
			entityName: "reminder-count-one-activation",
			title:      "reminder scenario count one with activation",
			state: ExternalAlertNotificationState{
				NotificationCount:    1,
				LastActivationTime:   activationThreeHoursAgo.UnixMilli(),
				LastNotificationTime: now.Add(-2 * time.Hour).UnixMilli(),
			},
			wantCount: 2,
		},
		{
			entityName: "reminder-count-two-prev-window",
			title:      "reminder scenario count two previous window",
			state: ExternalAlertNotificationState{
				NotificationCount:    2,
				LastActivationTime:   activationInWindow.UnixMilli(),
				LastNotificationTime: lastNotificationPreviousWindow.UnixMilli(),
			},
			wantCount: 3,
		},
		{
			entityName: "reminder-skip-same-window-count-three",
			title:      "reminder scenario skip same window count three",
			state: ExternalAlertNotificationState{
				NotificationCount:    3,
				LastActivationTime:   activationInWindow.UnixMilli(),
				LastNotificationTime: lastNotificationCurrentWindow.UnixMilli(),
			},
			skipReason: reminderSkipSameWindowReason,
		},
		{
			entityName: "reminder-skip-same-window-count-five",
			title:      "reminder scenario skip same window count five",
			state: ExternalAlertNotificationState{
				NotificationCount:    5,
				LastActivationTime:   activationInWindow.UnixMilli(),
				LastNotificationTime: now.Add(-30 * time.Minute).UnixMilli(),
			},
			skipReason: reminderSkipSameWindowReason,
		},
		{
			entityName: "reminder-skip-duration-ended",
			title:      "reminder scenario skip duration ended",
			state: ExternalAlertNotificationState{
				NotificationCount:    5,
				LastActivationTime:   activationExpired.UnixMilli(),
				LastNotificationTime: lastNotificationDurationEnded.UnixMilli(),
			},
			skipReason: reminderSkipDurationReason,
		},
	}

	type reminderAlert struct {
		id         string
		entityName string
		wantCount  int
		skipReason string
	}

	alerts := make([]reminderAlert, 0, len(scenarios))
	logCaptures := make([]string, 0, 3)
	for _, scenario := range scenarios {
		entityID := uuid.NewString()
		alert, err := BuildExternalAlertWithTitle(tenantID, entityID, scenario.entityName, scenario.title)
		if err != nil {
			t.Fatalf("build alert %q: %v", scenario.entityName, err)
		}
		IndexExternalAlertWithState(ctx, t, openSearch, alert, observedAt, scenario.state)

		if scenario.skipReason != "" {
			logCaptures = append(logCaptures, alertIDLogMarker(alert.Id))
		}

		alerts = append(alerts, reminderAlert{
			id:         alert.Id,
			entityName: scenario.entityName,
			wantCount:  scenario.wantCount,
			skipReason: scenario.skipReason,
		})
	}

	jobStartedAt := time.Now().UTC().Add(-1 * time.Minute).UnixMilli()
	result := runNotificationsForAlertsJob(t, ctx, tg, job, logCaptures...)

	kafka := job.GetKafka(t)
	expectedEmailSubject := fmt.Sprintf(
		"DataBahn.ai Alert - %s - configuration processing failure",
		tenantName,
	)
	emailMessage := WaitForKafkaMessage(t, kafka, EmailNotificationTopic, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	}, notificationJobPollRate, notificationJobTimeout)
	AssertKafkaMessageJSONPath(t, emailMessage, "$.subject", expectedEmailSubject)

	for _, alert := range alerts {
		if alert.skipReason != "" {
			AssertKafkaEmailBodyEntityAbsent(t, emailMessage, alert.entityName)
			continue
		}
		if alert.wantCount == 1 {
			AssertKafkaEmailBodyEntityInSection(t, emailMessage, "New Alerts", "Reminder Alerts", alert.entityName)
			AssertKafkaEmailBodyEntityAbsentFromSection(t, emailMessage, "Reminder Alerts", "", alert.entityName)
			continue
		}
		AssertKafkaEmailBodyEntityInSection(t, emailMessage, "Reminder Alerts", "", alert.entityName)
		AssertKafkaEmailBodyEntityAbsentFromSection(t, emailMessage, "New Alerts", "Reminder Alerts", alert.entityName)
		AssertKafkaEmailBodyReminderNumberInSection(t, emailMessage, "Reminder Alerts", "", alert.entityName, alert.wantCount)
	}

	skipCaptureIndex := 0
	for _, alert := range alerts {
		if alert.skipReason != "" {
			assertCapturedJobLogContains(t, tg, result.Logger, result.LogCaptureIDs[skipCaptureIndex], alert.skipReason)
			skipCaptureIndex++
			AssertNoNotificationSent(t, kafka, alert.id)
			continue
		}

		notificationSentMessage := WaitForKafkaMessage(t, kafka, AlertIndexingTopic, func(message dbkafka.Message) bool {
			return pramaan.GetHeader(&message, "action") == "notification_sent" &&
				KafkaBodyJSONPathEquals(message, "$.id", alert.id)
		}, notificationJobPollRate, notificationJobTimeout)
		assertNotificationSentMessage(t, notificationSentMessage, alert.id, alert.wantCount, jobStartedAt)
	}

	AssertKafkaMessageCount(t, kafka, EmailNotificationTopic, 1, func(message dbkafka.Message) bool {
		return KafkaBodyJSONPathEquals(message, "$.subject", expectedEmailSubject)
	})
}

func runNotificationsForAlertsJob(
	t *testing.T,
	ctx context.Context,
	tg pramaan.TestLogger,
	job *pramaan.JobPramaan,
	logCaptures ...string,
) *pramaan.JobRunResult {
	t.Helper()
	return job.Run(ctx, tg, pramaan.JobRunOptions{
		Cmd:         []string{"-job", "notifications-for-alerts"},
		LogCaptures: logCaptures,
	})
}

func alertIDLogMarker(alertID string) string {
	return `"alertId":"` + alertID + `"`
}

func assertCapturedJobLogContains(
	t *testing.T,
	tg pramaan.TestLogger,
	logger *pramaan.ServiceLogger,
	captureID string,
	want string,
) {
	t.Helper()

	log := logger.WaitForCapturedLog(tg, captureID, notificationJobPollRate, notificationJobTimeout)
	if !strings.Contains(*log, want) {
		t.Fatalf("captured log does not contain %q:\n%s", want, *log)
	}
}
