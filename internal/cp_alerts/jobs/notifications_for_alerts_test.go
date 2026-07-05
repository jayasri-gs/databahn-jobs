package jobs

import (
	"testing"
	"time"

	"github.com/databahn-ai/db-models/alerts_async"
)

var utc = time.UTC

func TestNewCustomerNotificationReminderConfig(t *testing.T) {
	tests := []struct {
		name         string
		firstSends   string
		frequencyEnv string
		durationEnv  string
		wantFirst    int
		wantInterval time.Duration
		wantDuration time.Duration
	}{
		{
			name:         "defaults when env unset",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "valid custom values",
			firstSends:   "5",
			frequencyEnv: "2h",
			durationEnv:  "48h",
			wantFirst:    5,
			wantInterval: 2 * time.Hour,
			wantDuration: 48 * time.Hour,
		},
		{
			name:         "duration with day suffix",
			frequencyEnv: "24h",
			durationEnv:  "7d",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: 24 * time.Hour,
			wantDuration: 7 * 24 * time.Hour,
		},
		{
			name:         "invalid first sends zero uses default",
			firstSends:   "0",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "invalid first sends negative uses default",
			firstSends:   "-2",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "reminder interval below minimum uses default interval",
			frequencyEnv: "30m",
			durationEnv:  "168h",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "reminder duration less than interval resets both to defaults",
			frequencyEnv: "48h",
			durationEnv:  "24h",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "reminder duration equal to interval resets both to defaults",
			frequencyEnv: "24h",
			durationEnv:  "24h",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "invalid reminder frequency uses default frequency",
			frequencyEnv: "not-a-duration",
			durationEnv:  "168h",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: DefaultCustomerNotificationReminderNotificationFrequencyEvery,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "invalid reminder duration uses default duration",
			frequencyEnv: "24h",
			durationEnv:  "bad",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: 24 * time.Hour,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
		{
			name:         "non positive reminder duration uses default duration",
			frequencyEnv: "24h",
			durationEnv:  "0s",
			wantFirst:    DefaultCustomerNotificationSendFirstAtJobFrequency,
			wantInterval: 24 * time.Hour,
			wantDuration: DefaultCustomerNotificationReminderNotificationEndDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CUSTOMER_NOTIFICATION_FIRST_SENDS", tt.firstSends)
			t.Setenv("CUSTOMER_NOTIFICATION_REMINDER_FREQUENCY_EVERY", tt.frequencyEnv)
			t.Setenv("CUSTOMER_NOTIFICATION_REMINDER_END_DURATION", tt.durationEnv)

			config := NewCustomerNotificationReminderConfig()
			assertCustomerNotificationReminderConfig(t, config, tt.wantFirst, tt.wantInterval, tt.wantDuration)
		})
	}
}

func assertCustomerNotificationReminderConfig(
	t *testing.T,
	config *CustomerNotificationReminderConfig,
	wantFirst int,
	wantInterval time.Duration,
	wantDuration time.Duration,
) {
	t.Helper()

	if config.sendFirstNotifications != wantFirst {
		t.Fatalf("sendFirstNotifications = %d, want %d", config.sendFirstNotifications, wantFirst)
	}
	if config.reminderInterval != wantInterval {
		t.Fatalf("reminderInterval = %v, want %v", config.reminderInterval, wantInterval)
	}
	if config.reminderDuration != wantDuration {
		t.Fatalf("reminderDuration = %v, want %v", config.reminderDuration, wantDuration)
	}
}

func TestCustomerNotificationReminderConfig_CheckSendingNotification_phaseOne(t *testing.T) {
	config := &CustomerNotificationReminderConfig{
		sendFirstNotifications: 3,
		reminderInterval:       time.Hour,
		reminderDuration:       12 * time.Hour,
	}

	tests := []struct {
		name              string
		notificationsSent int
		wantFirst         bool
		wantReminder      bool
	}{
		{name: "first notification", notificationsSent: 0, wantFirst: true},
		{name: "second notification", notificationsSent: 1, wantReminder: true},
		{name: "third notification", notificationsSent: 2, wantReminder: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := config.CheckSendingNotification(0, 0, tt.notificationsSent, 0)
			assertReminderDecision(t, decision, tt.wantFirst, tt.wantReminder, "")
		})
	}
}

func TestCustomerNotificationReminderConfig_CheckSendingNotification_phaseTwo(t *testing.T) {
	activation := time.Date(2026, 6, 1, 14, 0, 0, 0, utc)
	activationMillis := activation.UnixMilli()
	config := &CustomerNotificationReminderConfig{
		sendFirstNotifications: 3,
		reminderInterval:       2 * time.Hour,
		reminderDuration:       12 * time.Hour,
	}

	tests := []struct {
		name                 string
		activationMillis     int64
		now                  time.Time
		notificationsSent    int
		lastNotificationTime int64
		wantFirst            bool
		wantReminder         bool
		wantReason           string
	}{
		{
			name:              "phase one first notification before interval logic",
			activationMillis:  activationMillis,
			now:               time.Date(2026, 6, 1, 18, 0, 0, 0, utc),
			notificationsSent: 0,
			wantFirst:         true,
		},
		{
			name:                 "first phase two skipped when phase one finished in same slot",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 14, 30, 0, 0, utc),
			notificationsSent:    3,
			lastNotificationTime: time.Date(2026, 6, 1, 14, 25, 0, 0, utc).UnixMilli(),
			wantReason:           "reminder already sent for current window",
		},
		{
			name:                 "first phase two in new slot after phase one",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 16, 0, 0, 0, utc),
			notificationsSent:    3,
			lastNotificationTime: time.Date(2026, 6, 1, 14, 25, 0, 0, utc).UnixMilli(),
			wantReminder:         true,
		},
		{
			name:                 "already sent for current interval",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 14, 30, 0, 0, utc),
			notificationsSent:    4,
			lastNotificationTime: time.Date(2026, 6, 1, 14, 15, 0, 0, utc).UnixMilli(),
			wantReason:           "reminder already sent for current window",
		},
		{
			name:                 "second interval reminder in new slot",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 16, 0, 0, 0, utc),
			notificationsSent:    4,
			lastNotificationTime: time.Date(2026, 6, 1, 14, 30, 0, 0, utc).UnixMilli(),
			wantReminder:         true,
		},
		{
			name:                 "phase two reminder after slow phase one in later slot",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 18, 0, 0, 0, utc),
			notificationsSent:    3,
			lastNotificationTime: time.Date(2026, 6, 1, 15, 30, 0, 0, utc).UnixMilli(),
			wantReminder:         true,
		},
		{
			name:                 "multiple job runs in same slot are skipped",
			activationMillis:     activationMillis,
			now:                  time.Date(2026, 6, 1, 18, 30, 0, 0, utc),
			notificationsSent:    4,
			lastNotificationTime: time.Date(2026, 6, 1, 18, 10, 0, 0, utc).UnixMilli(),
			wantReason:           "reminder already sent for current window",
		},
		{
			name:              "after duration elapsed",
			activationMillis:  activationMillis,
			now:               time.Date(2026, 6, 2, 2, 0, 0, 0, utc),
			notificationsSent: 8,
			wantReason:        "reminder duration has elapsed",
		},
		{
			name:              "activation required in phase two",
			activationMillis:  0,
			now:               time.Date(2026, 6, 1, 16, 0, 0, 0, utc),
			notificationsSent: 3,
			wantReason:        "activation time is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := config.CheckSendingNotification(
				tt.activationMillis,
				tt.now.UnixMilli(),
				tt.notificationsSent,
				tt.lastNotificationTime,
			)
			assertReminderDecision(t, decision, tt.wantFirst, tt.wantReminder, tt.wantReason)
		})
	}
}

func Test_reminderSlotIndex(t *testing.T) {
	activation := time.Date(2026, 6, 1, 14, 0, 0, 0, utc)
	activationMillis := activation.UnixMilli()
	frequency := 2 * time.Hour
	duration := 12 * time.Hour

	tests := []struct {
		name     string
		now      time.Time
		wantOK   bool
		wantSlot int
	}{
		{name: "slot zero", now: time.Date(2026, 6, 1, 14, 30, 0, 0, utc), wantOK: true, wantSlot: 0},
		{name: "slot one boundary", now: time.Date(2026, 6, 1, 16, 0, 0, 0, utc), wantOK: true, wantSlot: 1},
		{name: "before activation", now: time.Date(2026, 6, 1, 13, 0, 0, 0, utc), wantOK: false},
		{name: "after duration", now: time.Date(2026, 6, 2, 2, 0, 0, 0, utc), wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, ok := reminderSlotIndex(activationMillis, tt.now.UnixMilli(), frequency, duration)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && slot != tt.wantSlot {
				t.Fatalf("slot = %d, want %d", slot, tt.wantSlot)
			}
		})
	}
}

type notificationScenarioState struct {
	count                int
	lastNotificationTime int64
}

type notificationScenarioStep struct {
	name         string
	now          time.Time
	wantFirst    bool
	wantReminder bool
	wantReason   string
}

func (s *notificationScenarioState) applySend(now time.Time, decision NotificationReminderDecision) {
	if decision.SendFirstNotification || decision.SentReminder {
		s.count++
		s.lastNotificationTime = now.UnixMilli()
	}
}

func runNotificationScenario(
	t *testing.T,
	config *CustomerNotificationReminderConfig,
	activation time.Time,
	steps []notificationScenarioStep,
) {
	t.Helper()

	activationMillis := activation.UnixMilli()
	state := notificationScenarioState{}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			decision := config.CheckSendingNotification(
				activationMillis,
				step.now.UnixMilli(),
				state.count,
				state.lastNotificationTime,
			)
			assertReminderDecision(t, decision, step.wantFirst, step.wantReminder, step.wantReason)
			state.applySend(step.now, decision)
		})
	}
}

func TestCustomerNotificationReminderConfig_oneHourWindowFrequency(t *testing.T) {
	activation := time.Date(2026, 6, 1, 14, 0, 0, 0, utc)
	config := &CustomerNotificationReminderConfig{
		sendFirstNotifications: 3,
		reminderInterval:       time.Hour,
		reminderDuration:       6 * time.Hour,
	}

	runNotificationScenario(t, config, activation, []notificationScenarioStep{
		{name: "hour 0 new", now: time.Date(2026, 6, 1, 14, 10, 0, 0, utc), wantFirst: true},
		{name: "hour 0 reminder 1", now: time.Date(2026, 6, 1, 14, 20, 0, 0, utc), wantReminder: true},
		{name: "hour 0 reminder 2", now: time.Date(2026, 6, 1, 14, 40, 0, 0, utc), wantReminder: true},
		{name: "hour 0 phase two deferred to next slot", now: time.Date(2026, 6, 1, 14, 50, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "hour 0 duplicate job run skipped", now: time.Date(2026, 6, 1, 14, 55, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "hour 1 reminder", now: time.Date(2026, 6, 1, 15, 10, 0, 0, utc), wantReminder: true},
		{name: "hour 1 duplicate job run skipped", now: time.Date(2026, 6, 1, 15, 45, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "hour 2 reminder", now: time.Date(2026, 6, 1, 16, 5, 0, 0, utc), wantReminder: true},
		{name: "hour 3 reminder", now: time.Date(2026, 6, 1, 17, 5, 0, 0, utc), wantReminder: true},
		{name: "after duration elapsed", now: time.Date(2026, 6, 1, 20, 30, 0, 0, utc), wantReason: "reminder duration has elapsed"},
	})
}

func TestCustomerNotificationReminderConfig_delayedFirstThreeNotifications(t *testing.T) {
	activation := time.Date(2026, 6, 1, 14, 0, 0, 0, utc)
	config := &CustomerNotificationReminderConfig{
		sendFirstNotifications: 3,
		reminderInterval:       time.Hour,
		reminderDuration:       12 * time.Hour,
	}

	runNotificationScenario(t, config, activation, []notificationScenarioStep{
		{name: "delayed new at 14:30", now: time.Date(2026, 6, 1, 14, 30, 0, 0, utc), wantFirst: true},
		{name: "delayed reminder at 15:00", now: time.Date(2026, 6, 1, 15, 0, 0, 0, utc), wantReminder: true},
		{name: "delayed reminder at 15:30", now: time.Date(2026, 6, 1, 15, 30, 0, 0, utc), wantReminder: true},
		{name: "phase two in hour 2 after slow phase one", now: time.Date(2026, 6, 1, 16, 10, 0, 0, utc), wantReminder: true},
		{name: "hour 2 duplicate job run skipped", now: time.Date(2026, 6, 1, 16, 25, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "hour 3 reminder", now: time.Date(2026, 6, 1, 17, 15, 0, 0, utc), wantReminder: true},
		{name: "hour 3 duplicate job run skipped", now: time.Date(2026, 6, 1, 17, 40, 0, 0, utc), wantReason: "reminder already sent for current window"},
	})
}

func TestCustomerNotificationReminderConfig_fasterPhaseOneNotifications(t *testing.T) {
	activation := time.Date(2026, 6, 1, 14, 0, 0, 0, utc)
	config := &CustomerNotificationReminderConfig{
		sendFirstNotifications: 3,
		reminderInterval:       time.Hour,
		reminderDuration:       12 * time.Hour,
	}

	runNotificationScenario(t, config, activation, []notificationScenarioStep{
		{name: "fast new at 14:05", now: time.Date(2026, 6, 1, 14, 5, 0, 0, utc), wantFirst: true},
		{name: "fast reminder at 14:15", now: time.Date(2026, 6, 1, 14, 15, 0, 0, utc), wantReminder: true},
		{name: "fast reminder at 14:25", now: time.Date(2026, 6, 1, 14, 25, 0, 0, utc), wantReminder: true},
		{name: "first phase two deferred to next slot", now: time.Date(2026, 6, 1, 14, 35, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "same hour duplicate job run skipped", now: time.Date(2026, 6, 1, 14, 50, 0, 0, utc), wantReason: "reminder already sent for current window"},
		{name: "next hour reminder", now: time.Date(2026, 6, 1, 15, 10, 0, 0, utc), wantReminder: true},
		{name: "next hour duplicate job run skipped", now: time.Date(2026, 6, 1, 15, 40, 0, 0, utc), wantReason: "reminder already sent for current window"},
	})
}

func assertReminderDecision(
	t *testing.T,
	decision NotificationReminderDecision,
	wantFirst bool,
	wantReminder bool,
	wantReason string,
) {
	t.Helper()

	if decision.SendFirstNotification != wantFirst {
		t.Fatalf("SendFirstNotification = %v, want %v", decision.SendFirstNotification, wantFirst)
	}
	if decision.SentReminder != wantReminder {
		t.Fatalf("SentReminder = %v, want %v", decision.SentReminder, wantReminder)
	}
	if wantReason != "" && decision.ReasonToNotSend != wantReason {
		t.Fatalf("ReasonToNotSend = %q, want %q", decision.ReasonToNotSend, wantReason)
	}
	if wantReason == "" && decision.ReasonToNotSend != "" {
		t.Fatalf("ReasonToNotSend = %q, want empty", decision.ReasonToNotSend)
	}
	if (wantFirst || wantReminder) && decision.ReasonToNotSend != "" {
		t.Fatalf("expected send decision, got reason %q", decision.ReasonToNotSend)
	}
}

func TestActivationTimeForNotification(t *testing.T) {
	activation := time.Date(2026, 6, 1, 10, 0, 0, 0, utc).UnixMilli()
	firstObserved := time.Date(2026, 6, 1, 8, 0, 0, 0, utc).UnixMilli()
	lastObserved := time.Date(2026, 6, 1, 12, 0, 0, 0, utc).UnixMilli()

	tests := []struct {
		name  string
		alert alerts_async.Alert
		want  int64
	}{
		{
			name:  "uses last activation time when set",
			alert: alerts_async.Alert{LastActivationTime: activation, FirstObservedAt: firstObserved},
			want:  activation,
		},
		{
			name:  "falls back to last observed when activation missing",
			alert: alerts_async.Alert{FirstObservedAt: firstObserved, LastObservedAt: lastObserved},
			want:  lastObserved,
		},
		{
			name:  "ignores first observed when last observed missing",
			alert: alerts_async.Alert{FirstObservedAt: firstObserved},
			want:  0,
		},
		{
			name:  "falls back to last observed only",
			alert: alerts_async.Alert{LastObservedAt: lastObserved},
			want:  lastObserved,
		},
		{
			name:  "returns zero when no timestamps",
			alert: alerts_async.Alert{},
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := activationTimeForNotification(tt.alert); got != tt.want {
				t.Fatalf("activationTimeForNotification() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEmailTemplatePathForAlerts(t *testing.T) {
	alert := func(criticality string) alerts_async.Alert {
		return alerts_async.Alert{Criticality: criticality}
	}

	tests := []struct {
		name     string
		alerts   *AlertsForNotification
		wantPath string
	}{
		{
			name:     "green when all info",
			alerts:   &AlertsForNotification{newAlerts: []alerts_async.Alert{alert(alerts_async.Info.String())}},
			wantPath: EmailTemplatesBasePath + "green_alert.html",
		},
		{
			name: "warning when any warning and no critical or severe",
			alerts: &AlertsForNotification{
				newAlerts:      []alerts_async.Alert{alert(alerts_async.Info.String())},
				reminderAlerts: []alerts_async.Alert{alert(alerts_async.Warning.String())},
			},
			wantPath: EmailTemplatesBasePath + "warning_alert.html",
		},
		{
			name: "critical template when any severe",
			alerts: &AlertsForNotification{
				newAlerts:      []alerts_async.Alert{alert(alerts_async.Info.String())},
				reminderAlerts: []alerts_async.Alert{alert(alerts_async.Sever.String())},
			},
			wantPath: EmailTemplatesBasePath + "error_alert.html",
		},
		{
			name: "critical template when any critical even with warning first",
			alerts: &AlertsForNotification{
				newAlerts: []alerts_async.Alert{
					alert(alerts_async.Warning.String()),
					alert(alerts_async.Critical.String()),
				},
			},
			wantPath: EmailTemplatesBasePath + "error_alert.html",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := emailTemplatePathForAlerts(tt.alerts); got != tt.wantPath {
				t.Fatalf("emailTemplatePathForAlerts() = %q, want %q", got, tt.wantPath)
			}
		})
	}
}

func TestNotificationSentUpdateBackfillsActivationFromLastObservedAt(t *testing.T) {
	lastObserved := time.Date(2026, 6, 1, 12, 0, 0, 0, utc).UnixMilli()
	firstObserved := time.Date(2026, 6, 1, 8, 0, 0, 0, utc).UnixMilli()
	now := time.Date(2026, 6, 1, 13, 0, 0, 0, utc).UnixMilli()

	alert := alerts_async.Alert{
		FirstObservedAt:   firstObserved,
		LastObservedAt:    lastObserved,
		NotificationCount: 0,
	}
	update := notificationSentUpdate(alert, now)

	if update.NotificationCount != 1 {
		t.Fatalf("NotificationCount = %d, want 1", update.NotificationCount)
	}
	if update.LastNotificationTime != now {
		t.Fatalf("LastNotificationTime = %d, want %d", update.LastNotificationTime, now)
	}
	if update.LastActivationTime != lastObserved {
		t.Fatalf("LastActivationTime = %d, want %d (lastObservedAt)", update.LastActivationTime, lastObserved)
	}
}

func TestNotificationSentUpdatePreservesExistingActivationTime(t *testing.T) {
	activation := time.Date(2026, 6, 1, 10, 0, 0, 0, utc).UnixMilli()
	lastObserved := time.Date(2026, 6, 1, 12, 0, 0, 0, utc).UnixMilli()
	now := time.Date(2026, 6, 1, 13, 0, 0, 0, utc).UnixMilli()

	alert := alerts_async.Alert{
		LastActivationTime: activation,
		LastObservedAt:     lastObserved,
		NotificationCount:  2,
	}
	update := notificationSentUpdate(alert, now)

	if update.LastActivationTime != 0 {
		t.Fatalf("LastActivationTime = %d, want 0 when activation already set", update.LastActivationTime)
	}
	if update.NotificationCount != 3 {
		t.Fatalf("NotificationCount = %d, want 3", update.NotificationCount)
	}
}

func TestAlertsToReminderEmailDetailsSetsNotificationNumber(t *testing.T) {
	observedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, utc).UnixMilli()
	alerts := []alerts_async.Alert{
		{
			FunctionalityEntityName: "entity-second-notification",
			Title:                   "reminder title",
			Message:                 "reminder message",
			LastObservedAt:          observedAt,
			NotificationCount:       1,
		},
		{
			FunctionalityEntityName: "entity-third-notification",
			Title:                   "reminder title",
			Message:                 "reminder message",
			LastObservedAt:          observedAt,
			NotificationCount:       2,
		},
	}

	details := alertsToReminderEmailDetails(alerts)
	if len(details) != 2 {
		t.Fatalf("len(details) = %d, want 2", len(details))
	}
	if details[0].ReminderNumber != 1 {
		t.Fatalf("details[0].ReminderNumber = %d, want 1", details[0].ReminderNumber)
	}
	if details[1].ReminderNumber != 2 {
		t.Fatalf("details[1].ReminderNumber = %d, want 2", details[1].ReminderNumber)
	}
	if details[0].Title != "Reminder Title" {
		t.Fatalf("details[0].Title = %q, want %q", details[0].Title, "Reminder Title")
	}
}

func TestFormatObservedAtForEmailUsesLastObservedAt(t *testing.T) {
	firstObserved := time.Date(2026, 6, 1, 8, 0, 0, 0, utc).UnixMilli()
	lastObserved := time.Date(2026, 6, 1, 12, 0, 0, 0, utc).UnixMilli()

	got := formatObservedAtForEmail(alerts_async.Alert{
		FirstObservedAt: firstObserved,
		LastObservedAt:  lastObserved,
	})
	want := time.UnixMilli(lastObserved).Format(time.RFC3339)
	if got != want {
		t.Fatalf("formatObservedAtForEmail() = %q, want %q", got, want)
	}
}

func TestFormatObservedAtForEmailFallsBackToFirstObservedAt(t *testing.T) {
	firstObserved := time.Date(2026, 6, 1, 8, 0, 0, 0, utc).UnixMilli()

	got := formatObservedAtForEmail(alerts_async.Alert{FirstObservedAt: firstObserved})
	want := time.UnixMilli(firstObserved).Format(time.RFC3339)
	if got != want {
		t.Fatalf("formatObservedAtForEmail() = %q, want %q", got, want)
	}
}

func TestAlertsToReminderEmailDetailsSortsByReminderNumberIncreasing(t *testing.T) {
	tests := []struct {
		functionalityType string
		want              string
	}{
		{alerts_async.IngestionChecker.String(), "No New Data Ingested"},
		{alerts_async.DeliveryChecker.String(), "No Data Delivered"},
		{"configuration_processing_failure", "Configuration Processing Failure"},
	}

	for _, tt := range tests {
		t.Run(tt.functionalityType, func(t *testing.T) {
			if got := buildEmailTitle(tt.functionalityType); got != tt.want {
				t.Fatalf("buildEmailTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}
