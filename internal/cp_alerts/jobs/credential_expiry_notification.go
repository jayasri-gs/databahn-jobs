package jobs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"text/template"
	"time"

	notification_common "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	"github.com/databahn-ai/databahn-jobs/internal/store/secrets"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CredentialExpiryModule is the notifications module tenants must enable to receive
// credential expiry emails. Seeded by backend-service Liquibase alongside the column.
const CredentialExpiryModule = "CREDENTIAL"

// credentialExpiryTemplateFile is copied into /home/databahn/templates by the Dockerfile.
const credentialExpiryTemplateFile = "credential_expiry.html"

// SendCredentialExpiryNotifications is the entry point wired into cmd/jobs.go for the
// "credential-expiry-notification" job. Runs every 3h (see charts-version-control).
//
// Flow:
//  1. Fetch secrets whose triggers are due (indexed EXISTS join on log_source / destination).
//  2. Group by tenant.
//  3. For each tenant, always dispatch the product (in-app) alert channel. The email
//     channel is dispatched only when the CREDENTIAL module has at least one email
//     target configured; otherwise it is recorded as SKIPPED for the trigger.
//  4. Persist trigger state (SENT / FAILED / SKIPPED) inside a transaction that re-locks
//     the row with FOR UPDATE SKIP LOCKED.
func SendCredentialExpiryNotifications(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := config.GetDB()

	notificationManager, err := notification.NewNotificationManager(ctx)
	if err != nil {
		return common.NewJobResultFromErrors([]common.JobError{{Message: fmt.Sprintf("error creating notification manager: %v", err)}})
	}
	defer notificationManager.Close(ctx)

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		return common.NewJobResultFromErrors([]common.JobError{{Message: fmt.Sprintf("error creating alerts manager: %v", err)}})
	}
	defer alertsManager.Close(ctx)

	channels := buildChannels(notificationManager, alertsManager)

	due, err := secrets.FetchDueCredentials(ctx, db)
	if err != nil {
		return common.NewJobResultFromErrors([]common.JobError{{Message: fmt.Sprintf("error fetching due credentials: %v", err)}})
	}
	if len(due) == 0 {
		logger.GetLogger().Info("no credential secrets due for expiry notification")
		return common.NewJobResultSuccess()
	}

	byTenant := groupByTenant(due)
	today := time.Now().UTC().Truncate(24 * time.Hour)

	for tenantId, tenantSecrets := range byTenant {
		targets, tErr := entities.GetTargetsForModule(db, tenantId, CredentialExpiryModule)
		if tErr != nil {
			jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to load targets for tenant %s: %v", tenantId, tErr)})
			logger.GetLogger().Error("failed to load targets", zap.Error(tErr), zap.String("tenant", tenantId.String()))
			continue
		}
		if len(targets) == 0 {
			// Module not enabled or no email targets attached — product (in-app) alerts
			// still fire so users see the expiry in the Alerts UI; email is skipped.
			logger.GetLogger().Info("no CREDENTIAL targets, product alerts only",
				zap.String("tenant", tenantId.String()),
				zap.Int("secrets", len(tenantSecrets)))
		}

		if err := processTenant(ctx, db, tenantId, tenantSecrets, targets, channels, today); err != nil {
			jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("tenant %s processing failed: %v", tenantId, err)})
			logger.GetLogger().Error("tenant processing failed", zap.Error(err), zap.String("tenant", tenantId.String()))
		}
	}

	if len(jobErrors) == 0 {
		return common.NewJobResultSuccess()
	}
	return common.NewJobResultFromErrors(jobErrors)
}

// processTenant batches due triggers per threshold and dispatches once through every
// configured channel. Trigger state is written back per-secret in its own transaction
// with FOR UPDATE SKIP LOCKED so a concurrent CRUD update on the same row is either
// waited on or skipped by another job replica.
func processTenant(
	ctx context.Context,
	db *gorm.DB,
	tenantId uuid.UUID,
	tenantSecrets []secrets.Secret,
	targets []entities.Targets,
	channels []NotificationChannel,
	today time.Time,
) error {
	// Group secret+trigger pairs by days-threshold so we send one email per bucket.
	bucketsByDays := map[int][]expiryDispatchEntry{}
	for _, sec := range tenantSecrets {
		series, err := DecodeSeries(sec.NotificationTriggers)
		if err != nil {
			logger.GetLogger().Error("failed to decode notification_triggers", zap.Error(err), zap.String("secret", sec.Id.String()))
			continue
		}
		if series == nil {
			continue
		}
		for _, idx := range DueTriggers(series, today) {
			t := series.Triggers[idx]
			bucketsByDays[t.Days] = append(bucketsByDays[t.Days], expiryDispatchEntry{
				secret:  sec,
				series:  series,
				trigger: t,
				triggerIndex: idx,
			})
		}
	}

	if len(bucketsByDays) == 0 {
		return nil
	}

	// Deterministic ordering: EXPIRED (0), then 1, 3, 7, 15.
	dayKeys := make([]int, 0, len(bucketsByDays))
	for d := range bucketsByDays {
		dayKeys = append(dayKeys, d)
	}
	sort.Ints(dayKeys)

	databahnTargets := toDatabahnTargets(tenantId, targets)
	hasEmailTargets := len(databahnTargets) > 0

	for _, days := range dayKeys {
		batch := bucketsByDays[days]
		batchPayload := NotificationBatch{
			TenantId: tenantId,
			Days:     days,
			Kind:     kindForDays(days),
			Entries:  toBatchEntries(ctx, db, batch, today),
			Targets:  databahnTargets,
		}

		// Fan out to every enabled channel. The email channel requires configured
		// targets; if none exist it is recorded as SKIPPED and the product channel
		// still fires so the alert surfaces in the UI. One failing channel does not
		// abort the others.
		channelResults := map[string]error{}
		channelSkipped := map[string]bool{}
		for _, ch := range channels {
			if ch.Name() == channelNameEmail && !hasEmailTargets {
				channelSkipped[ch.Name()] = true
				channelResults[ch.Name()] = nil
				continue
			}
			err := ch.Send(ctx, batchPayload)
			channelResults[ch.Name()] = err
			if err != nil {
				logger.GetLogger().Error("notification channel send failed",
					zap.String("channel", ch.Name()),
					zap.String("tenant", tenantId.String()),
					zap.Int("days", days),
					zap.Error(err))
			}
		}

		// Persist per-secret trigger state.
		for _, entry := range batch {
			if err := persistTriggerOutcome(ctx, db, entry, channels, channelResults, channelSkipped, today); err != nil {
				logger.GetLogger().Error("failed to persist trigger state",
					zap.Error(err),
					zap.String("secret", entry.secret.Id.String()),
					zap.Int("days", days))
			}
		}
	}
	return nil
}

// persistTriggerOutcome reloads the row with FOR UPDATE SKIP LOCKED, applies the trigger
// state change on the freshest jsonb, and writes it back. Skips silently when another
// writer holds the row (concurrent CRUD update); the next job run will pick it up again.
func persistTriggerOutcome(
	ctx context.Context,
	db *gorm.DB,
	entry expiryDispatchEntry,
	channels []NotificationChannel,
	channelResults map[string]error,
	channelSkipped map[string]bool,
	today time.Time,
) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var current secrets.Secret
		err := tx.WithContext(ctx).
			Raw(`SELECT notification_triggers FROM secrets WHERE id = ? FOR UPDATE SKIP LOCKED`, entry.secret.Id).
			Scan(&current).Error
		if err != nil {
			return err
		}
		// If nothing was locked (row taken by another writer), skip this cycle.
		if len(current.NotificationTriggers) == 0 {
			return nil
		}

		series, err := DecodeSeries(current.NotificationTriggers)
		if err != nil || series == nil {
			return err
		}
		// Locate the same trigger by days (index may have shifted if series was rewritten).
		targetIdx := -1
		for i, t := range series.Triggers {
			if t.Days == entry.trigger.Days {
				targetIdx = i
				break
			}
		}
		if targetIdx < 0 {
			// Series was reset while the job was mid-flight; nothing to update.
			return nil
		}

		// Any dispatched channel success -> mark trigger SENT so we do not re-notify the
		// same threshold. Skipped channels (e.g. email when no targets configured) do not
		// count as success or failure; if every attempted channel failed the trigger is
		// FAILED (auto-retry) or SKIPPED after max attempts.
		anySuccess := false
		anyAttempted := false
		for _, ch := range channels {
			if channelSkipped[ch.Name()] {
				RecordChannel(series, targetIdx, ch.Name(), CredExpiryStatusSkipped, today, nil)
				continue
			}
			anyAttempted = true
			chErr := channelResults[ch.Name()]
			status := CredExpiryStatusSent
			if chErr != nil {
				status = CredExpiryStatusFailed
			} else {
				anySuccess = true
			}
			RecordChannel(series, targetIdx, ch.Name(), status, today, chErr)
		}
		if anySuccess {
			MarkTriggerSent(series, targetIdx, today)
		} else if anyAttempted {
			MarkTriggerFailed(series, targetIdx, fmt.Errorf("all channels failed"))
		} else {
			// No channels attempted (all skipped) — trigger remains PENDING for retry.
			return nil
		}

		encoded, err := EncodeSeries(series)
		if err != nil {
			return err
		}
		return secrets.UpdateNotificationTriggers(ctx, tx, entry.secret.Id, encoded)
	})
}

// expiryDispatchEntry pairs a secret + its parsed series + the trigger being fired.
type expiryDispatchEntry struct {
	secret       secrets.Secret
	series       *CredExpirySeries
	trigger      CredExpiryTrigger
	triggerIndex int
}

// NotificationBatch is what a NotificationChannel consumes to deliver one bucket of
// per-threshold notifications for a single tenant.
type NotificationBatch struct {
	TenantId uuid.UUID
	Days     int
	Kind     string
	Entries  []NotificationBatchEntry
	Targets  []*notification_common.DatabahnTarget
}

// NotificationBatchEntry is a single-secret line item in a batch email.
type NotificationBatchEntry struct {
	SecretId           uuid.UUID
	Name               string
	Scope              string
	Vendor             string
	Device             string
	ExpiryDate         string
	DaysLeft           int
	IsExpired          bool
	LinkedSources      []string
	LinkedCollectors   []string
	LinkedDestinations []string
}

func groupByTenant(secretsList []secrets.Secret) map[uuid.UUID][]secrets.Secret {
	out := map[uuid.UUID][]secrets.Secret{}
	for _, s := range secretsList {
		out[s.TenantId] = append(out[s.TenantId], s)
	}
	return out
}

func toDatabahnTargets(tenantId uuid.UUID, targets []entities.Targets) []*notification_common.DatabahnTarget {
	out := make([]*notification_common.DatabahnTarget, 0, len(targets))
	for _, t := range targets {
		out = append(out, &notification_common.DatabahnTarget{
			TenantId: tenantId.String(),
			TargetId: t.ID.String(),
		})
	}
	return out
}

func toBatchEntries(ctx context.Context, db *gorm.DB, entries []expiryDispatchEntry, today time.Time) []NotificationBatchEntry {
	out := make([]NotificationBatchEntry, 0, len(entries))
	for _, e := range entries {
		expiry := ""
		daysLeft := 0
		isExpired := e.trigger.Kind == CredExpiryKindExpired
		if e.secret.ExpiryDate != nil {
			expiry = e.secret.ExpiryDate.UTC().Format(credExpiryDateLayout)
			hours := e.secret.ExpiryDate.UTC().Sub(today).Hours()
			daysLeft = int(hours / 24)
		}
		// Best-effort lookup — a DB error here should not block the notification.
		var linkedSources, linkedCollectors, linkedDests []string
		if linked, lErr := secrets.FetchLinkedEntities(ctx, db, e.secret.Id); lErr == nil {
			linkedSources = linked.Sources
			linkedCollectors = linked.Collectors
			linkedDests = linked.Destinations
		} else {
			logger.GetLogger().Warn("failed to load linked log sources / collectors / destinations",
				zap.Error(lErr),
				zap.String("secret", e.secret.Id.String()))
		}
		out = append(out, NotificationBatchEntry{
			SecretId:           e.secret.Id,
			Name:               e.secret.Name,
			Scope:              e.secret.Scope,
			Vendor:             e.secret.Vendor,
			Device:             e.secret.Device,
			ExpiryDate:         expiry,
			DaysLeft:           daysLeft,
			IsExpired:          isExpired,
			LinkedSources:      linkedSources,
			LinkedCollectors:   linkedCollectors,
			LinkedDestinations: linkedDests,
		})
	}
	return out
}

func kindForDays(days int) string {
	if days == 0 {
		return CredExpiryKindExpired
	}
	return CredExpiryKindWarning
}

// -----------------------------------------------------------------------------
// Channel strategy
// -----------------------------------------------------------------------------

// Channel names used in trigger.channels and for skip logic in processTenant.
const (
	channelNameEmail   = "email"
	channelNameProduct = "product"
)

// NotificationChannel is a pluggable dispatch target for credential-expiry notifications.
// Add new implementations (e.g. UI, Slack, OpsGenie) without changing job wiring or
// jsonb schema — channel names appear in trigger.channels for observability.
type NotificationChannel interface {
	Name() string
	Send(ctx context.Context, batch NotificationBatch) error
}

// buildChannels returns the ordered list of channels the job dispatches to. Email
// goes through the standard NotificationManager (Kafka -> notification-service ->
// SMTP). Product notifications are emitted as alerts on db.indexing.alerts so they
// surface in the customer UI at GET /alerts alongside every other operational alert.
//
// The credential functionality string is intentionally chosen so it does NOT match
// the CREDENTIAL notifications module in alertFunctionalityMatchesModuleName, which
// prevents notifications-for-alerts from re-emailing (customer email is our job's
// responsibility, dedup via notification_triggers jsonb).
func buildChannels(
	notificationManager *notification.NotificationManager,
	alertsManager *alert.AlertsManager,
) []NotificationChannel {
	return []NotificationChannel{
		newEmailChannel(notificationManager),
		newProductChannel(alertsManager),
	}
}

// -----------------------------------------------------------------------------
// Email channel
// -----------------------------------------------------------------------------

type emailChannel struct {
	mgr *notification.NotificationManager
}

func newEmailChannel(mgr *notification.NotificationManager) NotificationChannel {
	return &emailChannel{mgr: mgr}
}

func (e *emailChannel) Name() string { return channelNameEmail }

func (e *emailChannel) Send(ctx context.Context, batch NotificationBatch) error {
	if len(batch.Targets) == 0 {
		return nil
	}
	body, err := renderCredentialExpiryTemplate(batch)
	if err != nil {
		return err
	}
	subject := credentialExpirySubject(batch)
	return e.mgr.SendEmailNotification(notification_common.EmailNotificationRequest{
		Targets: batch.Targets,
		Subject: subject,
		Body:    body,
	})
}

func credentialExpirySubject(batch NotificationBatch) string {
	if batch.Kind == CredExpiryKindExpired {
		return fmt.Sprintf("DataBahn.ai Alert - %d credential(s) have expired", len(batch.Entries))
	}
	label := fmt.Sprintf("%d day", batch.Days)
	if batch.Days != 1 {
		label = fmt.Sprintf("%d days", batch.Days)
	}
	return fmt.Sprintf("DataBahn.ai Alert - %d credential(s) expiring in %s", len(batch.Entries), label)
}

func renderCredentialExpiryTemplate(batch NotificationBatch) (string, error) {
	path := EmailTemplatesBasePath + credentialExpiryTemplateFile
	tmpl, err := template.New(credentialExpiryTemplateFile).ParseFiles(path)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, batch); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// -----------------------------------------------------------------------------
// Product (in-app) channel
// -----------------------------------------------------------------------------

// Functionality string for credential-expiry alerts. Intentionally distinct from
// the CREDENTIAL notifications module name so notifications-for-alerts does not
// pick these up and double-email (see alertFunctionalityMatchesModuleName).
const (
	credentialFunctionality      = "credential_expiry"
	credentialFunctionalityType  = "credential_expiry_checker"
	credentialProductAlertAction = "Rotate the credential or extend its expiry via the Secrets Management page. " +
		"To change who receives these alerts, update the credential module targets under Notifications."
)

type productChannel struct {
	mgr *alert.AlertsManager
}

func newProductChannel(mgr *alert.AlertsManager) NotificationChannel {
	return &productChannel{mgr: mgr}
}

func (p *productChannel) Name() string { return channelNameProduct }

// Send emits one alert per secret in the batch so each credential appears as an
// individual, dismissible entry in the customer UI. Batching happens for email
// (single digest) but the product surface is per-entity by convention.
func (p *productChannel) Send(_ context.Context, batch NotificationBatch) error {
	alerts := make([]*alerts_async.Alert, 0, len(batch.Entries))
	now := time.Now().UTC().UnixMilli()
	for _, e := range batch.Entries {
		alerts = append(alerts, buildCredentialExpiryProductAlert(batch, e, now))
	}
	return p.mgr.SendAlerts(alerts)
}

// buildCredentialExpiryProductMessage assembles the human-readable body shown in
// the Alerts UI. Structure mirrors the email so operators see the same context in
// either channel — vendor/device, scope, expiry, and the list of log sources and
// destinations that will break if the credential is not rotated in time.
//
// When there are no linked entities we still emit the credential summary; the
// notification is triggered upstream only for linked secrets so the empty case is
// defensive.
func buildCredentialExpiryProductMessage(entry NotificationBatchEntry, statusLabel string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Credential '%s' %s.", entry.Name, statusLabel)
	if entry.Vendor != "" {
		vendor := entry.Vendor
		if entry.Device != "" {
			vendor = vendor + " / " + entry.Device
		}
		fmt.Fprintf(&b, " Vendor/device: %s.", vendor)
	}
	if entry.Scope != "" {
		fmt.Fprintf(&b, " Scope: %s.", entry.Scope)
	}
	if entry.ExpiryDate != "" {
		fmt.Fprintf(&b, " Expiry date: %s (UTC).", entry.ExpiryDate)
	}
	if len(entry.LinkedSources) > 0 {
		fmt.Fprintf(&b, " Log sources: %s.", strings.Join(entry.LinkedSources, ", "))
	}
	if len(entry.LinkedCollectors) > 0 {
		fmt.Fprintf(&b, " Collectors: %s.", strings.Join(entry.LinkedCollectors, ", "))
	}
	if len(entry.LinkedDestinations) > 0 {
		fmt.Fprintf(&b, " Destinations: %s.", strings.Join(entry.LinkedDestinations, ", "))
	}
	return b.String()
}

// buildCredentialExpiryProductAlert constructs an alerts_async.Alert directly so we
// can use a functionality string that isn't in the vendored enum. Dedup ID matches
// the builder's format (sha256 of tenantId/entityId/entityName/functionality/type)
// so re-firing on the same secret updates the existing UI entry instead of creating
// a new one.
func buildCredentialExpiryProductAlert(
	batch NotificationBatch,
	entry NotificationBatchEntry,
	nowMs int64,
) *alerts_async.Alert {
	criticality := alerts_async.Warning.String()
	statusLabel := fmt.Sprintf("expires in %d day(s)", entry.DaysLeft)
	if entry.IsExpired {
		criticality = alerts_async.Critical.String()
		statusLabel = "has expired"
	} else if entry.DaysLeft <= 3 {
		criticality = alerts_async.Sever.String()
	}

	tenantIdStr := batch.TenantId.String()
	entityIdStr := entry.SecretId.String()

	title := fmt.Sprintf("Credential '%s' %s", entry.Name, statusLabel)
	message := buildCredentialExpiryProductMessage(entry, statusLabel)

	// buildId equivalent — see alerts_async/builder.go buildId.
	idInput := fmt.Sprintf(
		"tenantId=%s&entityId=%s&entityName=%s&functionality=%s&type=%s",
		tenantIdStr, entityIdStr, entry.Name, credentialFunctionality, credentialFunctionalityType,
	)
	hash := sha256.Sum256([]byte(idInput))
	alertId := hex.EncodeToString(hash[:])

	return &alerts_async.Alert{
		Id:                      alertId,
		Criticality:             criticality,
		Title:                   title,
		Message:                 message,
		CreatedAt:               nowMs,
		UpdatedAt:               nowMs,
		FirstObservedAt:         nowMs,
		LastObservedAt:          nowMs,
		TenantId:                tenantIdStr,
		Functionality:           credentialFunctionality,
		FunctionalityEntityId:   entityIdStr,
		FunctionalityEntityName: entry.Name,
		FunctionalityType:       credentialFunctionalityType,
		Status:                  alerts_async.AlertOpen.Value(),
		Dismissed:               false,
		AlertType:               alerts_async.External.String(),
		ErrorCode:               alerts_async.DCFE10001.Value(),
		ErrorMessage:            message,
		DataPlaneId:             common.DatabahnDataPlaneId,
		Action:                  credentialProductAlertAction,
	}
}
