//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"text/template"
)

type EmailTemplate struct {
	Name           string
	Title          string
	NewAlerts      *EmailAlertSection
	ReminderAlerts *EmailAlertSection
}

type EmailAlertSection struct {
	Heading string
	Details []EmailTemplateDetails
}

type EmailTemplateDetails struct {
	FunctionalityEntityName string
	Message                 string
	FirstObservedAt         string
	Title                   string
	ReminderNumber          int
}

func main() {
	dir := filepath.Dir(os.Args[0])
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	sample := EmailTemplate{
		Name:  "Dear Team,",
		Title: "No new data ingested",
		NewAlerts: &EmailAlertSection{
			Heading: "New Alerts",
			Details: []EmailTemplateDetails{
				{
					FunctionalityEntityName: "aws-cloudtrail-prod",
					Title:                   "No data ingested in the last 30 minutes",
					Message:                 "Source has not received any events since the configured inactivity threshold was crossed.",
					FirstObservedAt:         "2026-06-23T10:15:00Z",
				},
				{
					FunctionalityEntityName: "okta-sso-logs",
					Title:                   "No data ingested in the last 30 minutes",
					Message:                 "Last event timestamp is older than the inactivity window.",
					FirstObservedAt:         "2026-06-23T10:18:00Z",
				},
			},
		},
		ReminderAlerts: &EmailAlertSection{
			Heading: "Reminder Alerts",
			Details: []EmailTemplateDetails{
				{
					FunctionalityEntityName: "crowdstrike-edr",
					Title:                   "Reminder: no data delivered",
					Message:                 "Delivery has not resumed. This is a follow-up for an ongoing alert.",
					FirstObservedAt:         "2026-06-23T07:30:00Z",
					ReminderNumber:          3,
				},
				{
					FunctionalityEntityName: "palo-alto-fw-east",
					Title:                   "Reminder: no data ingested",
					Message:                 "This alert is still active. No new data has been observed since the last notification.",
					FirstObservedAt:         "2026-06-23T08:00:00Z",
					ReminderNumber:          2,
				},
			},
		},
	}

	sort.Slice(sample.ReminderAlerts.Details, func(i, j int) bool {
		return sample.ReminderAlerts.Details[i].ReminderNumber < sample.ReminderAlerts.Details[j].ReminderNumber
	})

	templates := []string{"green_alert.html", "warning_alert.html", "error_alert.html"}
	outDir := filepath.Join(dir, "samples")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}

	for _, name := range templates {
		t, err := template.ParseFiles(filepath.Join(dir, name))
		if err != nil {
			panic(err)
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, sample); err != nil {
			panic(err)
		}
		outPath := filepath.Join(outDir, "sample_"+name)
		if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
			panic(err)
		}
		fmt.Println("wrote", outPath)
	}
}
