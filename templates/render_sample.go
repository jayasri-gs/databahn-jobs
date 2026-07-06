//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"os"
	"path/filepath"
	"sort"
	"text/template"
)

type EmailTheme struct {
	Accent                string
	Heading               string
	EntityHeading         string
	SectionBackground     string
	LastSectionBackground string
	CardBorder            string
	BadgeBackground       string
	LastAccent            string
}

type EmailTemplate struct {
	Name               string
	Title              string
	Theme              EmailTheme
	NewAlerts          *EmailAlertSection
	ReminderAlerts     *EmailAlertSection
	LastReminderAlerts *EmailAlertSection
}

type EmailAlertSection struct {
	Heading string
	Details []EmailTemplateDetails
}

type EmailTemplateDetails struct {
	FunctionalityEntityName string
	Message                 string
	LastObservedAt          string
	Title                   string
	ReminderNumber          int
	Theme                   EmailTheme
}

var (
	errorEmailTheme = EmailTheme{
		Accent:                "#E85D5D",
		Heading:               "#C03939",
		EntityHeading:         "#C03939",
		SectionBackground:     "#FEF2F2",
		LastSectionBackground: "#FEF2F2",
		CardBorder:            "#F5D5D5",
		BadgeBackground:       "#FDE8E8",
		LastAccent:            "#C03939",
	}
	warningEmailTheme = EmailTheme{
		Accent:                "#E8943A",
		Heading:               "#C47A15",
		EntityHeading:         "#C47A15",
		SectionBackground:     "#FFF8F0",
		LastSectionBackground: "#FFEFD9",
		CardBorder:            "#F5E4CC",
		BadgeBackground:       "#FFEFD9",
		LastAccent:            "#C47A15",
	}
	greenEmailTheme = EmailTheme{
		Accent:                "#45C96A",
		Heading:               "#1F7A4A",
		EntityHeading:         "#2B8A58",
		SectionBackground:     "#F0FBF4",
		LastSectionBackground: "#D4F5DE",
		CardBorder:            "#C8EBD4",
		BadgeBackground:       "#D4F5DE",
		LastAccent:            "#2B8A58",
	}
)

func toTitleCase(value string) string {
	if value == "" {
		return value
	}
	return cases.Title(language.English).String(value)
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
					LastObservedAt:          "2026-06-23T10:15:00Z",
				},
				{
					FunctionalityEntityName: "okta-sso-logs",
					Title:                   "No data ingested in the last 30 minutes",
					Message:                 "Last event timestamp is older than the inactivity window.",
					LastObservedAt:          "2026-06-23T10:18:00Z",
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
					LastObservedAt:          "2026-06-23T07:30:00Z",
					ReminderNumber:          2,
				},
				{
					FunctionalityEntityName: "palo-alto-fw-east",
					Title:                   "Reminder: no data ingested",
					Message:                 "This alert is still active. No new data has been observed since the last notification.",
					LastObservedAt:          "2026-06-23T08:00:00Z",
					ReminderNumber:          1,
				},
			},
		},
	}

	sort.Slice(sample.ReminderAlerts.Details, func(i, j int) bool {
		return sample.ReminderAlerts.Details[i].ReminderNumber < sample.ReminderAlerts.Details[j].ReminderNumber
	})

	sample.Title = toTitleCase(sample.Title)
	for i := range sample.NewAlerts.Details {
		sample.NewAlerts.Details[i].Title = toTitleCase(sample.NewAlerts.Details[i].Title)
	}
	for i := range sample.ReminderAlerts.Details {
		sample.ReminderAlerts.Details[i].Title = toTitleCase(sample.ReminderAlerts.Details[i].Title)
	}

	outDir := filepath.Join(dir, "samples")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}

	themes := []struct {
		name  string
		theme EmailTheme
	}{
		{name: "green", theme: greenEmailTheme},
		{name: "warning", theme: warningEmailTheme},
		{name: "error", theme: errorEmailTheme},
	}

	templatePath := filepath.Join(dir, "customer_alert.html")
	for _, theme := range themes {
		sample.Theme = theme.theme
		for i := range sample.NewAlerts.Details {
			sample.NewAlerts.Details[i].Theme = theme.theme
		}
		for i := range sample.ReminderAlerts.Details {
			sample.ReminderAlerts.Details[i].Theme = theme.theme
		}
		t, err := template.ParseFiles(templatePath)
		if err != nil {
			panic(err)
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, sample); err != nil {
			panic(err)
		}
		outPath := filepath.Join(outDir, "sample_"+theme.name+"_alert.html")
		if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
			panic(err)
		}
		fmt.Println("wrote", outPath)
	}

	lastReminderSample := EmailTemplate{
		Name:  "Dear Team,",
		Title: "Final Reminder: No New Data Ingested",
		LastReminderAlerts: &EmailAlertSection{
			Heading: "Last Reminder Alerts",
			Details: []EmailTemplateDetails{
				{
					FunctionalityEntityName: "palo-alto-fw-east",
					Title:                   "Reminder: no data ingested",
					Message:                 "This is the final reminder for this alert before notifications end.",
					LastObservedAt:          "2026-06-23T08:00:00Z",
					ReminderNumber:          5,
				},
			},
		},
	}
	lastReminderSample.LastReminderAlerts.Details[0].Title = toTitleCase(lastReminderSample.LastReminderAlerts.Details[0].Title)

	for _, theme := range themes {
		lastReminderSample.Theme = theme.theme
		for i := range lastReminderSample.LastReminderAlerts.Details {
			lastReminderSample.LastReminderAlerts.Details[i].Theme = theme.theme
		}
		t, err := template.ParseFiles(templatePath)
		if err != nil {
			panic(err)
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, lastReminderSample); err != nil {
			panic(err)
		}
		outPath := filepath.Join(outDir, "sample_last_reminder_"+theme.name+"_alert.html")
		if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
			panic(err)
		}
		fmt.Println("wrote", outPath)
	}
}
