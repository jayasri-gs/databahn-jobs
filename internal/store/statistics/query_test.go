package statistics

import (
	"testing"
	"time"
)

func TestParseAlertDocuments(t *testing.T) {
	tests := []struct {
		name        string
		osDocuments []map[string]any
		expected    []AlertDocument
		expectError bool
	}{
		{
			name: "RFC3339 format with Z timezone",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-1",
					"title":                   "Test Alert 1",
					"criticality":             "high",
					"message":                 "Test message",
					"createdAt":               "2025-07-24T21:30:04.275Z",
					"updatedAt":               "2025-07-24T21:35:04.275Z",
					"firstObservedAt":         "2025-07-24T21:30:04.275Z",
					"lastObservedAt":          "2025-07-24T21:40:04.275Z",
					"tenantId":                "tenant-123",
					"functionalityType":       "test-type",
					"functionality":           "test-func",
					"functionalityEntityId":   "entity-123",
					"functionalityEntityName": "Test Entity",
					"dismissed":               false,
					"dismissedAt":             "2025-07-24T21:45:04.275Z",
					"dismissedBy":             "user-123",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-1",
					Title:                   "Test Alert 1",
					Criticality:             "high",
					Message:                 "Test message",
					CreatedAt:               time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275000000, time.UTC).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275000000, time.UTC).UnixMilli(),
					TenantId:                "tenant-123",
					FunctionalityType:       "test-type",
					Functionality:           "test-func",
					FunctionalityEntityId:   "entity-123",
					FunctionalityEntityName: "Test Entity",
					Dismissed:               false,
					DismissedAt:             time.Date(2025, 7, 24, 21, 45, 4, 275000000, time.UTC).UnixMilli(),
					DismissedBy:             "user-123",
				},
			},
			expectError: false,
		},
		{
			name: "RFC3339 format with timezone offset",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-2",
					"title":                   "Test Alert 2",
					"criticality":             "medium",
					"message":                 "Test message 2",
					"createdAt":               "2025-07-24T21:30:04.275+05:30",
					"updatedAt":               "2025-07-24T21:35:04.275-08:00",
					"firstObservedAt":         "2025-07-24T21:30:04.275+05:30",
					"lastObservedAt":          "2025-07-24T21:40:04.275+00:00",
					"tenantId":                "tenant-456",
					"functionalityType":       "test-type-2",
					"functionality":           "test-func-2",
					"functionalityEntityId":   "entity-456",
					"functionalityEntityName": "Test Entity 2",
					"dismissed":               true,
					"dismissedAt":             "2025-07-24T21:45:04.275+00:00",
					"dismissedBy":             "user-456",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-2",
					Title:                   "Test Alert 2",
					Criticality:             "medium",
					Message:                 "Test message 2",
					CreatedAt:               time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275000000, time.FixedZone("", -8*3600)).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275000000, time.FixedZone("", 0)).UnixMilli(),
					TenantId:                "tenant-456",
					FunctionalityType:       "test-type-2",
					Functionality:           "test-func-2",
					FunctionalityEntityId:   "entity-456",
					FunctionalityEntityName: "Test Entity 2",
					Dismissed:               true,
					DismissedAt:             time.Date(2025, 7, 24, 21, 45, 4, 275000000, time.UTC).UnixMilli(),
					DismissedBy:             "user-456",
				},
			},
			expectError: false,
		},
		{
			name: "RFC3339Nano format",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-3",
					"title":                   "Test Alert 3",
					"criticality":             "low",
					"message":                 "Test message 3",
					"createdAt":               "2025-07-24T21:30:04.275123456Z",
					"updatedAt":               "2025-07-24T21:35:04.275123456+05:30",
					"firstObservedAt":         "2025-07-24T21:30:04.275123456Z",
					"lastObservedAt":          "2025-07-24T21:40:04.275123456+05:30",
					"tenantId":                "tenant-789",
					"functionalityType":       "test-type-3",
					"functionality":           "test-func-3",
					"functionalityEntityId":   "entity-789",
					"functionalityEntityName": "Test Entity 3",
					"dismissed":               false,
					"dismissedAt":             "2025-07-24T21:45:04.275123456Z",
					"dismissedBy":             "user-789",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-3",
					Title:                   "Test Alert 3",
					Criticality:             "low",
					Message:                 "Test message 3",
					CreatedAt:               time.Date(2025, 7, 24, 21, 30, 4, 275123456, time.UTC).UnixMilli(),
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275123456, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275123456, time.UTC).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275123456, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					TenantId:                "tenant-789",
					FunctionalityType:       "test-type-3",
					Functionality:           "test-func-3",
					FunctionalityEntityId:   "entity-789",
					FunctionalityEntityName: "Test Entity 3",
					Dismissed:               false,
					DismissedAt:             time.Date(2025, 7, 24, 21, 45, 4, 275123456, time.UTC).UnixMilli(),
					DismissedBy:             "user-789",
				},
			},
			expectError: false,
		},
		{
			name: "Custom format with milliseconds and no timezone",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-4",
					"title":                   "Test Alert 4",
					"criticality":             "high",
					"message":                 "Test message 4",
					"createdAt":               "2025-07-24T21:30:04.275",
					"updatedAt":               "2025-07-24T21:35:04.275",
					"firstObservedAt":         "2025-07-24T21:30:04.275",
					"lastObservedAt":          "2025-07-24T21:40:04.275",
					"tenantId":                "tenant-101",
					"functionalityType":       "test-type-4",
					"functionality":           "test-func-4",
					"functionalityEntityId":   "entity-101",
					"functionalityEntityName": "Test Entity 4",
					"dismissed":               true,
					"dismissedAt":             "2025-07-24T21:45:04.275",
					"dismissedBy":             "user-101",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-4",
					Title:                   "Test Alert 4",
					Criticality:             "high",
					Message:                 "Test message 4",
					CreatedAt:               time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275000000, time.UTC).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275000000, time.UTC).UnixMilli(),
					TenantId:                "tenant-101",
					FunctionalityType:       "test-type-4",
					Functionality:           "test-func-4",
					FunctionalityEntityId:   "entity-101",
					FunctionalityEntityName: "Test Entity 4",
					Dismissed:               true,
					DismissedAt:             time.Date(2025, 7, 24, 21, 45, 4, 275000000, time.UTC).UnixMilli(),
					DismissedBy:             "user-101",
				},
			},
			expectError: false,
		},
		{
			name: "Integer timestamp values (already in int64 format)",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-5",
					"title":                   "Test Alert 5",
					"criticality":             "medium",
					"message":                 "Test message 5",
					"createdAt":               int64(1721856604275),
					"updatedAt":               int64(1721856904275),
					"firstObservedAt":         int64(1721856604275),
					"lastObservedAt":          int64(1721857204275),
					"tenantId":                "tenant-202",
					"functionalityType":       "test-type-5",
					"functionality":           "test-func-5",
					"functionalityEntityId":   "entity-202",
					"functionalityEntityName": "Test Entity 5",
					"dismissed":               false,
					"dismissedAt":             int64(1721857504275),
					"dismissedBy":             "user-202",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-5",
					Title:                   "Test Alert 5",
					Criticality:             "medium",
					Message:                 "Test message 5",
					CreatedAt:               1721856604275,
					UpdatedAt:               1721856904275,
					FirstObservedAt:         1721856604275,
					LastObservedAt:          1721857204275,
					TenantId:                "tenant-202",
					FunctionalityType:       "test-type-5",
					Functionality:           "test-func-5",
					FunctionalityEntityId:   "entity-202",
					FunctionalityEntityName: "Test Entity 5",
					Dismissed:               false,
					DismissedAt:             1721857504275,
					DismissedBy:             "user-202",
				},
			},
			expectError: false,
		},
		{
			name: "Multiple documents with mixed date formats",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-6",
					"title":                   "Test Alert 6",
					"criticality":             "high",
					"message":                 "Test message 6",
					"createdAt":               "2025-07-24T21:30:04.275Z",
					"updatedAt":               "2025-07-24T21:35:04.275+05:30",
					"firstObservedAt":         "2025-07-24T21:30:04.275Z",
					"lastObservedAt":          "2025-07-24T21:40:04.275",
					"tenantId":                "tenant-303",
					"functionalityType":       "test-type-6",
					"functionality":           "test-func-6",
					"functionalityEntityId":   "entity-303",
					"functionalityEntityName": "Test Entity 6",
					"dismissed":               false,
					"dismissedAt":             int64(1721857504275),
					"dismissedBy":             "user-303",
				},
				{
					"_class":                  "Alert",
					"id":                      "test-id-7",
					"title":                   "Test Alert 7",
					"criticality":             "low",
					"message":                 "Test message 7",
					"createdAt":               int64(1721856604275),
					"updatedAt":               "2025-07-24T21:35:04.275Z",
					"firstObservedAt":         "2025-07-24T21:30:04.275+05:30",
					"lastObservedAt":          "2025-07-24T21:40:04.275Z",
					"tenantId":                "tenant-404",
					"functionalityType":       "test-type-7",
					"functionality":           "test-func-7",
					"functionalityEntityId":   "entity-404",
					"functionalityEntityName": "Test Entity 7",
					"dismissed":               true,
					"dismissedAt":             "2025-07-24T21:45:04.275",
					"dismissedBy":             "user-404",
				},
			},
			expected: []AlertDocument{
				{
					Class:                   "Alert",
					Id:                      "test-id-6",
					Title:                   "Test Alert 6",
					Criticality:             "high",
					Message:                 "Test message 6",
					CreatedAt:               time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275000000, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.UTC).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275000000, time.UTC).UnixMilli(),
					TenantId:                "tenant-303",
					FunctionalityType:       "test-type-6",
					Functionality:           "test-func-6",
					FunctionalityEntityId:   "entity-303",
					FunctionalityEntityName: "Test Entity 6",
					Dismissed:               false,
					DismissedAt:             1721857504275,
					DismissedBy:             "user-303",
				},
				{
					Class:                   "Alert",
					Id:                      "test-id-7",
					Title:                   "Test Alert 7",
					Criticality:             "low",
					Message:                 "Test message 7",
					CreatedAt:               1721856604275,
					UpdatedAt:               time.Date(2025, 7, 24, 21, 35, 4, 275000000, time.UTC).UnixMilli(),
					FirstObservedAt:         time.Date(2025, 7, 24, 21, 30, 4, 275000000, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
					LastObservedAt:          time.Date(2025, 7, 24, 21, 40, 4, 275000000, time.UTC).UnixMilli(),
					TenantId:                "tenant-404",
					FunctionalityType:       "test-type-7",
					Functionality:           "test-func-7",
					FunctionalityEntityId:   "entity-404",
					FunctionalityEntityName: "Test Entity 7",
					Dismissed:               true,
					DismissedAt:             time.Date(2025, 7, 24, 21, 45, 4, 275000000, time.UTC).UnixMilli(),
					DismissedBy:             "user-404",
				},
			},
			expectError: false,
		},
		{
			name:        "Empty documents array",
			osDocuments: []map[string]any{},
			expected:    []AlertDocument{},
			expectError: false,
		},
		{
			name: "Invalid date format (should be ignored and left as string)",
			osDocuments: []map[string]any{
				{
					"_class":                  "Alert",
					"id":                      "test-id-8",
					"title":                   "Test Alert 8",
					"criticality":             "high",
					"message":                 "Test message 8",
					"createdAt":               "invalid-date-format",
					"updatedAt":               "2025-07-24T21:35:04.275Z",
					"firstObservedAt":         "2025-07-24T21:30:04.275Z",
					"lastObservedAt":          "2025-07-24T21:40:04.275Z",
					"tenantId":                "tenant-505",
					"functionalityType":       "test-type-8",
					"functionality":           "test-func-8",
					"functionalityEntityId":   "entity-505",
					"functionalityEntityName": "Test Entity 8",
					"dismissed":               false,
					"dismissedAt":             "2025-07-24T21:45:04.275Z",
					"dismissedBy":             "user-505",
				},
			},
			expectError: true, // Should fail because invalid date format can't be converted to int64
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseAlertDocuments(tt.osDocuments)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d results, got %d", len(tt.expected), len(result))
			}

			for i, expected := range tt.expected {
				actual := result[i]
				if expected.Class != actual.Class {
					t.Errorf("Class mismatch at index %d: expected %s, got %s", i, expected.Class, actual.Class)
				}
				if expected.Id != actual.Id {
					t.Errorf("Id mismatch at index %d: expected %s, got %s", i, expected.Id, actual.Id)
				}
				if expected.Title != actual.Title {
					t.Errorf("Title mismatch at index %d: expected %s, got %s", i, expected.Title, actual.Title)
				}
				if expected.Criticality != actual.Criticality {
					t.Errorf("Criticality mismatch at index %d: expected %s, got %s", i, expected.Criticality, actual.Criticality)
				}
				if expected.Message != actual.Message {
					t.Errorf("Message mismatch at index %d: expected %s, got %s", i, expected.Message, actual.Message)
				}
				if expected.CreatedAt != actual.CreatedAt {
					t.Errorf("CreatedAt mismatch at index %d: expected %d, got %d", i, expected.CreatedAt, actual.CreatedAt)
				}
				if expected.UpdatedAt != actual.UpdatedAt {
					t.Errorf("UpdatedAt mismatch at index %d: expected %d, got %d", i, expected.UpdatedAt, actual.UpdatedAt)
				}
				if expected.FirstObservedAt != actual.FirstObservedAt {
					t.Errorf("FirstObservedAt mismatch at index %d: expected %d, got %d", i, expected.FirstObservedAt, actual.FirstObservedAt)
				}
				if expected.LastObservedAt != actual.LastObservedAt {
					t.Errorf("LastObservedAt mismatch at index %d: expected %d, got %d", i, expected.LastObservedAt, actual.LastObservedAt)
				}
				if expected.TenantId != actual.TenantId {
					t.Errorf("TenantId mismatch at index %d: expected %s, got %s", i, expected.TenantId, actual.TenantId)
				}
				if expected.FunctionalityType != actual.FunctionalityType {
					t.Errorf("FunctionalityType mismatch at index %d: expected %s, got %s", i, expected.FunctionalityType, actual.FunctionalityType)
				}
				if expected.Functionality != actual.Functionality {
					t.Errorf("Functionality mismatch at index %d: expected %s, got %s", i, expected.Functionality, actual.Functionality)
				}
				if expected.FunctionalityEntityId != actual.FunctionalityEntityId {
					t.Errorf("FunctionalityEntityId mismatch at index %d: expected %s, got %s", i, expected.FunctionalityEntityId, actual.FunctionalityEntityId)
				}
				if expected.FunctionalityEntityName != actual.FunctionalityEntityName {
					t.Errorf("FunctionalityEntityName mismatch at index %d: expected %s, got %s", i, expected.FunctionalityEntityName, actual.FunctionalityEntityName)
				}
				if expected.Dismissed != actual.Dismissed {
					t.Errorf("Dismissed mismatch at index %d: expected %t, got %t", i, expected.Dismissed, actual.Dismissed)
				}
				if expected.DismissedAt != actual.DismissedAt {
					t.Errorf("DismissedAt mismatch at index %d: expected %d, got %d", i, expected.DismissedAt, actual.DismissedAt)
				}
				if expected.DismissedBy != actual.DismissedBy {
					t.Errorf("DismissedBy mismatch at index %d: expected %s, got %s", i, expected.DismissedBy, actual.DismissedBy)
				}
			}
		})
	}
}

func TestParseAlertDocuments_EdgeCases(t *testing.T) {
	t.Run("nil documents array", func(t *testing.T) {
		result, err := ParseAlertDocuments(nil)
		if err != nil {
			t.Errorf("Expected no error for nil input, got: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("Expected empty result for nil input, got %d items", len(result))
		}
	})

	t.Run("document with missing optional fields", func(t *testing.T) {
		osDocuments := []map[string]any{
			{
				"_class":                  "Alert",
				"id":                      "test-id-minimal",
				"title":                   "Minimal Alert",
				"criticality":             "high",
				"message":                 "Minimal message",
				"createdAt":               "2025-07-24T21:30:04.275Z",
				"updatedAt":               "2025-07-24T21:35:04.275Z",
				"firstObservedAt":         "2025-07-24T21:30:04.275Z",
				"lastObservedAt":          "2025-07-24T21:40:04.275Z",
				"tenantId":                "tenant-minimal",
				"functionalityType":       "test-type",
				"functionality":           "test-func",
				"functionalityEntityId":   "entity-minimal",
				"functionalityEntityName": "Minimal Entity",
				"dismissed":               false,
				// dismissedAt and dismissedBy are missing
			},
		}

		result, err := ParseAlertDocuments(osDocuments)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(result))
		}

		alert := result[0]
		if alert.Id != "test-id-minimal" {
			t.Errorf("Expected Id 'test-id-minimal', got '%s'", alert.Id)
		}
		if alert.Title != "Minimal Alert" {
			t.Errorf("Expected Title 'Minimal Alert', got '%s'", alert.Title)
		}
		if alert.DismissedAt != 0 {
			t.Errorf("Expected DismissedAt 0, got %d", alert.DismissedAt)
		}
		if alert.DismissedBy != "" {
			t.Errorf("Expected DismissedBy '', got '%s'", alert.DismissedBy)
		}
	})

	t.Run("document with zero values for date fields", func(t *testing.T) {
		osDocuments := []map[string]any{
			{
				"_class":                  "Alert",
				"id":                      "test-id-zero",
				"title":                   "Zero Date Alert",
				"criticality":             "high",
				"message":                 "Zero date message",
				"createdAt":               int64(0),
				"updatedAt":               int64(0),
				"firstObservedAt":         int64(0),
				"lastObservedAt":          int64(0),
				"tenantId":                "tenant-zero",
				"functionalityType":       "test-type",
				"functionality":           "test-func",
				"functionalityEntityId":   "entity-zero",
				"functionalityEntityName": "Zero Entity",
				"dismissed":               false,
				"dismissedAt":             int64(0),
				"dismissedBy":             "",
			},
		}

		result, err := ParseAlertDocuments(osDocuments)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(result))
		}

		alert := result[0]
		if alert.CreatedAt != 0 {
			t.Errorf("Expected CreatedAt 0, got %d", alert.CreatedAt)
		}
		if alert.UpdatedAt != 0 {
			t.Errorf("Expected UpdatedAt 0, got %d", alert.UpdatedAt)
		}
		if alert.FirstObservedAt != 0 {
			t.Errorf("Expected FirstObservedAt 0, got %d", alert.FirstObservedAt)
		}
		if alert.LastObservedAt != 0 {
			t.Errorf("Expected LastObservedAt 0, got %d", alert.LastObservedAt)
		}
		if alert.DismissedAt != 0 {
			t.Errorf("Expected DismissedAt 0, got %d", alert.DismissedAt)
		}
	})
}
