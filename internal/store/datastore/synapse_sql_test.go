package datastore

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestBuildSynapseConnectionString(t *testing.T) {
	got := BuildSynapseConnectionString("myws", "mydb", "user", "pass")
	parsed, err := msdsn.Parse(got)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	if parsed.Host != "myws-ondemand.sql.azuresynapse.net" {
		t.Fatalf("host=%q", parsed.Host)
	}
	if parsed.Database != "mydb" || parsed.User != "user" || parsed.Password != "pass" {
		t.Fatalf("db/user/pass=%q/%q/%q", parsed.Database, parsed.User, parsed.Password)
	}
}

func TestBuildSynapseConnectionString_EscapesSpecialCharacters(t *testing.T) {
	got := BuildSynapseConnectionString("myws", "mydb", "user", "pass;encrypt=false")
	parsed, err := msdsn.Parse(got)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	if parsed.Password != "pass;encrypt=false" {
		t.Fatalf("password=%q", parsed.Password)
	}
}

func TestFirstNonEmptyStr(t *testing.T) {
	if got := firstNonEmptyStr("", "  ", "ok"); got != "ok" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveDestinationSynapseSQL_UsesStoreConfigOnly(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		AzureSynapseConfiguration: &synapseSQLConfigJSON{
			Workspace:   "store-ws",
			Database:    "store-db",
			SqlUsername: "store-user",
			SqlPassword: "store-pass",
		},
	}
	dsSyn := &datasetSynapseConfiguration{Workspace: "ds-ws", Database: "ds-db"}

	cfg, err := resolveDestinationSynapseSQL(context.Background(), nil, nil, uuid.Nil, storeCfg, dsSyn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Workspace != "store-ws" || cfg.Database != "store-db" {
		t.Fatalf("expected store workspace/db, got %q/%q", cfg.Workspace, cfg.Database)
	}
	if cfg.SqlUsername != "store-user" || cfg.SqlPassword != "store-pass" {
		t.Fatalf("expected store creds, got %q/%q", cfg.SqlUsername, cfg.SqlPassword)
	}
}

func TestResolveExternalSynapseSQL_UsesDatasetAndSecret(t *testing.T) {
	dsSyn := &datasetSynapseConfiguration{Workspace: "ds-ws", Database: "ds-db"}
	merged := map[string]string{
		azureSynapseSQLUsernameKey: "secret-user",
		azureSynapseSQLPasswordKey: "secret-pass",
	}

	cfg := &SynapseSQLConfig{
		Workspace:   strings.TrimSpace(dsSyn.Workspace),
		Database:    strings.TrimSpace(dsSyn.Database),
		SqlUsername: strings.TrimSpace(merged[azureSynapseSQLUsernameKey]),
		SqlPassword: merged[azureSynapseSQLPasswordKey],
	}
	cfg, err := validateSynapseSQLConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Workspace != "ds-ws" || cfg.Database != "ds-db" {
		t.Fatalf("expected dataset workspace/db, got %q/%q", cfg.Workspace, cfg.Database)
	}
	if cfg.SqlUsername != "secret-user" || cfg.SqlPassword != "secret-pass" {
		t.Fatalf("expected secret creds, got %q/%q", cfg.SqlUsername, cfg.SqlPassword)
	}
}
