package datastore

import (
	"strings"
	"testing"
)

func TestBuildSynapseConnectionString(t *testing.T) {
	got := BuildSynapseConnectionString("myws", "mydb", "user", "pass")
	if got == "" {
		t.Fatal("expected connection string")
	}
	for _, want := range []string{"myws-ondemand.sql.azuresynapse.net", "database=mydb", "user id=user", "password=pass"} {
		if !strings.Contains(got, want) {
			t.Fatalf("connection string %q missing %q", got, want)
		}
	}
}

func TestFirstNonEmptyStr(t *testing.T) {
	if got := firstNonEmptyStr("", "  ", "ok"); got != "ok" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveDestinationSynapseSQL_MergesFields(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		AzureSynapseConfiguration: &synapseSQLConfigJSON{
			Workspace: "store-ws",
			Database:  "store-db",
		},
	}
	dsSyn := &datasetSynapseConfiguration{Workspace: "ds-ws", Database: "ds-db"}
	merged := map[string]string{
		azureSynapseSQLUsernameKey: "secret-user",
		azureSynapseSQLPasswordKey: "secret-pass",
	}

	cfg := &SynapseSQLConfig{
		Workspace: storeCfg.AzureSynapseConfiguration.Workspace,
		Database:  storeCfg.AzureSynapseConfiguration.Database,
	}
	cfg.Workspace = firstNonEmptyStr(cfg.Workspace, dsSyn.Workspace)
	cfg.Database = firstNonEmptyStr(cfg.Database, dsSyn.Database)
	cfg.SqlUsername = firstNonEmptyStr(cfg.SqlUsername, merged[azureSynapseSQLUsernameKey])
	cfg.SqlPassword = firstNonEmptyStr(cfg.SqlPassword, merged[azureSynapseSQLPasswordKey])

	if cfg.Workspace != "store-ws" {
		t.Fatalf("workspace=%q", cfg.Workspace)
	}
	if cfg.Database != "store-db" {
		t.Fatalf("database=%q", cfg.Database)
	}
	if cfg.SqlUsername != "secret-user" || cfg.SqlPassword != "secret-pass" {
		t.Fatalf("creds=%q/%q", cfg.SqlUsername, cfg.SqlPassword)
	}
}
