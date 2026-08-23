package destination

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestBlobStagingSASURLs(t *testing.T) {
	s := BlobStagingSAS{AccountName: "acct", Container: "exports", SASToken: "sv=2024-11-04&sig=abc"}
	if got := s.ContainerURL(); got != "https://acct.blob.core.windows.net/exports" {
		t.Fatalf("ContainerURL = %q", got)
	}
	// Kusto takes a SAS as a query string, not a ';'-delimited secret.
	if got := s.KustoConnectionString(); got != "https://acct.blob.core.windows.net/exports?sv=2024-11-04&sig=abc" {
		t.Fatalf("KustoConnectionString = %q", got)
	}
}

func TestGenerateContainerWriteSAS_SharedKey(t *testing.T) {
	cfg := &AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		Container:        "exports",
		ConnectionString: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
	}
	staging, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour)
	if err != nil {
		t.Fatalf("GenerateContainerWriteSAS: %v", err)
	}
	if staging.AccountName != "acct" || staging.Container != "exports" {
		t.Fatalf("staging = %+v", staging)
	}
	for _, want := range []string{"sig=", "sp=", "se="} {
		if !strings.Contains(staging.SASToken, want) {
			t.Fatalf("SAS token missing parameter %q", want)
		}
	}
}

// Failure messages here identify the case rather than echoing token material: a SAS is
// credential material, and printing it into test output is the same pattern as logging it.
func TestGenerateContainerWriteSAS_SASConnectionString(t *testing.T) {
	cfg := &AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		Container:        "exports",
		ConnectionString: "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=" + usableSASToken(),
	}
	staging, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour)
	if err != nil {
		t.Fatalf("GenerateContainerWriteSAS: %v", err)
	}
	if staging.AccountName != "acct" {
		t.Fatalf("account name = %q", staging.AccountName)
	}
	if staging.SASToken != usableSASToken() {
		t.Fatal("configured SAS token was not passed through unchanged")
	}
}

func TestGenerateContainerWriteSAS_Rejects(t *testing.T) {
	if _, err := GenerateContainerWriteSAS(context.Background(), nil, time.Hour); err == nil {
		t.Fatal("nil config should error")
	}
	if _, err := GenerateContainerWriteSAS(context.Background(), &AzureBlobConfig{AuthType: "AUTH_CONNECTION_STRING"}, time.Hour); err == nil {
		t.Fatal("missing container should error")
	}
	cfg := &AzureBlobConfig{AuthType: "AUTH_SERVICE_PRINCIPAL", Container: "exports"}
	if _, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour); err == nil {
		t.Fatal("incomplete service principal credentials should error")
	}
	unknown := &AzureBlobConfig{AuthType: "AUTH_SOMETHING_ELSE", Container: "exports"}
	if _, err := GenerateContainerWriteSAS(context.Background(), unknown, time.Hour); err == nil {
		t.Fatal("unsupported auth type should error")
	}
}

func TestParseBlobConnectionStrings(t *testing.T) {
	name, key, err := ParseBlobConnectionString("AccountName=acct;AccountKey=abc==;EndpointSuffix=core.windows.net")
	if err != nil || name != "acct" || key != "abc==" {
		t.Fatalf("name=%q key=%q err=%v", name, key, err)
	}
	if _, _, err := ParseBlobConnectionString("AccountName=acct"); err == nil {
		t.Fatal("missing account key should error")
	}
	name, token, err := ParseSASConnectionString("AccountName=acct;SharedAccessSignature=sv=1&sig=2")
	if err != nil || name != "acct" || token != "sv=1&sig=2" {
		t.Fatalf("name=%q err=%v tokenMatched=%v", name, err, token == "sv=1&sig=2")
	}
	if _, _, err := ParseSASConnectionString("AccountName=acct"); err == nil {
		t.Fatal("missing SAS should error")
	}
}

// usableSASToken is a pre-configured token that can actually carry an export: write
// permission, and an expiry well beyond the requested window.
func usableSASToken() string {
	return "sv=2024-11-04&sp=racw&se=" + time.Now().UTC().Add(48*time.Hour).Format(time.RFC3339) + "&sig=given"
}

// A SAS-only connection string has no account key to sign a fresh token with, so the
// configured one is reused — but only if it can do the job. Forwarding an unusable token
// would surface as an opaque storage error inside Kusto instead of here.
func TestGenerateContainerWriteSAS_RejectsUnusableConfiguredToken(t *testing.T) {
	cases := []struct{ name, token string }{
		{"expired", "sv=2024-11-04&sp=racw&se=" + time.Now().UTC().Add(-time.Hour).Format(time.RFC3339) + "&sig=given"},
		{"expires before the export window ends", "sv=2024-11-04&sp=racw&se=" + time.Now().UTC().Add(10*time.Minute).Format(time.RFC3339) + "&sig=given"},
		{"read only", "sv=2024-11-04&sp=rl&se=" + time.Now().UTC().Add(48*time.Hour).Format(time.RFC3339) + "&sig=given"},
		{"no expiry", "sv=2024-11-04&sp=racw&sig=given"},
		{"no permissions", "sv=2024-11-04&se=" + time.Now().UTC().Add(48*time.Hour).Format(time.RFC3339) + "&sig=given"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &AzureBlobConfig{
				AuthType:         "AUTH_CONNECTION_STRING",
				Container:        "exports",
				ConnectionString: "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=" + tc.token,
			}
			if _, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour); err == nil {
				t.Fatalf("%s token was accepted", tc.name)
			}
		})
	}
}

// .export only needs to create and write blobs; the worker reads and deletes the staged
// output with the destination's own credentials.
func TestGenerateContainerWriteSAS_GrantsOnlyWritePermissions(t *testing.T) {
	cfg := &AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		Container:        "exports",
		ConnectionString: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
	}
	staging, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour)
	if err != nil {
		t.Fatalf("GenerateContainerWriteSAS: %v", err)
	}
	values, err := url.ParseQuery(staging.SASToken)
	if err != nil {
		t.Fatalf("parse SAS: %v", err)
	}
	perms := values.Get("sp")
	for _, denied := range []string{"d", "l", "r"} {
		if strings.Contains(perms, denied) {
			t.Fatalf("permissions %q still include %q", perms, denied)
		}
	}
	if !strings.Contains(perms, "w") || !strings.Contains(perms, "c") {
		t.Fatalf("permissions %q must allow create and write", perms)
	}
}
