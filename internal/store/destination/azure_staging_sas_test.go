package destination

import (
	"context"
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
			t.Fatalf("SAS token missing %q: %s", want, staging.SASToken)
		}
	}
}

func TestGenerateContainerWriteSAS_SASConnectionString(t *testing.T) {
	cfg := &AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		Container:        "exports",
		ConnectionString: "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=sv=2024-11-04&sig=given",
	}
	staging, err := GenerateContainerWriteSAS(context.Background(), cfg, time.Hour)
	if err != nil {
		t.Fatalf("GenerateContainerWriteSAS: %v", err)
	}
	if staging.AccountName != "acct" {
		t.Fatalf("account name = %q", staging.AccountName)
	}
	if staging.SASToken != "sv=2024-11-04&sig=given" {
		t.Fatalf("SAS token = %q", staging.SASToken)
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
		t.Fatalf("name=%q token=%q err=%v", name, token, err)
	}
	if _, _, err := ParseSASConnectionString("AccountName=acct"); err == nil {
		t.Fatal("missing SAS should error")
	}
}
