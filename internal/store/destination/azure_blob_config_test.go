package destination

import "testing"

func TestParseAzureBlobConfigFromWrapper_ConnectionString(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"azure_blob_auth_type":                         "AUTH_CONNECTION_STRING",
			"azure_blob_container":                         "exports",
			"azure_blob_storage_account_connection_string": "DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;EndpointSuffix=core.windows.net",
		},
	}
	cfg, err := parseAzureBlobConfigFromWrapper(wrapper, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Container != "exports" {
		t.Fatalf("container: got %q want exports", cfg.Container)
	}
	if cfg.AuthType != "AUTH_CONNECTION_STRING" {
		t.Fatalf("auth type: got %q want AUTH_CONNECTION_STRING", cfg.AuthType)
	}
}
