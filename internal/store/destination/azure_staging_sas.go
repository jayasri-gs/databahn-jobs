package destination

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// BlobStagingSAS is a container-scoped, write-capable SAS handed to an external engine
// (currently ADX .export) so it can write staged export files into our blob container.
type BlobStagingSAS struct {
	AccountName string
	Container   string
	SASToken    string // query string, no leading '?'
}

// ContainerURL returns the plain HTTPS container URL without credentials.
func (s BlobStagingSAS) ContainerURL() string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s", s.AccountName, s.Container)
}

// KustoConnectionString renders the container as a Kusto storage connection string.
// Kusto appends a SAS token as a query string (account keys use ';' instead).
func (s BlobStagingSAS) KustoConnectionString() string {
	return fmt.Sprintf("%s?%s", s.ContainerURL(), s.SASToken)
}

// GenerateContainerWriteSAS mints a container-scoped read/write/list/delete SAS.
//
// AUTH_CONNECTION_STRING signs with the account key, or reuses the embedded SAS when the
// connection string carries one instead of a key. AUTH_SERVICE_PRINCIPAL signs a user
// delegation SAS, because Kusto storage connection strings accept a SAS or an account
// key but never a service principal secret.
//
//nolint:gosec // CWE-532 false positive: the SAS is returned to callers, never logged
func GenerateContainerWriteSAS(ctx context.Context, cfg *AzureBlobConfig, expiry time.Duration) (*BlobStagingSAS, error) {
	if cfg == nil {
		return nil, fmt.Errorf("azure blob config is required")
	}
	if cfg.Container == "" {
		return nil, fmt.Errorf("azure blob container is required")
	}
	if expiry <= 0 {
		expiry = 24 * time.Hour
	}

	authType := cfg.AuthType
	if authType == "" && cfg.ConnectionString != "" {
		authType = "AUTH_CONNECTION_STRING"
	}

	now := time.Now().UTC()
	expiresAt := now.Add(expiry)
	// Only what `.export` needs to create and write blobs. Read, List and Delete are
	// deliberately absent: the worker reads the staged blobs and deletes them afterwards with
	// the destination's own credentials (see unload.BlobReader), never with this token.
	permissions := (&sas.ContainerPermissions{Add: true, Create: true, Write: true}).String()

	switch authType {
	case "AUTH_CONNECTION_STRING":
		accountName, accountKey, keyErr := ParseBlobConnectionString(cfg.ConnectionString)
		if keyErr != nil {
			// SAS-only connection string: there is no account key to sign a fresh token with,
			// so the configured one is reused — but only after checking it can actually do the
			// job. Forwarding it blind would hand ADX a token that is already expired, expires
			// mid-export, or lacks write permission, and the export would fail inside Kusto
			// with an opaque storage error instead of here.
			sasAccount, sasToken, sasErr := ParseSASConnectionString(cfg.ConnectionString)
			if sasErr != nil {
				return nil, fmt.Errorf("parse connection string: %w", keyErr)
			}
			token := strings.TrimPrefix(sasToken, "?")
			if err := validateStagingSASToken(token, expiresAt); err != nil {
				return nil, err
			}
			return &BlobStagingSAS{AccountName: sasAccount, Container: cfg.Container, SASToken: token}, nil
		}
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("shared key credential: %w", err)
		}
		values := sas.BlobSignatureValues{
			Version:       sas.Version,
			Protocol:      sas.ProtocolHTTPS,
			StartTime:     now.Add(-5 * time.Minute),
			ExpiryTime:    expiresAt,
			ContainerName: cfg.Container,
			Permissions:   permissions,
		}
		queryParams, err := values.SignWithSharedKey(cred)
		if err != nil {
			return nil, fmt.Errorf("sign staging SAS: %w", err)
		}
		return &BlobStagingSAS{AccountName: accountName, Container: cfg.Container, SASToken: queryParams.Encode()}, nil

	case "AUTH_SERVICE_PRINCIPAL":
		if cfg.AccountName == "" || cfg.TenantID == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
			return nil, fmt.Errorf("service principal azure blob credentials are incomplete")
		}
		adCred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("create service principal credential: %w", err)
		}
		serviceClient, err := service.NewClient(fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AccountName), adCred, nil)
		if err != nil {
			return nil, fmt.Errorf("create azure service client: %w", err)
		}
		keyInfo := service.KeyInfo{
			Start:  strPtr(now.Add(-5 * time.Minute).Format(time.RFC3339)),
			Expiry: strPtr(expiresAt.Format(time.RFC3339)),
		}
		udc, err := serviceClient.GetUserDelegationCredential(ctx, keyInfo, nil)
		if err != nil {
			return nil, fmt.Errorf("get user delegation credential: %w", err)
		}
		values := sas.BlobSignatureValues{
			Version:       sas.Version,
			Protocol:      sas.ProtocolHTTPS,
			StartTime:     now.Add(-5 * time.Minute),
			ExpiryTime:    expiresAt,
			ContainerName: cfg.Container,
			Permissions:   permissions,
		}
		queryParams, err := values.SignWithUserDelegation(udc)
		if err != nil {
			return nil, fmt.Errorf("sign staging SAS: %w", err)
		}
		return &BlobStagingSAS{AccountName: cfg.AccountName, Container: cfg.Container, SASToken: queryParams.Encode()}, nil

	default:
		return nil, fmt.Errorf("unsupported azure blob auth type for staging SAS: %s", authType)
	}
}

// ParseBlobConnectionString extracts AccountName and AccountKey from a storage connection string.
func ParseBlobConnectionString(connStr string) (accountName, accountKey string, err error) {
	for _, part := range strings.Split(connStr, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "AccountName":
			accountName = kv[1]
		case "AccountKey":
			accountKey = kv[1]
		}
	}
	if accountName == "" || accountKey == "" {
		return "", "", fmt.Errorf("connection string missing AccountName or AccountKey")
	}
	return accountName, accountKey, nil
}

// ParseSASConnectionString extracts AccountName and SharedAccessSignature from a SAS connection string.
func ParseSASConnectionString(connStr string) (accountName, sasToken string, err error) {
	var blobEndpoint string
	for _, part := range strings.Split(connStr, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "AccountName":
			accountName = kv[1]
		case "SharedAccessSignature":
			sasToken = kv[1]
		case "BlobEndpoint":
			blobEndpoint = kv[1]
		}
	}
	if sasToken == "" {
		return "", "", fmt.Errorf("connection string missing SharedAccessSignature")
	}
	if accountName == "" && blobEndpoint != "" {
		host := strings.TrimPrefix(strings.TrimPrefix(blobEndpoint, "https://"), "http://")
		accountName = strings.SplitN(host, ".", 2)[0]
	}
	if accountName == "" {
		return "", "", fmt.Errorf("connection string missing AccountName")
	}
	return accountName, sasToken, nil
}

// validateStagingSASToken checks a pre-configured SAS is usable for an ADX export: it must
// still be valid when the export is expected to finish, and it must permit writing.
func validateStagingSASToken(token string, needsUntil time.Time) error {
	values, err := url.ParseQuery(token)
	if err != nil {
		return fmt.Errorf("configured staging SAS is not a valid token: %w", err)
	}

	expiry := values.Get("se")
	if expiry == "" {
		return fmt.Errorf("configured staging SAS has no expiry (se)")
	}
	expiresAt, err := time.Parse(time.RFC3339, expiry)
	if err != nil {
		return fmt.Errorf("configured staging SAS has an unparseable expiry %q: %w", expiry, err)
	}
	if expiresAt.Before(needsUntil) {
		return fmt.Errorf(
			"configured staging SAS expires at %s, before the export window ends at %s",
			expiresAt.UTC().Format(time.RFC3339), needsUntil.UTC().Format(time.RFC3339))
	}

	// sp is an unordered set of permission letters; w (write) is the one .export requires.
	perms := values.Get("sp")
	if perms == "" {
		return fmt.Errorf("configured staging SAS has no permissions (sp)")
	}
	if !strings.Contains(perms, "w") {
		return fmt.Errorf("configured staging SAS permissions %q do not include write", perms)
	}
	return nil
}
