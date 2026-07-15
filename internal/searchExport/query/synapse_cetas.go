package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

const cetasSASExpiry = 24 * time.Hour

// CETASExecutor executes Synapse DDL statements needed for CETAS staging.
// *SynapseExecutor implements this interface via ExecDDL below.
type CETASExecutor interface {
	ExecDDL(ctx context.Context, ddl string) error
	GetQueryColumns(ctx context.Context, sql, database string) ([]string, error)
	// ExternalTableExists reports whether a CETAS external table exists. CETAS creates
	// the table object only after the statement fully succeeds, so existence proves the
	// staged query output is complete.
	ExternalTableExists(ctx context.Context, tableName string) (bool, error)
}

// StagingCreds holds credential parameters for a Synapse DATABASE SCOPED CREDENTIAL.
type StagingCreds struct {
	Identity   string // "SHARED ACCESS SIGNATURE" or service principal client ID
	Secret     string // SAS token query string or client secret
	DataSrcURL string // https://<account>.blob.core.windows.net/<container>
}

// StagingCredsFromBlobConfig derives Synapse CREDENTIAL parameters from an Azure Blob config.
// For AUTH_CONNECTION_STRING: parses AccountName+AccountKey, generates a write-capable SAS token.
// For AUTH_SERVICE_PRINCIPAL: returns service principal identity/secret with ADLS Gen2 URL.
func StagingCredsFromBlobConfig(cfg *destination.AzureBlobConfig) (*StagingCreds, error) {
	switch cfg.AuthType {
	case "AUTH_CONNECTION_STRING", "":
		accountName, accountKey, keyErr := parseConnectionString(cfg.ConnectionString)
		if keyErr != nil {
			// May be a SAS connection string (no AccountKey). Use SAS token directly.
			accountName, sasToken, sasErr := parseSASConnectionString(cfg.ConnectionString)
			if sasErr != nil {
				return nil, fmt.Errorf("parse connection string: %w", keyErr)
			}
			return &StagingCreds{
				Identity:   "SHARED ACCESS SIGNATURE",
				Secret:     sasToken,
				DataSrcURL: fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, cfg.Container),
			}, nil
		}
		sasToken, err := generateWriteSAS(accountName, accountKey, cfg.Container, cetasSASExpiry)
		if err != nil {
			return nil, fmt.Errorf("generate staging SAS: %w", err)
		}
		return &StagingCreds{
			Identity:   "SHARED ACCESS SIGNATURE",
			Secret:     sasToken,
			DataSrcURL: fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, cfg.Container),
		}, nil
	case "AUTH_SERVICE_PRINCIPAL":
		if cfg.AccountName == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
			return nil, fmt.Errorf("service principal credentials incomplete: need AccountName, ClientID, ClientSecret")
		}
		return &StagingCreds{
			Identity:   cfg.ClientID,
			Secret:     cfg.ClientSecret,
			DataSrcURL: fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net", cfg.Container, cfg.AccountName),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported auth type for CETAS staging: %s", cfg.AuthType)
	}
}

// CETASTableName returns the Synapse external table name for a report's CETAS export.
func CETASTableName(reportIDShort string) string {
	return fmt.Sprintf("staging_%s", reportIDShort)
}

// CETASStagingPrefix returns the blob prefix written by CETAS for a report.
func CETASStagingPrefix(reportID string) string {
	return fmt.Sprintf("databahn_out/%s/full/", reportID)
}

// CETASFileFormatName returns the Synapse file format name for a given export format.
// CSV → DELIMITEDTEXT format; all others → PARQUET format.
func CETASFileFormatName(exportFormat, reportIDShort string) string {
	if exportFormat == "csv" {
		return "DatabahnCETASCSV_" + reportIDShort
	}
	return "DatabahnCETASParquet_" + reportIDShort
}

// CETASCredentialDDL returns DDL to create a DATABASE SCOPED CREDENTIAL.
func CETASCredentialDDL(name, identity, secret string) string {
	return fmt.Sprintf(
		"CREATE DATABASE SCOPED CREDENTIAL [%s]\nWITH IDENTITY = '%s',\n     SECRET = '%s'",
		name, escapeSQL(identity), escapeSQL(secret),
	)
}

// CETASCredentialDropIfExistsDDL returns idempotent DDL to drop a credential.
func CETASCredentialDropIfExistsDDL(name string) string {
	return fmt.Sprintf(
		"IF EXISTS (SELECT 1 FROM sys.database_credentials WHERE name = '%s')\n    DROP DATABASE SCOPED CREDENTIAL [%s]",
		escapeSQL(name), name,
	)
}

// CETASDataSourceDDL returns DDL to create an EXTERNAL DATA SOURCE.
func CETASDataSourceDDL(name, credName, location string) string {
	return fmt.Sprintf(
		"CREATE EXTERNAL DATA SOURCE [%s]\nWITH (LOCATION = '%s', CREDENTIAL = [%s])",
		name, escapeSQL(location), credName,
	)
}

// CETASDataSourceDropIfExistsDDL returns idempotent DDL to drop a data source.
func CETASDataSourceDropIfExistsDDL(name string) string {
	return fmt.Sprintf(
		"IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = '%s')\n    DROP EXTERNAL DATA SOURCE [%s]",
		escapeSQL(name), name,
	)
}

// CETASFileFormatDDL returns DDL to create an EXTERNAL FILE FORMAT.
// exportFormat "csv" → DELIMITEDTEXT with given delimiter; anything else → PARQUET.
func CETASFileFormatDDL(name, exportFormat, delimiter string) string {
	if exportFormat == "csv" {
		delim := delimiter
		if delim == "" {
			delim = ","
		}
		return fmt.Sprintf(
			"CREATE EXTERNAL FILE FORMAT [%s]\nWITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (\n  FIELD_TERMINATOR = '%s',\n  STRING_DELIMITER = '\"',\n  ENCODING = 'UTF8'\n))",
			name, escapeSQL(delim),
		)
	}
	return fmt.Sprintf("CREATE EXTERNAL FILE FORMAT [%s]\nWITH (FORMAT_TYPE = PARQUET)", name)
}

// CETASFileFormatDropDDL returns DDL to drop an EXTERNAL FILE FORMAT.
func CETASFileFormatDropDDL(name string) string {
	return fmt.Sprintf("DROP EXTERNAL FILE FORMAT [%s]", name)
}

// CETASFileFormatDropIfExistsDDL returns idempotent DDL to drop an EXTERNAL FILE FORMAT.
func CETASFileFormatDropIfExistsDDL(name string) string {
	return fmt.Sprintf(
		"IF EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = '%s')\n    DROP EXTERNAL FILE FORMAT [%s]",
		escapeSQL(name), name,
	)
}

// CETASTableDDL returns DDL to create a CETAS external table writing to stagingPrefix.
func CETASTableDDL(tableName, dataSourceName, stagingPrefix, fileFormatName, selectSQL string) string {
	return fmt.Sprintf(
		"CREATE EXTERNAL TABLE [%s]\nWITH (\n  LOCATION = '%s',\n  DATA_SOURCE = [%s],\n  FILE_FORMAT = [%s]\n)\nAS\n%s",
		tableName, escapeSQL(stagingPrefix), dataSourceName, fileFormatName, selectSQL,
	)
}

// CETASTableDropDDL returns DDL to drop an external table.
func CETASTableDropDDL(tableName string) string {
	return fmt.Sprintf("DROP EXTERNAL TABLE [%s]", tableName)
}

// CETASTableDropIfExistsDDL returns idempotent DDL to drop an external table.
// Used before each CETAS to handle retries after partial failures.
func CETASTableDropIfExistsDDL(tableName string) string {
	return fmt.Sprintf(
		"IF EXISTS (SELECT 1 FROM sys.external_tables WHERE name = '%s')\n    DROP EXTERNAL TABLE [%s]",
		escapeSQL(tableName), tableName,
	)
}

// ExecDDL executes a DDL statement on the Synapse Serverless SQL connection.
func (e *SynapseExecutor) ExecDDL(ctx context.Context, ddl string) error {
	if e.db == nil {
		return fmt.Errorf("synapse not connected")
	}
	if _, err := e.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("DDL failed: %w", err)
	}
	return nil
}

// ExternalTableExists checks sys.external_tables for a table created by CETAS.
func (e *SynapseExecutor) ExternalTableExists(ctx context.Context, tableName string) (bool, error) {
	if e.db == nil {
		return false, fmt.Errorf("synapse not connected")
	}
	var n int
	if err := e.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.external_tables WHERE name = @p1", tableName,
	).Scan(&n); err != nil {
		return false, fmt.Errorf("check external table %s: %w", tableName, err)
	}
	return n > 0, nil
}

// escapeSQL single-quote-escapes a string for Synapse DDL.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func parseSASConnectionString(connStr string) (accountName, sasToken string, err error) {
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

func parseConnectionString(connStr string) (accountName, accountKey string, err error) {
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

func generateWriteSAS(accountName, accountKey, container string, expiry time.Duration) (string, error) {
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return "", fmt.Errorf("shared key credential: %w", err)
	}
	values := sas.BlobSignatureValues{
		Version:       sas.Version,
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(expiry),
		ContainerName: container,
		Permissions:   (&sas.ContainerPermissions{Read: true, Write: true, Create: true, List: true, Delete: true}).String(),
	}
	queryParams, err := values.SignWithSharedKey(cred)
	if err != nil {
		return "", fmt.Errorf("sign SAS: %w", err)
	}
	return queryParams.Encode(), nil
}
