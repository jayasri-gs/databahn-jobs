package query

import (
	"strings"
	"testing"
)

func TestCETASTableName(t *testing.T) {
	if got := CETASTableName("abc12345", 0); got != "staging_abc12345_0000" {
		t.Errorf("got %q, want staging_abc12345_0000", got)
	}
	if got := CETASTableName("abc12345", 999); got != "staging_abc12345_0999" {
		t.Errorf("got %q, want staging_abc12345_0999", got)
	}
}

func TestCETASStagingPrefix(t *testing.T) {
	got := CETASStagingPrefix("report-uuid-1234", 2)
	if got != "databahn_out/report-uuid-1234/chunk_0002/" {
		t.Errorf("got %q", got)
	}
}

func TestCETASFileFormatName(t *testing.T) {
	if CETASFileFormatName("csv", "abc12345") != "DatabahnCETASCSV_abc12345" {
		t.Errorf("csv format name wrong")
	}
	if CETASFileFormatName("json", "abc12345") != "DatabahnCETASParquet_abc12345" {
		t.Errorf("json format name wrong")
	}
	if CETASFileFormatName("xlsx", "abc12345") != "DatabahnCETASParquet_abc12345" {
		t.Errorf("xlsx format name wrong")
	}
}

func TestCETASCredentialDDL(t *testing.T) {
	ddl := CETASCredentialDDL("MyCred", "SHARED ACCESS SIGNATURE", "sv=2023&sig=abc")
	if !strings.Contains(ddl, "CREATE DATABASE SCOPED CREDENTIAL [MyCred]") {
		t.Error("missing CREATE DATABASE SCOPED CREDENTIAL")
	}
	if !strings.Contains(ddl, "IDENTITY = 'SHARED ACCESS SIGNATURE'") {
		t.Error("missing IDENTITY")
	}
	if !strings.Contains(ddl, "SECRET = 'sv=2023&sig=abc'") {
		t.Error("missing SECRET")
	}
}

func TestCETASCredentialDropIfExistsDDL(t *testing.T) {
	ddl := CETASCredentialDropIfExistsDDL("MyCred")
	if !strings.Contains(ddl, "DROP DATABASE SCOPED CREDENTIAL [MyCred]") {
		t.Error("missing DROP")
	}
	if !strings.Contains(ddl, "sys.database_credentials") {
		t.Error("missing existence check")
	}
}

func TestCETASDataSourceDDL(t *testing.T) {
	ddl := CETASDataSourceDDL("MyDS", "MyCred", "https://account.blob.core.windows.net/container")
	if !strings.Contains(ddl, "CREATE EXTERNAL DATA SOURCE [MyDS]") {
		t.Error("missing CREATE EXTERNAL DATA SOURCE")
	}
	if !strings.Contains(ddl, "LOCATION = 'https://account.blob.core.windows.net/container'") {
		t.Error("missing LOCATION")
	}
	if !strings.Contains(ddl, "CREDENTIAL = [MyCred]") {
		t.Error("missing CREDENTIAL")
	}
}

func TestCETASFileFormatDDL_CSV(t *testing.T) {
	ddl := CETASFileFormatDDL("DatabahnCETASCSV_abc12345", "csv", ",")
	if !strings.Contains(ddl, "CREATE EXTERNAL FILE FORMAT [DatabahnCETASCSV_abc12345]") {
		t.Error("missing CREATE EXTERNAL FILE FORMAT")
	}
	if !strings.Contains(ddl, "FORMAT_TYPE = DELIMITEDTEXT") {
		t.Error("missing DELIMITEDTEXT")
	}
	if !strings.Contains(ddl, "FIELD_TERMINATOR = ','") {
		t.Error("missing delimiter")
	}
}

func TestCETASFileFormatDDL_Parquet(t *testing.T) {
	ddl := CETASFileFormatDDL("DatabahnCETASParquet_abc12345", "json", "")
	if !strings.Contains(ddl, "FORMAT_TYPE = PARQUET") {
		t.Error("missing PARQUET")
	}
	if strings.Contains(ddl, "DELIMITEDTEXT") {
		t.Error("should not contain DELIMITEDTEXT for parquet")
	}
}

func TestCETASTableDDL(t *testing.T) {
	ddl := CETASTableDDL("staging_abc12345_0000", "MyDS", "databahn_out/report/chunk_0000/", "DatabahnFmt", "SELECT * FROM t")
	if !strings.Contains(ddl, "CREATE EXTERNAL TABLE [staging_abc12345_0000]") {
		t.Error("missing CREATE EXTERNAL TABLE")
	}
	if !strings.Contains(ddl, "LOCATION = 'databahn_out/report/chunk_0000/'") {
		t.Error("missing LOCATION")
	}
	if !strings.Contains(ddl, "DATA_SOURCE = [MyDS]") {
		t.Error("missing DATA_SOURCE")
	}
	if !strings.Contains(ddl, "FILE_FORMAT = [DatabahnFmt]") {
		t.Error("missing FILE_FORMAT")
	}
	if !strings.Contains(ddl, "SELECT * FROM t") {
		t.Error("missing SELECT statement")
	}
}

func TestCETASTableDropIfExistsDDL(t *testing.T) {
	ddl := CETASTableDropIfExistsDDL("staging_abc12345_0000")
	if !strings.Contains(ddl, "sys.external_tables") {
		t.Error("missing sys.external_tables check")
	}
	if !strings.Contains(ddl, "DROP EXTERNAL TABLE [staging_abc12345_0000]") {
		t.Error("missing DROP EXTERNAL TABLE")
	}
}

func TestEscapeSQL_SingleQuote(t *testing.T) {
	got := escapeSQL("O'Brien")
	if got != "O''Brien" {
		t.Errorf("got %q, want O''Brien", got)
	}
}

func TestParseConnectionString(t *testing.T) {
	connStr := "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=abc123==;EndpointSuffix=core.windows.net"
	name, key, err := parseConnectionString(connStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "myaccount" {
		t.Errorf("account name = %q, want myaccount", name)
	}
	if key != "abc123==" {
		t.Errorf("account key = %q, want abc123==", key)
	}
}

func TestParseConnectionString_Missing(t *testing.T) {
	_, _, err := parseConnectionString("DefaultEndpointsProtocol=https")
	if err == nil {
		t.Error("expected error for connection string missing AccountName/AccountKey")
	}
}
