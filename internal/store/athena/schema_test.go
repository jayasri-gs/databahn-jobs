package athena

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

func row(values ...string) types.Row {
	data := make([]types.Datum, len(values))
	for i, v := range values {
		data[i] = types.Datum{VarCharValue: aws.String(v)}
	}
	return types.Row{Data: data}
}

func TestParseDescribeResultRows_HeaderAndData(t *testing.T) {
	rows := []types.Row{
		row("col_name", "data_type", "comment"),
		row("src_ip", "string", ""),
		row("dst_port", "bigint", ""),
	}
	cols := parseDescribeResultRows(rows, true)
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if _, ok := cols["src_ip"]; !ok {
		t.Fatal("expected src_ip")
	}
	if _, ok := cols["dst_port"]; !ok {
		t.Fatal("expected dst_port")
	}
}

func TestParseDescribeResultRows_PaginatedNoHeader(t *testing.T) {
	rows := []types.Row{
		row("host", "string", ""),
		row("user", "string", ""),
	}
	cols := parseDescribeResultRows(rows, false)
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
}

func TestParseDescribeResultRows_PartitionSectionIgnored(t *testing.T) {
	rows := []types.Row{
		row("col_name", "data_type", "comment"),
		row("event_id", "string", ""),
		row("# Partition Information", "", ""),
		row("# col_name", "data_type", "comment"),
		row("dt", "string", ""),
	}
	cols := parseDescribeResultRows(rows, true)
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns (event_id + partition col dt), got %d: %v", len(cols), cols)
	}
	if _, ok := cols["event_id"]; !ok {
		t.Fatal("expected event_id")
	}
	if _, ok := cols["dt"]; !ok {
		t.Fatal("expected partition column dt")
	}
}

func TestParseDescribeResultRows_Empty(t *testing.T) {
	cols := parseDescribeResultRows(nil, true)
	if len(cols) != 0 {
		t.Fatalf("expected empty map, got %v", cols)
	}
}

func TestParseDescribeColumnName_TabSeparatedCell(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"accountname         \tstring", "accountname"},
		{"customstring55      \tstring", "customstring55"},
		{"db_edge_ts          \tdouble", "db_edge_ts"},
		{"`AccountName`\tstring", "accountname"},
		{"  src_ip  ", "src_ip"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := parseDescribeColumnName(tt.raw); got != tt.want {
			t.Fatalf("parseDescribeColumnName(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

func TestParseDescribeResultRows_SingleCellTabSeparated(t *testing.T) {
	rows := []types.Row{
		row("col_name             \tdata_type"),
		row("accountname         \tstring"),
		row("db_edge_ts          \tdouble"),
	}
	cols := parseDescribeResultRows(rows, true)
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}
	if _, ok := cols["accountname"]; !ok {
		t.Fatal("expected accountname")
	}
	if _, ok := cols["db_edge_ts"]; !ok {
		t.Fatal("expected db_edge_ts")
	}
}

func TestParseDescribeResultRows_EmptyColNameSkipped(t *testing.T) {
	rows := []types.Row{
		row("col_name", "data_type", "comment"),
		row("", "string", ""),
		row("valid", "string", ""),
	}
	cols := parseDescribeResultRows(rows, true)
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}
}

func TestBuildDescribeQuery_Valid(t *testing.T) {
	q, err := buildDescribeQuery("databahn_tenant_x", "events")
	if err != nil {
		t.Fatal(err)
	}
	want := "DESCRIBE `databahn_tenant_x`.`events`"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuildDescribeQuery_InvalidDatabase(t *testing.T) {
	_, err := buildDescribeQuery("bad-db", "events")
	if err == nil {
		t.Fatal("expected error for invalid database")
	}
}

func TestIsTableNotFound(t *testing.T) {
	if !IsTableNotFound(&TableNotFoundError{Msg: "x"}) {
		t.Fatal("expected TableNotFoundError to match")
	}
	if !IsTableNotFound(errors.New("TABLE_NOT_FOUND")) {
		t.Fatal("expected message match")
	}
	if IsTableNotFound(errors.New("timeout")) {
		t.Fatal("expected unrelated error to not match")
	}
}

func TestMergeDescribeColumns(t *testing.T) {
	dest := map[string]struct{}{"a": {}}
	mergeDescribeColumns(dest, map[string]struct{}{"b": {}})
	if len(dest) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(dest))
	}
}
