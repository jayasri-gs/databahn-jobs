package pramaan

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
)

/*
PostgresPramaan is a utility to start PostgreSQL for module testing.
It can start PostgreSQL with predefined databases and users to create and manage data for testing.
You can connect to the database and perform operations with it.
*/
type PostgresPramaan struct {
	t           TestLogger
	Container   *postgres.PostgresContainer
	ExternalURL string
	NetworkURL  string
	db          *sql.DB
	username    string
	password    string
	database    string
	host        string
	port        string
}

const (
	postgresInternalNetworkDns = "pramaanpostgres"
	postgresPort               = "5432"
	defaultUsername            = "db_root"
	defaultPassword            = "db_root"
	defaultDatabase            = "postgres"
)

func NewPostgresPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *PostgresPramaan {
	// Create PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithUsername(defaultUsername),
		postgres.WithPassword(defaultPassword),
		postgres.WithDatabase(defaultDatabase),
		network.WithNetwork([]string{postgresInternalNetworkDns}, dockerNetwork),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("Could not create postgres container %v", err)
	}

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get host: %v", err)
	}

	port, err := postgresContainer.MappedPort(ctx, postgresPort)
	if err != nil {
		log.Fatalf("failed to get port: %v", err)
	}

	externalURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		defaultUsername, defaultPassword, host, port.Num(), defaultDatabase)
	networkURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		defaultUsername, defaultPassword, postgresInternalNetworkDns, postgresPort, defaultDatabase)

	// Connect to the database
	db, err := sql.Open("postgres", externalURL)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping postgres: %v", err)
	}

	return &PostgresPramaan{
		t:           t,
		Container:   postgresContainer,
		ExternalURL: externalURL,
		NetworkURL:  networkURL,
		db:          db,
		username:    defaultUsername,
		password:    defaultPassword,
		database:    defaultDatabase,
		host:        host,
		port:        port.Port(),
	}
}

/*
GetDB returns the database connection for direct operations.
*/
func (p *PostgresPramaan) GetDB() *sql.DB {
	return p.db
}

/*
ExecuteQuery executes a query and returns the result.
*/
func (p *PostgresPramaan) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	return rows, nil
}

/*
ExecuteCommand executes a command (INSERT, UPDATE, DELETE) and returns the result.
*/
func (p *PostgresPramaan) ExecuteCommand(ctx context.Context, command string, args ...interface{}) (sql.Result, error) {
	result, err := p.db.ExecContext(ctx, command, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}
	return result, nil
}

/*
CreateTable creates a table with the specified schema.
*/
func (p *PostgresPramaan) CreateTable(ctx context.Context, tableName, schema string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, schema)
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %v", tableName, err)
	}
	return nil
}

/*
DropTable drops a table if it exists.
*/
func (p *PostgresPramaan) DropTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %v", tableName, err)
	}
	return nil
}

/*
InsertData inserts data into a table.
*/
func (p *PostgresPramaan) InsertData(ctx context.Context, tableName string, columns []string, values []interface{}) error {
	placeholders := ""
	for i := range values {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	columnList := ""
	for i, col := range columns {
		if i > 0 {
			columnList += ", "
		}
		columnList += col
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholders)
	_, err := p.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert data into %s: %v", tableName, err)
	}
	return nil
}

/*
GetExternalURL returns the external URL for accessing PostgreSQL.
*/
func (p *PostgresPramaan) GetExternalURL() string {
	return p.ExternalURL
}

/*
GetNetworkURL returns the internal network URL for accessing PostgreSQL.
*/
func (p *PostgresPramaan) GetNetworkURL() string {
	return p.NetworkURL
}

func (p *PostgresPramaan) GetUsername() string {
	return p.username
}

func (p *PostgresPramaan) GetPassword() string {
	return p.password
}

func (p *PostgresPramaan) GetDatabase() string {
	return p.database
}

func (p *PostgresPramaan) GetHost() string {
	return p.host
}

func (p *PostgresPramaan) GetNetworkHost() string {
	return postgresInternalNetworkDns
}

func (p *PostgresPramaan) GetNetworkPort() string {
	return postgresPort
}

func (p *PostgresPramaan) GetPort() string {
	return p.port
}
