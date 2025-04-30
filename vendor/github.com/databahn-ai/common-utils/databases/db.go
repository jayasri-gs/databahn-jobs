package databases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configs"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/common-utils/vault"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"
	"strings"
	"time"
)

type DatabaseCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Connection struct {
	Credentials  DatabaseCredentials
	Host         string
	Port         string
	DatabaseName string
	SchemaName   string
}

// Connect moved database to one object to avoid multiple functions to connect
func (c *Connection) Connect(ctx context.Context, useTablePrefix bool) (*gorm.DB, error) {
	tablePrefix := ""
	if useTablePrefix {
		tablePrefix = "db_"
	}

	logger := logging.GetLoggerWithContext(ctx)
	dbLogger := zapgorm2.Logger{
		ZapLogger:                 logging.GetLogger(),
		LogLevel:                  glogger.LogLevel(logging.GetLogger().Level()),
		SlowThreshold:             100 * time.Millisecond,
		SkipCallerLookup:          true,
		IgnoreRecordNotFoundError: false,
		Context:                   nil,
	}

	connectString, maskedConnectString := c.getConnectionString()

	logger.Debug("Attempting to connect to database", zap.String("connection string", maskedConnectString))

	db, err := gorm.Open(postgres.Open(connectString), &gorm.Config{
		Logger: dbLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   tablePrefix,
			SingularTable: true,  // use singular table name, table for `User` would be `user` with this option enabled
			NoLowerCase:   false, // skip the snake_casing of names
		},
	})

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Unable to connect to database", zap.Error(err))
		return nil, err
	}
	logging.GetLogger().Debug("connection attempt successful.")
	return db, nil
}

func (c *Connection) ConnectWithSecrets(ctx context.Context, useTablePrefix bool, appConfig configuration.ConfigReader) (*gorm.DB, error) {
	var dbSecrets *DatabaseCredentials
	var err error
	secretBackend := appConfig.GetString(configuration.SecretBackend)
	if secretBackend == "" {
		logging.GetLogger().Info("No secret backend configured. selecting default")
		secretBackend = configuration.SecretBackendAWS
	}

	if appConfig.GetString(configuration.DataBaseSecretName) == "" {
		return nil, fmt.Errorf("database secret name not found in config")
	}
	secretName := appConfig.GetString(configuration.DataBaseSecretName)
	logging.GetLoggerWithContext(ctx).Info("Attempting to connect to database", zap.String("secret name", utils.GetMaskedString(secretName, 5)))
	switch secretBackend {
	case configuration.SecretBackendAWS:
		logging.GetLogger().Info("loading aws secrets")
		dbSecrets, err = ReadDBSecrets(ctx, secretName, appConfig.GetString(configuration.Region))
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching database credentials", zap.Error(err))
			return nil, err
		}
	case configuration.SecretBackendVault:
		logging.GetLogger().Info("loading vault secrets", zap.String("secret name", utils.GetMaskedString(secretName, 5)))
		secretName = strings.TrimPrefix(secretName, "vault://")
		vaultAddress := appConfig.GetString(configuration.VaultAddress)
		vaultToken := appConfig.GetString(configuration.VaultToken)
		if vaultAddress == "" || vaultToken == "" {
			return nil, fmt.Errorf("vault address or token not found in config")
		}
		dbSecrets, err = readDBSecretsFromVault(ctx, secretName, vaultAddress, vaultToken)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching database credentials", zap.Error(err))
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid secret backend: %s", secretBackend)
	}

	c.Credentials = *dbSecrets
	return c.Connect(ctx, useTablePrefix)
}

func (c *Connection) getConnectionString() (string, string) {
	connectString := buildDBConnectStringWith(
		c.Credentials.Username,
		c.Credentials.Password,
		c.Host,
		c.Port,
		c.DatabaseName,
		"30",
	)
	maskedConnectString := buildDBConnectStringWith(
		c.Credentials.Username,
		"****",
		c.Host,
		c.Port,
		c.DatabaseName,
		"30",
	)
	return connectString, maskedConnectString
}

// Initialize DBInit initializes connection to database
// Deprecated: use Connection.Connect method instread
func Initialize(config configuration.ConfigReader) (*gorm.DB, error) {
	connectString, maskedConnectString := buildMysqlConnectString(config)
	dbLogger := zapgorm2.Logger{
		ZapLogger:                 logging.GetLogger(),
		LogLevel:                  glogger.LogLevel(logging.GetLogger().Level()),
		SlowThreshold:             100 * time.Millisecond,
		SkipCallerLookup:          true,
		IgnoreRecordNotFoundError: false,
		Context:                   nil,
	}
	logger := logging.GetLogger()
	logger.Debug("Attempting to connect to database", zap.String("connection string", maskedConnectString))

	db, err := gorm.Open(postgres.Open(connectString), &gorm.Config{
		Logger: dbLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   fmt.Sprintf("%s.", config.GetString(configs.DBDatabase)),
			SingularTable: true,  // use singular table name, table for `User` would be `user` with this option enabled
			NoLowerCase:   false, // skip the snake_casing of names
		},
	})

	if err != nil {
		logger.Error("Unable to connect to database", zap.Error(err))
		return nil, err
	}
	logging.GetLogger().Debug("connection attempt successful.")
	return db, nil
}

func readDBSecretsFromVault(ctx context.Context, secretName, vaultAddress, vaultToken string) (*DatabaseCredentials, error) {
	logging.GetLoggerWithContext(ctx).Info("Attempting to read database credentials",
		zap.String("secret name", secretName), zap.String("vault address", vaultAddress),
		zap.String("vault token", utils.GetMaskedString(vaultToken, 4)))
	if strings.HasPrefix(secretName, "vault://") {
		secretName = strings.TrimPrefix(secretName, "vault://")
	}

	secret, err := vault.ReadSecrets(vaultAddress, vaultToken, secretName)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while reading secret from vault", zap.Error(err))
		return nil, err
	}

	// Unmarshal the secret data into a DatabaseCredentials struct
	creds := &DatabaseCredentials{}
	if username, ok := secret["username"].(string); ok {
		creds.Username = username
	} else {
		return nil, fmt.Errorf("username is missing or not a string in secret data")
	}

	if password, ok := secret["password"].(string); ok {
		creds.Password = password
	} else {
		return nil, fmt.Errorf("password is missing or not a string in secret data")
	}

	logging.GetLoggerWithContext(ctx).Info("Successfully read database credentials")
	return creds, nil
}

func ReadDBSecrets(ctx context.Context, secretName, region string) (*DatabaseCredentials, error) {
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return nil, err
	}

	creds := &DatabaseCredentials{}
	err = json.Unmarshal([]byte(*data.SecretString), &creds)
	if err != nil {
		return nil, err
	}
	return creds, nil
}

// buildMysqlConnectString - returns connect string and masked connect string(for logging) for a mysql db to be used by
// sqlx.Connect() or sql.Open() using values in config (eg OS env variables)
func buildMysqlConnectString(c configuration.ConfigReader) (string, string) {
	connectString := buildDBConnectStringWith(
		c.GetString(configs.DBUsername),
		c.GetString(configs.DBPassword),
		c.GetString(configs.DBHost),
		c.GetString(configs.DBPort),
		c.GetString(configs.DBDatabase),
		"30",
	)
	maskedConnectString := buildDBConnectStringWith(
		c.GetString(configs.DBUsername),
		"*****",
		c.GetString(configs.DBHost),
		c.GetString(configs.DBPort),
		c.GetString(configs.DBDatabase),
		"30",
	)
	return connectString, maskedConnectString
}

// buildMysqlConnectString - returns connect string for a mysql db to be used by sqlx.Connect() or
// sql.Open() using passed parameters
func buildDBConnectStringWith(dbUsername, dbPassword, dbHost, dbPort, dbName string, dbTimeout string) string {
	dsn := "host=%s user=%s password=%s dbname=%s port=%s sslmode=disable connect_timeout=%s"
	return fmt.Sprintf(dsn,
		dbHost,
		dbUsername,
		dbPassword,
		dbName,
		dbPort,
		dbTimeout,
	)
}
