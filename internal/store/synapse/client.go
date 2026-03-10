package synapse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"

	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	_ "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"
)

const (
	configSynapseWorkspace = "search.synapse.workspace"
	configSynapseDatabase  = "search.synapse.database"
	configSearchSecret     = "search.secret_name"

	// synapseServerlessSuffix is appended to workspace name to form the serverless SQL endpoint (e.g. myworkspace -> myworkspace-ondemand.sql.azuresynapse.net).
	synapseServerlessSuffix = "-ondemand.sql.azuresynapse.net"
)

var (
	connOnce sync.Once
	conn     *sql.DB
	initErr  error
)

// GetDB returns a singleton database connection to Azure Synapse (serverless SQL).
// Uses search.synapse.workspace (server host), search.synapse.database from config, and search.synapse.sql_username / search.synapse.sql_password from the search secret (Azure payload).
func GetDB(ctx context.Context) (*sql.DB, error) {
	connOnce.Do(func() {
		cfg := appConfig.GetAppConfiguration()
		workspace := cfg.GetString(configSynapseWorkspace)
		if workspace == "" {
			initErr = fmt.Errorf("%s not configured", configSynapseWorkspace)
			logger.GetLogger().Error("Synapse workspace not configured", zap.String("config_key", configSynapseWorkspace))
			return
		}
		server := strings.TrimSuffix(workspace, "/") + synapseServerlessSuffix

		database := cfg.GetString(configSynapseDatabase)
		if database == "" {
			database = "master"
		}
		secretName := cfg.GetString(configSearchSecret)
		if secretName == "" {
			initErr = fmt.Errorf("%s not configured", configSearchSecret)
			logger.GetLogger().Error("search secret name not configured")
			return
		}
		user, password, err := util.GetAzureSearchSynapseCreds(ctx, cfg, secretName)
		if err != nil {
			initErr = fmt.Errorf("failed to get Synapse credentials from search secret: %w", err)
			logger.GetLogger().Error("failed to get Synapse credentials", zap.Error(err))
			return
		}
		connStr := buildConnString(server, database, user, password)
		conn, initErr = sql.Open("sqlserver", connStr)
		if initErr != nil {
			logger.GetLogger().Error("failed to open Synapse connection", zap.Error(initErr))
			return
		}
		if err := conn.PingContext(ctx); err != nil {
			_ = conn.Close()
			conn = nil
			initErr = fmt.Errorf("Synapse ping failed: %w", err)
			logger.GetLogger().Error("Synapse connection ping failed", zap.Error(err))
			return
		}
		logger.GetLogger().Info("Synapse client initialized",
			zap.String("workspace", workspace),
			zap.String("database", database))
	})
	if initErr != nil {
		return nil, initErr
	}
	return conn, nil
}

func buildConnString(server, database, user, password string) string {
	query := url.Values{}
	query.Set("database", database)
	query.Set("encrypt", "true")
	query.Set("TrustServerCertificate", "false")
	query.Set("hostNameInCertificate", "*.database.windows.net")
	query.Set("connection timeout", "30")
	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, password),
		Host:     server,
		RawQuery: query.Encode(),
	}
	return u.String()
}
