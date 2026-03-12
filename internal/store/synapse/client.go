package synapse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	mssql "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"
)

const (
	configSynapseWorkspace = "search.synapse.workspace"
	configSynapseDatabase  = "search.synapse.database"

	// synapseServerlessSuffix is appended to workspace name to form the serverless SQL endpoint (e.g. myworkspace -> myworkspace-ondemand.sql.azuresynapse.net).
	synapseServerlessSuffix = "-ondemand.sql.azuresynapse.net"

	// azureSQLScope is the scope for Azure SQL / Synapse token acquisition (Managed Identity, DefaultAzureCredential, etc.).
	azureSQLScope = "https://database.windows.net//.default"
)

var (
	connOnce sync.Once
	conn     *sql.DB
	initErr  error
)

// GetDB returns a singleton database connection to Azure Synapse (serverless SQL).
// Uses Azure Managed Identity (DefaultAzureCredential): in Azure (e.g. AKS, App Service) MI is used;
// locally, Azure CLI or env credentials can be used. Config: search.synapse.workspace, search.synapse.database.
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

		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			initErr = fmt.Errorf("Azure credential for Synapse: %w", err)
			logger.GetLogger().Error("failed to create Azure credential", zap.Error(err))
			return
		}
		tokenProvider := func() (string, error) {
			tk, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{azureSQLScope}})
			if err != nil {
				return "", err
			}
			return tk.Token, nil
		}

		dsn := buildSynapseDSN(server, database)
		connector, err := mssql.NewAccessTokenConnector(dsn, tokenProvider)
		if err != nil {
			initErr = fmt.Errorf("Synapse token connector: %w", err)
			logger.GetLogger().Error("failed to create Synapse connector", zap.Error(err))
			return
		}
		conn = sql.OpenDB(connector)
		if err := conn.PingContext(ctx); err != nil {
			_ = conn.Close()
			conn = nil
			initErr = fmt.Errorf("Synapse ping failed: %w", err)
			logger.GetLogger().Error("Synapse connection ping failed", zap.Error(err))
			return
		}
		logger.GetLogger().Info("Synapse client initialized (Azure MI)",
			zap.String("workspace", workspace),
			zap.String("database", database))
	})
	if initErr != nil {
		return nil, initErr
	}
	return conn, nil
}

// buildSynapseDSN returns a DSN for Synapse without user/password (used with token auth).
func buildSynapseDSN(server, database string) string {
	query := url.Values{}
	query.Set("database", database)
	query.Set("encrypt", "true")
	query.Set("TrustServerCertificate", "false")
	query.Set("hostNameInCertificate", "*.database.windows.net")
	query.Set("connection timeout", "30")
	u := &url.URL{
		Scheme:   "sqlserver",
		Host:     server,
		RawQuery: query.Encode(),
	}
	return u.String()
}
