package query

import "fmt"

func buildSynapseConnectionString(workspace, database, username, password string) string {
	return fmt.Sprintf(
		"server=%s-ondemand.sql.azuresynapse.net;port=1433;database=%s;user id=%s;password=%s;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;connection timeout=60",
		workspace, database, username, password,
	)
}
