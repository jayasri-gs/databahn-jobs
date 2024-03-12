package os

type OpenSearchConf struct {
	Url        string
	Username   string
	Password   string
	StatsIndex string
}

func GetConf() *OpenSearchConf {
	//url := config.GetOpenSearchSecrets().Url
	url := "https://localhost:9200"
	user := "osadmin"
	pass := "6RuMm?g-k~5Wzy^8"
	statsIndex := "db_statistics"
	return &OpenSearchConf{
		Url:        url,
		Username:   user,
		Password:   pass,
		StatsIndex: statsIndex,
	}
}

func (c *OpenSearchConf) Creds() *Creds {
	return &Creds{
		Username: c.Username,
		Password: c.Password,
	}
}
func (c *OpenSearchConf) StatisticsIndexAlias(tenantId string) string {
	return c.StatsIndex + "_alias_" + tenantId

}
