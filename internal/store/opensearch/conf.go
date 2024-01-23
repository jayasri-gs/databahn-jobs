package opensearch

import (
	"github.com/databahn-ai/databahn-jobs/internal/config"
)

type OpenSearchConf struct {
	Url        string
	Username   string
	Password   string
	StatsIndex string
}

func GetConf() *OpenSearchConf {
	url := config.GetOpenSearchSecrets().Url
	user := config.GetOpenSearchSecrets().Username
	pass := config.GetOpenSearchSecrets().Password
	statsIndex := config.GetOpenSearchSecrets().StatisticsIndexName
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
