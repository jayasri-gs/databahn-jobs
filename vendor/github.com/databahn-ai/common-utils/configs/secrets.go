package configs

import (
	"encoding/json"
	"github.com/spf13/viper"
	"log"
	"os"

	"github.com/databahn-ai/common-utils/aws"
)

type DatabaseCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type OpenSearchCredentials struct {
	Url                 string `json:"url"`
	Username            string `json:"username"`
	Password            string `json:"password"`
	StatsIndexName      string `json:"statsIndexName"`
	StatisticsIndexName string `json:"statisticsIndexName"`
}

func loadSecrets(config *viper.Viper) error {
	err := loadDbSecrets(config)
	if err != nil {
		return err
	}
	err = loadOpenSearchSecrets(config)
	if err != nil {
		return err
	}
	return nil
}

func loadDbSecrets(config *viper.Viper) error {
	data, err := aws.ReadSecretByName(config.GetString(DbSecretsName))
	if err != nil {
		return err
	}

	databaseCredentials := DatabaseCredentials{}

	err = json.Unmarshal([]byte(*data.SecretString), &databaseCredentials)

	if err != nil {
		return err
	}
	if os.Getenv("LOG_LEVEL") == "debug" {
		log.Println("secret loaded successfully")
	}
	setDatabaseCredentials(config, databaseCredentials)
	return nil
}

func loadOpenSearchSecrets(config *viper.Viper) error {
	data, err := aws.ReadSecretByName(config.GetString(OpenSearchSecretsName))
	if err != nil {
		return err
	}

	creds := OpenSearchCredentials{}
	err = json.Unmarshal([]byte(*data.SecretString), &creds)
	if err != nil {
		return err
	}
	if os.Getenv("LOG_LEVEL") == "debug" {
		log.Println("secret loaded successfully")
	}
	setOpenSearchCredentials(config, creds)
	return nil
}

func setDatabaseCredentials(config *viper.Viper, db DatabaseCredentials) {
	config.Set(DBUsername, db.Username)
	config.Set(DBPassword, db.Password)
}

func setOpenSearchCredentials(config *viper.Viper, creds OpenSearchCredentials) {
	config.Set(OpenSearchUrl, creds.Url)
	config.Set(OpenSearchUserName, creds.Username)
	config.Set(OpenSearchPassword, creds.Password)
	config.Set(OpenSearchStatsIndex, creds.StatsIndexName)
	config.Set(OpenSearchStatisticsIndex, creds.StatisticsIndexName)
}
