package aws

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"log"
)

func ReadSecretByName(secretName string, region string) (data *secretsmanager.GetSecretValueOutput, err error) {
	sm := secretsmanager.NewFromConfig(getClient(region))

	smInput := secretsmanager.GetSecretValueInput{SecretId: &secretName}

	data, err = sm.GetSecretValue(context.TODO(), &smInput)

	if err != nil {
		log.Println("error while reading db credentials", err)
		return nil, err
	}

	return data, nil
}

// ! getClient may panic
func getClient(region string) aws.Config {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(region))
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	return cfg
}

func CreateSecret(secretName string, secretString string, region string) (data *secretsmanager.CreateSecretOutput, err error) {
	sm := secretsmanager.NewFromConfig(getClient(region))

	input := &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(secretString),
	}
	res, err := sm.CreateSecret(context.TODO(), input)
	if err != nil {
		log.Println("error while reading creating secret", err)
		return nil, err
	}

	return res, nil
}

func CreateJSONSecret(secretId string, secretKey string, secretValue string, region string) error {
	sm := secretsmanager.NewFromConfig(getClient(region))

	secretMap := make(map[string]string)
	secretMap[secretKey] = secretValue
	secret, err := json.Marshal(secretMap)
	if err != nil {
		log.Println("unable to marshal secret map", err)
		return err
	}
	input := &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretId),
		SecretString: aws.String(string(secret)),
	}
	_, err = sm.CreateSecret(context.TODO(), input)
	if err != nil {
		log.Println("unable to create secret", err)
		return err
	}
	return nil
}

func AppendJSONSecret(secretId string, secretKey string, secretValue, region string) error {
	sm := secretsmanager.NewFromConfig(getClient(region))

	//If secret is not present create secret
	exist, err := checkSecretExists(secretId, region)
	if err != nil {
		return err
	}

	if !exist {
		return CreateJSONSecret(secretId, secretKey, secretValue, region)
	}

	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretId),
	}

	// Get current secret value
	res, err := sm.GetSecretValue(context.TODO(), input)
	if err != nil {
		log.Println("error while getting secret value", err)
		return err
	}

	// Parse current secret value into a map
	secretMap := make(map[string]string)
	err = json.Unmarshal([]byte(*res.SecretString), &secretMap)
	if err != nil {
		log.Println("unable to deserialize secret", err)
		return err
	}

	// Add new key value pair to the map
	secretMap[secretKey] = secretValue
	updatedSecret, err := json.Marshal(secretMap)
	if err != nil {
		log.Println("unable to serialize updated secret", err)
		return err
	}

	// Update secret with new value
	updateInput := &secretsmanager.UpdateSecretInput{
		SecretId:     aws.String(secretId),
		SecretString: aws.String(string(updatedSecret)),
	}
	_, err = sm.UpdateSecret(context.TODO(), updateInput)
	if err != nil {
		log.Println("unable to update secret", err)
		return err
	}
	return nil
}

func checkSecretExists(secretId string, region string) (bool, error) {
	sm := secretsmanager.NewFromConfig(getClient(region))

	input := &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretId),
	}

	// Get current secret value
	_, err := sm.DescribeSecret(context.TODO(), input)
	if err != nil {
		var notFoundError *types.ResourceNotFoundException
		if errors.As(err, &notFoundError) {
			return false, nil
		} else {
			log.Println("unable to describe secret", err)
			return true, err
		}
	}
	return true, err
}
