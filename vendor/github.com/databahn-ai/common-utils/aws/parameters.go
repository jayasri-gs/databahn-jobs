package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

func ParameterStoreByName(parameterName, region string) (*ssm.GetParameterOutput, error) {
	input := &ssm.GetParameterInput{
		Name: &parameterName,
	}
	client := ssm.NewFromConfig(getClient(region))

	data, err := client.GetParameter(context.TODO(), input)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return data, err
}
