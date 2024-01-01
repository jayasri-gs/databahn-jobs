package aws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitoTypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type (
	User struct {
		Username string `json:"username" validate:"required" db:"username"`
		Password string `json:"password" validate:"required"`
		Token    string `json:"token"`
	}

	UserForgot struct {
		Username string `json:"username" validate:"required"`
	}

	Logout struct {
		Token    string
		ClientId string
	}

	UserConfirmationCode struct {
		ConfirmationCode string `json:"confirmationCode" validate:"required"`
		User             User   `json:"user" validate:"required"`
	}

	UserRegister struct {
		Email string `json:"email" validate:"required" db:"email"`
		User  User   `json:"user" validate:"required"`
	}

	// OTP is the struct to handle otp verification.
	OTP struct {
		Username string `json:"username"`
		OTP      string `json:"otp"`
	}
	Response struct {
		Error error `json:"error"`
	}
)

func (a *User) Refresh(c context.Context, clientId string, region string) (output *cognitoidentityprovider.InitiateAuthOutput, err error) {

	if a.Token == "" {
		return nil, errors.New("token is mandatory to refresh")
	}

	authTry := &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow:       cognitoTypes.AuthFlowTypeRefreshTokenAuth,
		AuthParameters: map[string]string{"REFRESH_TOKEN": a.Token},
		ClientId:       aws.String(clientId), // this is the app client ID
	}

	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(c).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	output, err = client.InitiateAuth(c, authTry)

	if err != nil {
		return nil, err
	}
	return output, nil
}

func (a *User) Login(c context.Context, clientId string, region string) (output *cognitoidentityprovider.InitiateAuthOutput, err error) {

	if err := utils.IsValid(a); err != nil {
		return nil, err
	}

	authTry := &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: cognitoTypes.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": a.Username,
			"PASSWORD": a.Password,
		},
		ClientId: aws.String(clientId), // this is the app client ID
	}

	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(c).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	output, err = client.InitiateAuth(c, authTry)

	if err != nil {
		return nil, err
	}
	return output, nil
}

func (l *Logout) Logout(ctx context.Context, region string) (*cognitoidentityprovider.GlobalSignOutOutput, error) {
	client, err := getCognitoClient(region)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	revokeReq := &cognitoidentityprovider.GlobalSignOutInput{
		AccessToken: aws.String(l.Token),
	}
	return client.GlobalSignOut(ctx, revokeReq)
}
