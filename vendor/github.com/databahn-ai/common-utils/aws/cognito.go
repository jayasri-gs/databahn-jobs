package aws

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitoTypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var logging = logger.GetLogger()

type ChangePasswordRequest struct {
	AuthToken        string
	PreviousPassword string
	ProposedPassword string
}

type ForgetPasswordRequest struct {
	ClientId         string
	ConfirmationCode string
	Username         string
	Password         string
}

type NewPasswordRequest struct {
	ChallengeName     string
	ClientId          string
	Session           string
	ChallengeResponse map[string]string
}

// CognitoAuthenticationClaim represents the authentication claim resulting from a Cognito authentication on AWS API Gateway.
type CognitoAuthenticationClaim struct {
	Aud             string `json:"aud"`
	AuthTime        string `json:"auth_time"`
	CognitoUsername string `json:"cognito:username"`
	Email           string `json:"email"`
	EmailVerified   string `json:"email_verified"`
	EventID         string `json:"event_id"`
	Exp             string `json:"exp"`
	Iat             string `json:"iat"`
	Iss             string `json:"iss"`
	Jti             string `json:"jti"`
	Name            string `json:"name"`
	OriginJti       string `json:"origin_jti"`
	Sub             string `json:"sub"`
	TokenUse        string `json:"token_use"`
}

// DecodeCognitoAuthorizerData converts the "authorizer" data for users authenticated by Cognito on an AWS API Gateway
// into a managed CognitoAuthenticationClaim struct for later use. While this is authentication data, it's
// referred to as coming from the cognito "authorizer"
func DecodeCognitoAuthorizerData(authorizerData map[string]interface{}) (cad CognitoAuthenticationClaim, err error) {
	marshalled, err := json.Marshal(authorizerData["claims"])
	if err != nil {
		logging.Error("Error marshalling cognito authorizer data", zap.Error(err))
		return cad, err
	}
	// logging.GetLogger().Debug("marshalled data", zap.String("authorizerData", string(marshalled)))
	err = json.Unmarshal(marshalled, &cad)
	if err != nil {
		logging.Error("Error unmarshalling cognito authorizer data", zap.Error(err))
	}
	// logging.Debug("marshalled data", zap.String("marshalled data", fmt.Sprintf("%+v", cad)))

	return cad, nil
}

// CognitoFetchUser User will perform sign up action to support registration flow
func CognitoFetchUser(ctx context.Context, region string, userPoolId string, userEmail string) (*cognitoidentityprovider.AdminGetUserOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	getUserRequest := &cognitoidentityprovider.AdminGetUserInput{
		UserPoolId: &userPoolId,
		Username:   &userEmail,
	}

	return client.AdminGetUser(ctx, getUserRequest)
}

// getCognitoClient returns the cognito client of aws. Input region
func getCognitoClient(region string) (client *cognitoidentityprovider.Client, err error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(), awsConfig.WithRegion(region))
	if err != nil {
		logging.Warn("issue getting s3 configuration", zap.Error(err))
		return client, err
	}
	client = cognitoidentityprovider.NewFromConfig(cfg)
	return client, err
}

// CognitoInviteUser is built to invite user to system
func CognitoInviteUser(ctx context.Context, region string, userPoolId string, userEmail string) (*cognitoidentityprovider.AdminCreateUserOutput, error) {

	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	CognitoEmailidAttr := "email"

	userAttributeData := []cognitoTypes.AttributeType{{Name: &CognitoEmailidAttr, Value: &userEmail}, {Name: aws.String("name"), Value: &userEmail}, {Name: aws.String("email_verified"), Value: aws.String("true")}}
	inviteUserInput := &cognitoidentityprovider.AdminCreateUserInput{
		UserPoolId: &userPoolId,
		Username:   &userEmail,
		DesiredDeliveryMediums: []cognitoTypes.DeliveryMediumType{
			cognitoTypes.DeliveryMediumTypeEmail,
		},
		UserAttributes: userAttributeData,
	}

	return client.AdminCreateUser(ctx, inviteUserInput)
}

// CognitoSetNewPassword will set new password for user whos password change request initiated by admin(NEW_PASSWORD_REQUIRED)
func CognitoSetNewPassword(ctx context.Context, region string, request NewPasswordRequest) (*cognitoidentityprovider.RespondToAuthChallengeOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logging.Error("error while creating cognito client", zap.Error(err), zap.String("component", "cognito"))
		return nil, err
	}
	logger.GetLoggerWithContext(ctx).Info("performing set password request in cognito")
	input := &cognitoidentityprovider.RespondToAuthChallengeInput{
		ChallengeName:      cognitoTypes.ChallengeNameTypeNewPasswordRequired,
		ChallengeResponses: request.ChallengeResponse,
		ClientId:           &request.ClientId,
		Session:            &request.Session,
	}
	return client.RespondToAuthChallenge(ctx, input)
}

// CognitoConfirmForgetPassword is build to update user password using auth token.
func CognitoConfirmForgetPassword(ctx context.Context, region string, request ForgetPasswordRequest) (*cognitoidentityprovider.ConfirmForgotPasswordOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logging.Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}
	logger.GetLoggerWithContext(ctx).Info("performing change password")
	changePasswordInput := &cognitoidentityprovider.ConfirmForgotPasswordInput{
		ConfirmationCode: &request.ConfirmationCode,
		Password:         &request.Password,
		Username:         &request.Username,
		ClientId:         &request.ClientId,
	}

	return client.ConfirmForgotPassword(ctx, changePasswordInput)
}

// CognitoForgetPassword is build to update user password using auth token.
func CognitoForgetPassword(ctx context.Context, username string, region string, clientId string) (*cognitoidentityprovider.ForgotPasswordOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logging.Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}
	logger.GetLoggerWithContext(ctx).Info("performing change password")
	forgetPasswordInput := &cognitoidentityprovider.ForgotPasswordInput{
		Username: &username,
		ClientId: &clientId,
	}

	return client.ForgotPassword(ctx, forgetPasswordInput)
}

// CognitoChangePassword is build to update user password using auth token.
func CognitoChangePassword(ctx context.Context, region string, request ChangePasswordRequest) (*cognitoidentityprovider.ChangePasswordOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logging.Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}
	logger.GetLoggerWithContext(ctx).Info("performing change password")
	changePasswordInput := &cognitoidentityprovider.ChangePasswordInput{
		PreviousPassword: &request.PreviousPassword,
		ProposedPassword: &request.ProposedPassword,
		AccessToken:      &request.AuthToken,
	}

	return client.ChangePassword(ctx, changePasswordInput)
}

// AdminSetPassword is created to set new password for any user by admin. Needs to be called via admin api
func AdminSetPassword(ctx context.Context, region string, userPoolId string, cognitoId string, newPassword string) error {
	client, err := getCognitoClient(region)
	if err != nil {
		logging.Error("error while creating cognito client", zap.Error(err))
		return err
	}

	adminSetUserPasswordInput := &cognitoidentityprovider.AdminSetUserPasswordInput{
		Username:   &cognitoId,
		UserPoolId: &userPoolId,
		Password:   &newPassword,
		Permanent:  false,
	}
	_, err = client.AdminSetUserPassword(ctx, adminSetUserPasswordInput)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while setting cognito user password", zap.Error(err))
		return err
	}
	return nil
}

// AdminResetPassword is created to reset password for any user by admin. Needs to be called via admin api
func AdminResetPassword(ctx context.Context, region string, userPoolId string, cognitoId string) error {
	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while creating cognito client", zap.Error(err))
		return err
	}

	adminResetUserPasswordInput := &cognitoidentityprovider.AdminResetUserPasswordInput{
		Username:   &cognitoId,
		UserPoolId: &userPoolId,
	}
	_, err = client.AdminResetUserPassword(ctx, adminResetUserPasswordInput)
	if err != nil {
		logging.Error("error while resetting cognito user password", zap.Error(err))
		return err
	}
	return nil
}

func GetCognitoErrorMessage(err error) string {
	return strings.Split(err.Error(), "Exception:")[1]
}

// CognitoSignUp User will perform sign up action to support registration flow
func CognitoSignUp(ctx context.Context, region string, userEmail string, clientId string, password string) (*cognitoidentityprovider.SignUpOutput, error) {
	client, err := getCognitoClient(region)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while creating cognito client", zap.Error(err))
		return nil, err
	}

	COGNITO_EMAILID_ATTR := "email"

	userAttributeData := []cognitoTypes.AttributeType{
		{Name: &COGNITO_EMAILID_ATTR, Value: &userEmail},
		{Name: aws.String("name"), Value: &userEmail},
	}

	signUpRequest := &cognitoidentityprovider.SignUpInput{
		ClientId:       &clientId,
		Password:       &password,
		Username:       &userEmail,
		UserAttributes: userAttributeData,
	}

	return client.SignUp(ctx, signUpRequest)
}
