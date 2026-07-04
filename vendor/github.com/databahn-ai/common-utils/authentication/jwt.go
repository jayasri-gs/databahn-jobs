package authentication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/databahn-ai/common-utils/utils"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/go-chi/render"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
)

type Authentication struct {
	Realm                   string
	AuthUrl                 string
	TokenValidationEndpoint string
	NoAuthRoutes            []string
	HeaderAuthKey           string
}

func NewCustom(realm, authUrl, headerAuthKey, tokenValidationEndpoint string, noAuthHeaders []string) *Authentication {
	return &Authentication{
		Realm:                   utils.GetValueOrDefault(realm, DefaultRealm),
		AuthUrl:                 utils.UseOrAddProtocol(authUrl),
		TokenValidationEndpoint: tokenValidationEndpoint,
		NoAuthRoutes:            noAuthHeaders,
		HeaderAuthKey:           utils.GetValueOrDefault(headerAuthKey, DefaultAuthHeader),
	}
}

func NewKeycloak(keycloakHost string) *Authentication {
	return &Authentication{
		Realm:                   DefaultRealm,
		AuthUrl:                 utils.UseOrAddProtocol(keycloakHost),
		TokenValidationEndpoint: fmt.Sprintf(DefaultTokenValidationEndpoint, DefaultRealm),
		NoAuthRoutes:            NoAuthRoutes,
		HeaderAuthKey:           DefaultAuthHeader,
	}
}

func (a *Authentication) JwtAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logging.GetLoggerWithContext(ctx).Debug("In JWT authentication", zap.String("uri", r.RequestURI))
		if utils.ListContains(a.NoAuthRoutes, r.RequestURI) {
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		authToken := r.Header.Get(a.HeaderAuthKey)

		if authToken == "" {
			render.Status(r, http.StatusUnauthorized)
			render.JSON(w, r, "token not provided")
			return
		}
		claims, err := a.validateAuthToken(ctx, strings.Replace(authToken, "Bearer ", "", 1))

		if err != nil {
			if err.Error() != "Token is expired" {
				logging.GetLoggerWithContext(ctx).Error("error while validating token", zap.Error(err))
				render.Status(r, http.StatusUnauthorized)
				render.JSON(w, r, "token expired")
				return
			} else {
				logging.GetLoggerWithContext(ctx).Debug("error while validating token", zap.Error(err))
				render.Status(r, http.StatusUnauthorized)
				render.JSON(w, r, "token validation failed")
				return
			}
		}

		if claims != nil {
			logging.GetLoggerWithContext(ctx).Debug("claim validated, setting up context")
			association := claims["association"].(map[string]interface{})
			ctx = context.WithValue(ctx, TenantUuid, getStringFromClaimOrNil(association, "tenantId"))
			ctx = context.WithValue(ctx, CustomerUuid, getStringFromClaimOrNil(association, "customerId"))
			ctx = context.WithValue(ctx, UserUuid, getStringFromClaimOrNil(claims, "sub"))
			ctx = context.WithValue(ctx, UserEmailId, getStringFromClaimOrNil(claims, "email"))
			ctx = context.WithValue(ctx, UserFullName, getStringFromClaimOrNil(claims, "name"))
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			render.Status(r, http.StatusUnauthorized)
			render.JSON(w, r, "user with given id doesnt exists in system")
			return
		}
	})
}

func getStringFromClaimOrNil(claim jwt.MapClaims, key string) string {
	if claim[key] == nil {
		return ""
	} else {
		return claim[key].(string)
	}
}

func (a *Authentication) validateAuthToken(ctx context.Context, token string) (jwt.MapClaims, error) {

	options := keyfunc.Options{
		RefreshErrorHandler: func(err error) {
			logging.GetLoggerWithContext(ctx).Error("error while checking refresh token", zap.Error(err))
		},
		RefreshInterval:   time.Hour,
		RefreshRateLimit:  time.Minute * 5,
		RefreshTimeout:    time.Second * 10,
		RefreshUnknownKID: true,
	}
	jwks, err := keyfunc.Get(fmt.Sprintf("%s%s", a.AuthUrl, a.TokenValidationEndpoint), options)
	if err != nil {
		return nil, err
	}
	parsedToken, err := jwt.Parse(token, jwks.Keyfunc)
	if err != nil {
		return nil, err
	}

	if !parsedToken.Valid {
		return nil, errors.New("invalid token")
	}
	logging.GetLoggerWithContext(ctx).Debug("validated token")

	if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok {
		if claims["association"] == nil {
			return nil, errors.New("claim is invalid")
		}
		return claims, nil
	}
	return nil, errors.New("claim is invalid")
}
