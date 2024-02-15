package authentication

const TenantUuid = "TENANT_UUID"
const CustomerUuid = "CUSTOMER_UUID"
const UserUuid = "USER_UUID"
const UserEmailId = "USER_EMAIL_ID"
const UserFullName = "USER_FULL_NAME"
const keycloakJwksURI = "https://%s/realms/%s/protocol/openid-connect/certs"

var NoAuthRoutes = []string{"/v1/health"}

const DefaultRealm = "databahn"
const DefaultAuthHeader = "Authorization"
const DefaultTokenValidationEndpoint = "/realms/%s/protocol/openid-connect/certs"
const ScopeOpenId = "openid"
const Realm = "databahn"
