package constants

// LctxLambdaRequestId LCTX stands for Logging context - keys we're setting in context to use in logs.
const LctxLambdaRequestId = iota
const EdgeStatusCreated = 0
const EdgeStatusActive = 1
const EdgeStatusInactive = 2
const EdgeStatusDeleted = 3

const RegisterEdgeEndpoint = "/v1/public/edge"
const ActivationKeyValidateEndPoint = "/v1/public/edge/validate"

const SyslogConnector = "SYSLOG"

var ConnectorType = []string{SyslogConnector}
var _ = []int{EdgeStatusCreated, EdgeStatusInactive, EdgeStatusDeleted}

const HeaderTenantUuid = "tenant_uuid"
const TenantUuid = "TENANT_UUID"
const EdgeUuid = "EDGE_UUID"
const UserUuid = "USER_UUID"
const ActivationCode = "APP_ACTIVATION_CODE"

const RegisterEndpoint = "/v1/register"
const LoggingMode = "LOG_LEVEL"
const LogModeDebug = "DEBUG"
const LogModeInfo = "INFO"
const LogModeWarn = "WARN"
const LogModeError = "ERROR"

const LoggingEncoding = "LOG_ENCODING"
const LoggingJsonEncoding = "json"
const LoggingConsoleEncoding = "console"
