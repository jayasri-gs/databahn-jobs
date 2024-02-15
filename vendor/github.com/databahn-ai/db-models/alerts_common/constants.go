package alerts_common

const InfoAlert = "info"
const WarningAlert = "warning"
const SevereAlert = "severe"

const EdgeFunctionality = "edge"
const LogSourceFunctionality = "log-source"
const CloudLogSourceFunctionality = "cloud-log-source"
const DispenserFunctionality = "dispenser"

const EdgeNodeHealthCheck = "edge-node-health-check"
const EdgeNodeHealthCheckTitle = "Edge health not reported for more than 15 minutes"
const EdgeNodeHealthCheckMessage = "Edge health not reported for more than 15 minutes"

const LogSourceStatsNotReceived = "log-source-activity-check"
const LogSourceStatsNotReceivedTitle = "Events not received for last 15 minutes"
const LogSourceStatsNotReceivedMessage = "Events not received for last 15 minutes"

const TokenValidationFailed = "token-validation"
const TokenValidationFailedTitle = "Invalid credentials"
const TokenValidationFailedMessage = "Marking Ingestion for log source temporarily disabled due to invalid/incorrect authentication token. Please verify source configuration."

const S3DispenserOnboarding = "s3-dispenser-onboarding"

const S3CredentialsInvalidTitle = "Invalid Credentials. Disabling S3 destination"
const S3CredentialsInvalidMessage = "Invalid aws credentials"

const S3BucketNotExistsTitle = "S3 bucket doesn't exist"
const S3BucketNotExistsMessage = "S3 bucket doesn't exist"

const S3SqsConnectorOnboarding = "s3-sqs-onboarding"

const S3SqsCredentialsInvalidTitle = "Invalid Credentials. Disabling cloud log-source"
const S3SqsCredentialsInvalidMessage = "Invalid aws credentials"

const SqsQueueNotExistsTitle = "Sqs Queue doesn't exist"
const SqsQueueNotExistsMessage = "Sqs Queue doesn't exist"

const SyslogDestinationUnavailable = "syslog-destination-unavailable"
const SyslogDestinationUnavailableTitle = "Syslog destination is down or unavailable."
const SyslogDestinationUnavailableMessage = "Marking destination temporarily disabled. Please verify destination configuration or contact admin."

//Other error messages/alert messages

const InvalidCredentials = "Invalid credentials"
const DestinationUnavailable = "Destination unavailable"
