package alerts_common

const InfoAlert = "info"
const WarningAlert = "warning"
const SevereAlert = "severe"
const CriticalAlert = "critical"

const FleetFunctionality = "fleet"
const LogSourceFunctionality = "log-source"
const CloudLogSourceFunctionality = "cloud-log-source"
const DispenserFunctionality = "dispenser"

const FleetNodeHealthCheck = "fleet-health-checker"
const FleetNodeHealthCheckTitle = "no health received from fleet node for more than %s minutes"
const FleetNodeHealthCheckMessage = "%s node health not reported for more than %s minutes"

const FleetConnectorHealthCheck = "connector-health-checker"
const FleetConnectorHealthCheckTitle = "no health received from fleet connector for more than %s minutes"
const FleetConnectorHealthCheckMessage = "%s connector health not reported for more than %s minutes"

const LogSourceStatsNotReceived = "ingestion-checker"
const LogSourceStatsNotReceivedTitle = "No new events received in the last %s minutes"
const LogSourceStatsNotReceivedMessage = "No new events received in the last %s minutes"

const TokenValidationFailed = "token-validation"
const TokenValidationFailedTitle = "Invalid credentials"
const TokenValidationFailedMessage = "Marking Ingestion for log source temporarily disabled due to invalid/incorrect authentication token. Please verify source configuration."

const S3DispenserOnboarding = "s3-dispenser-onboarding"
const GooglePubsubDispenserOnboarding = "google-pubsub-dispenser-onboarding"
const GoogleStorageDispenserOnboarding = "google-storage-dispenser-onboarding"

const DestinationFunctionality = "destination"
const DestinationStatsNotReceived = "delivery-checker"
const DestinationStatsNotReceivedTitle = "No new events delivered to destination in the last %s minutes"
const DestinationStatsNotReceivedMessage = "No new events delivered to destination in the last %s minutes"

const S3CredentialsInvalidTitle = "Invalid Credentials. Disabling S3 destination"
const S3CredentialsInvalidMessage = "Invalid aws credentials"
const GoogleStorageCredentialsInvalidTitle = "Invalid Credentials. Disabling google cloud storage destination"
const GoogleStorageCredentialsInvalidMessage = "Invalid google storage credentials"
const GooglePubsubCredentialsInvalidTitle = "Invalid Credentials. Disabling google pubsub destination"
const GooglePubsubCredentialsInvalidMessage = "Invalid google pubsub credentials"

const S3BucketNotExistsTitle = "S3 bucket doesn't exist"
const S3BucketNotExistsMessage = "S3 bucket doesn't exist"
const GoogleStorageBucketNotExistsTitle = "Google storage bucket doesn't exist"
const GoogleStorageBucketNotExistsMessage = "Google storage bucket doesn't exist"
const GooglePubsubTopicNotExistsTitle = "Google pubsub topic doesn't exist"
const GooglePubsubTopicNotExistsMessage = "Google pubsub topic doesn't exist"

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

const WhisperingAlertTitle = "Whispering Log Source"
const WhisperingAlertMessage = "Log source is whispering"
const WhisperingAlertType = "whispering-log-source-alert"
const NoisyAlertTitle = "Noisy Log Source"
const NoisyAlertMessage = "Log source is noisy"
const NoisyAlertType = "noisy-log-source-alert"
const SilentAlertTitle = "Silent Log Source"
const SilentAlertMessage = "Log source is silent"
const SilentAlertType = "silent-log-source-alert"

const (
	AlertOpen         = 1
	AlertDismissed    = 2
	AlertResolved     = 3
	AlertAutoResolved = 4
)
