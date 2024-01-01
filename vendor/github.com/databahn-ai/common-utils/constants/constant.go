package constants

const TenantUuid = "TENANT_UUID"

const StatusCreated = 0
const StatusAccepted = 1
const StatusDeploying = 2
const StatusActive = 3
const StatusDisabled = 4
const StatusDeleted = 5

func IsStatusRunning(status int) bool {
	return status >= 1 && status <= 3
}

const EndpointAvailabilityZoneId = "/placement/availability-zone-id"
const EndpointAvailabilityZone = "/placement/availability-zone"
const EndpointRegion = "/placement/region"
const TeamsNotificationColorGreen = "#64a837"
const TeamsNotificationColorRed = "#d63333"
