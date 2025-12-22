package constants

// Databahn Sandbox destination - alerts for this destination should be skipped
const SandboxDestinationID = "dbd00000-0000-0000-0000-000000000000"
const SandboxDestinationName = "Databahn Sandbox"
const SandboxStorageDestinationID = "dbd00000-0000-0000-0000-000000000001"

const IngestionCheckerFunctionalityType = "ingestion-checker"
const DeliveryCheckerFunctionalityType = "dispenser-checker"
const UnparsedEventCheckerFunctionalityType = "unparsed-event-checker"
const SilentDeviceCheckerFunctionalityType = "silent-device-checker"
const IngestionCheckerFunctionalityTitle = "No new events received in the last %s"
const IngestionCheckerFunctionalityMessage = "This source is configured to be alerted on not receiving data in last %s. As of '%s' last event was received at '%s'."
const DeliveryCheckerFunctionalityTitle = "No new events delivered in the last %s"
const DeliveryCheckerFunctionalityMessage = "This alert is triggered because no new events were delivered to this destination in the last %s. As of '%s' last event was delivered at '%s'."
const UnparsedEventCheckerFunctionalityTitle = "Source '%s' has %d unparsed events (%.2f%% of total events) in the last %d hours"
const UnparsedEventCheckerFunctionalityMessage = "Source '%s' has %d unparsed events, which represents %.2f%% of the total events received."
