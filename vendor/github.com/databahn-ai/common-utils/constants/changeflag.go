package constants

const (
	ActionAdd      = "add"
	ActionUpdate   = "update"
	ActionDelete   = "delete"
	ActionInactive = "inactive"

	HeaderEntity               = "entity"
	EntityRule                 = "rule"
	EntityLookup               = "lookup"
	EntityEnrichment           = "enrichment"
	EntitySyslogDestination    = "destination_syslog"
	EntityS3Destination        = "destination_s3"
	EntityKafkaDestination     = "destination_kafka"
	EntityElasticDestination   = "destination_elastic_search"
	EntitySnowflakeDestination = "destination_snowflake"
	EntityEventHubDestination  = "destination_event_hub"
	EntityAzureBlobDestination = "destination_azure_blob"
	EntityChronicleDestination = "destination_chronicle"
	EntityDevoDestination      = "destination_devo"
	EntitySource               = "source"
	EntityTransformer          = "transformer"

	ChangeFlagTopic         = "db.management.change.flag"
	ChangeFlagKafkaCluster  = "change_flag_cluster"
	ChangeFlagKafkaConsumer = "change_flag_consumer"
)
