package alerts_async

type ErrorCode interface {
	Value() string
	MessageTemplate() string
	privateErrorCode()
}

type errorCodeEnum struct {
	value           string
	messageTemplate string
}

func (f errorCodeEnum) Value() string {
	return f.value
}

func (f errorCodeEnum) MessageTemplate() string {
	return f.messageTemplate
}

func (f errorCodeEnum) privateErrorCode() {
}

var (
	DIOE10001 = errorCodeEnum{value: "DIOE10001", messageTemplate: "Service unavailable. sending data to back up service. {0}"}
	DIOE20001 = errorCodeEnum{value: "DIOE20001", messageTemplate: "Kafka Service unavailable. {0}"}
	DIOE30001 = errorCodeEnum{value: "DIOE30001", messageTemplate: "Service unavailable. {0}"}
	DIOE40001 = errorCodeEnum{value: "DIOE40001", messageTemplate: "Checkpointing failed. {0}"}
	DIOE50001 = errorCodeEnum{value: "DIOE50001", messageTemplate: "File processing failure. {0}"}
	DWBE30001 = errorCodeEnum{value: "DWBE30001", messageTemplate: "HTTP GET call failed. {0}"}
	DWBE30002 = errorCodeEnum{value: "DWBE30002", messageTemplate: "HTTP DELETE call failed. {0}"}
	DWBE30003 = errorCodeEnum{value: "DWBE30003", messageTemplate: "HTTP PUT call failed. {0}"}
	DWBE30004 = errorCodeEnum{value: "DWBE30004", messageTemplate: "HTTP POST call failed. {0}"}
	DWBE30005 = errorCodeEnum{value: "DWBE30005", messageTemplate: "HTTP PATCH call failed. {0}"}
	DWBE30006 = errorCodeEnum{value: "DWBE30006", messageTemplate: "HTTP call failed. {0}"}
	DCFE10001 = errorCodeEnum{value: "DCFE10001", messageTemplate: "Configuration Not Supported. {0}"}
	DCFE10002 = errorCodeEnum{value: "DCFE10002", messageTemplate: "Configuration Not Found. {0}"}
	DCFE10003 = errorCodeEnum{value: "DCFE10003", messageTemplate: "Configuration not processed successfully. {0}"}
	DDTE10001 = errorCodeEnum{value: "DDTE10001", messageTemplate: "Data Conversion Issue. {0}"}
	DDTE10002 = errorCodeEnum{value: "DDTE10002", messageTemplate: "Data Parsing Issue. {0}"}
	DDTE10003 = errorCodeEnum{value: "DDTE10003", messageTemplate: "Data Forwarding Issue. {0}"}
	DDTE10004 = errorCodeEnum{value: "DDTE10004", messageTemplate: "Data Processing Issue. {0}"}
	DNDW10001 = errorCodeEnum{value: "DNDW10001", messageTemplate: "No Data For Logsource Received. {0}"}
	DNDW10002 = errorCodeEnum{value: "DNDW10002", messageTemplate: "No Data For Destination Received. {0}"}
	DNDW10003 = errorCodeEnum{value: "DNDW10003", messageTemplate: "Reputation update for device . {0}"}
	DNDW10004 = errorCodeEnum{value: "DNDW10004", messageTemplate: "Destination threshold limit has exceeded . {0}"}
	DNDW10005 = errorCodeEnum{value: "DNDW10005", messageTemplate: "Volume deviation detected . {0}"}
	DNDW10006 = errorCodeEnum{value: "DNDW10006", messageTemplate: "Incoming/Outgoing data ratio exceeded . {0}"}
	DGRW10001 = errorCodeEnum{value: "DGRW10001", messageTemplate: "System guardrail triggered. {0}"}
	DDBE10001 = errorCodeEnum{value: "DDBE10001", messageTemplate: "Database source unavailable. {0}"}
	DHRW10001 = errorCodeEnum{value: "DHRW10001", messageTemplate: "Agent is unhealthy. {0}"}
	DHRW10002 = errorCodeEnum{value: "DHRW10002", messageTemplate: "Fleet is unhealthy. {0}"}
	DHRW10003 = errorCodeEnum{value: "DHRW10003", messageTemplate: "Fleet Component is unhealthy. {0}"}
	DHRW10004 = errorCodeEnum{value: "DHRW10004", messageTemplate: "Fleet Connector is unhealthy. {0}"}
	DBPW10001 = errorCodeEnum{value: "DBPW10001", messageTemplate: "Unparsed Events Received. {0}"}
	DIIS10001 = errorCodeEnum{value: "DIIS10001", messageTemplate: "Report generated successfully. {0}"}
	DIIS20001 = errorCodeEnum{value: "DIIS20001", messageTemplate: "Alert report generation failed. {0}"}
	DWSE10001 = errorCodeEnum{value: "DWSE10001", messageTemplate: "Smart edge alert. {0}"}
)
