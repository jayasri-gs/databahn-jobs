package query

// Shared fixtures and assertion messages for this package's tests, kept as constants so a
// change to one cannot leave other call sites behind.
const (
	testSentinelQuery = "SecurityEvent | take 10"

	errNewSentinelExecutor = "NewSentinelExecutor: %v"
	errStreamRows          = "StreamRows: %v"
	errDecodeResponse      = "decode: %v"
	errParseADXOpStatus    = "ParseADXOperationStatus: %v"
	gotColumns             = "columns = %v"
	gotOutputLocation      = "output location = %q"
)
