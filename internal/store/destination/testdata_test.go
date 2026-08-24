package destination

import "time"

// Shared fixtures for this package's tests. They are constants rather than repeated literals
// so a change to one of them cannot leave some call sites behind.
const (
	testADXClusterURI = "https://cluster.eastus.kusto.windows.net"
	// testShortURI is syntactically a URL but not an Azure Data Explorer host, so it stands
	// in for a cluster URI that must be rejected.
	testShortURI = "https://c"

	testAthenaOutBucket = "sl-athena-out"
	testAWSRegion       = "us-east-1"

	testSASSignature = "&sig=given"
	testSASPrefix    = "sv=2024-11-04&sp=racw&se="

	errGenerateSAS = "GenerateContainerWriteSAS: %v"
)

// testSASExpiry is far enough out to cover any export window a test asks for.
func testSASExpiry() string {
	return time.Now().UTC().Add(48 * time.Hour).Format(time.RFC3339)
}
