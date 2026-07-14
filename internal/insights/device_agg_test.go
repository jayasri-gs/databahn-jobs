package insights

import "testing"

func TestGetDeviceAggWriteBatch(t *testing.T) {
	t.Setenv(deviceAggWriteBatchEnv, "150")
	if got := getDeviceAggWriteBatch(); got != 150 {
		t.Fatalf("getDeviceAggWriteBatch() = %d, want 150", got)
	}

	t.Setenv(deviceAggWriteBatchEnv, "0")
	if got := getDeviceAggWriteBatch(); got != defaultDeviceAggWriteBatch {
		t.Fatalf("getDeviceAggWriteBatch() = %d, want default %d", got, defaultDeviceAggWriteBatch)
	}

	t.Setenv(deviceAggWriteBatchEnv, "")
	if got := getDeviceAggWriteBatch(); got != defaultDeviceAggWriteBatch {
		t.Fatalf("getDeviceAggWriteBatch() = %d, want default %d", got, defaultDeviceAggWriteBatch)
	}
}

func TestChunkDeviceDocuments(t *testing.T) {
	docs := []DeviceDocument{
		{Id: "tenant:host-1"},
		{Id: "tenant:host-2"},
		{Id: "tenant:host-3"},
	}

	batches := chunkDeviceDocuments(docs, 2)
	if len(batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("unexpected batch sizes: %d, %d", len(batches[0]), len(batches[1]))
	}

	if got := chunkDeviceDocuments(nil, 2); got != nil {
		t.Fatalf("chunkDeviceDocuments(nil) = %#v, want nil", got)
	}
}

func TestDeviceId(t *testing.T) {
	id := DeviceId("tenant-1", "host.example.com")
	if id != "tenant-1:host.example.com" {
		t.Fatalf("unexpected device id: %s", id)
	}
}

func TestDeviceIndexName(t *testing.T) {
	index := DeviceIndexName("tenant-1")
	if index != "db_insights_devices_tenant-1" {
		t.Fatalf("unexpected index name: %s", index)
	}
}

func TestSightToDeviceDoc(t *testing.T) {
	doc := sightToDeviceDoc(Sight{
		Key1:        "host1",
		Key2:        "agent-1",
		Key3:        "short",
		SourceId:    "source-1",
		TenantId:    "tenant-1",
		DataPlaneId: "dp-1",
		MinTime:     100,
		MaxTime:     200,
		Timestamp:   300,
	})

	if doc.Id != "tenant-1:host1" {
		t.Fatalf("unexpected id: %s", doc.Id)
	}
	if doc.SourceId != "source-1" {
		t.Fatalf("unexpected source id: %s", doc.SourceId)
	}
	if doc.MinTime != 100 || doc.MaxTime != 200 {
		t.Fatalf("unexpected min/max: %d/%d", doc.MinTime, doc.MaxTime)
	}
}

func TestCountUniqueDeviceIDs(t *testing.T) {
	docs := []DeviceDocument{
		{Id: "a"},
		{Id: "a"},
		{Id: "b"},
	}
	if count := countUniqueDeviceIDs(docs); count != 2 {
		t.Fatalf("expected 2 unique devices, got %d", count)
	}
}

func TestStreamingUniqueDeviceCounter(t *testing.T) {
	counter := &streamingUniqueDeviceCounter{}

	pageOneUnique := counter.observeBatch([]string{"tenant:host-a", "tenant:host-a", "tenant:host-b"})
	if pageOneUnique != 2 {
		t.Fatalf("page one unique = %d, want 2", pageOneUnique)
	}
	if counter.totalUnique() != 2 {
		t.Fatalf("total after page one = %d, want 2", counter.totalUnique())
	}

	pageTwoUnique := counter.observeBatch([]string{"tenant:host-b", "tenant:host-c"})
	if pageTwoUnique != 1 {
		t.Fatalf("page two unique = %d, want 1", pageTwoUnique)
	}
	if counter.totalUnique() != 3 {
		t.Fatalf("total after page two = %d, want 3", counter.totalUnique())
	}
}

func TestTenantIdFromSightsIndex(t *testing.T) {
	tenantId, err := tenantIdFromSightsIndex("db_insights_sights_sourcehostname_a3a885d9-a0c1-4e94-8ccd-0aba12b169f4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenantId != "a3a885d9-a0c1-4e94-8ccd-0aba12b169f4" {
		t.Fatalf("unexpected tenant id: %s", tenantId)
	}
}

func TestListSourceHostnameSightsIndices(t *testing.T) {
	indices := listSourceHostnameSightsIndices([]string{
		"db_insights_sights_sourcehostname_tenant-1",
		"db_insights_sights_other_tenant-1",
		"db_staging_insights_x",
	})
	if len(indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(indices))
	}
}
