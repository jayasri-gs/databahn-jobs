package upload

import "testing"

func TestBlockIDsThrough(t *testing.T) {
	ids := BlockIDsThrough(3)
	if len(ids) != 3 {
		t.Fatalf("len=%d", len(ids))
	}
	if ids[0] != BlockIDForPart(1) || ids[2] != BlockIDForPart(3) {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestAzureUploader_ReattachMultipartRequiresClient(t *testing.T) {
	u := &AzureUploader{uploadID: "old"}
	err := u.ReattachMultipart("container", "blob.csv", "container/blob.csv", BlockIDsThrough(2), "text/csv")
	if err == nil {
		t.Fatal("expected error without client")
	}
}
