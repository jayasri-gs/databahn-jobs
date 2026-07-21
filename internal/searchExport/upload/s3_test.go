package upload

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeMultipartAPI struct {
	pages    []*s3.ListMultipartUploadsOutput
	pageIdx  int
	listErr  error
	abortErr error
	aborted  []string
}

func (f *fakeMultipartAPI) ListMultipartUploads(_ context.Context, _ *s3.ListMultipartUploadsInput, _ ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	page := f.pages[f.pageIdx]
	f.pageIdx++
	return page, nil
}

func (f *fakeMultipartAPI) AbortMultipartUpload(_ context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if f.abortErr != nil {
		return nil, f.abortErr
	}
	f.aborted = append(f.aborted, aws.ToString(in.Key)+":"+aws.ToString(in.UploadId))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func multipartUpload(key, id string) s3types.MultipartUpload {
	return s3types.MultipartUpload{Key: aws.String(key), UploadId: aws.String(id)}
}

func TestAbortIncompleteUploads_AbortsAllOnPrefix(t *testing.T) {
	fake := &fakeMultipartAPI{pages: []*s3.ListMultipartUploadsOutput{
		{Uploads: []s3types.MultipartUpload{multipartUpload("exports/r1/a.csv", "u1"), multipartUpload("exports/r1/b.csv", "u2")}},
	}}
	abortIncompleteUploads(context.Background(), fake, "bucket", "exports/r1/")
	if len(fake.aborted) != 2 {
		t.Fatalf("expected 2 aborts, got %v", fake.aborted)
	}
}

func TestAbortIncompleteUploads_Paginates(t *testing.T) {
	fake := &fakeMultipartAPI{pages: []*s3.ListMultipartUploadsOutput{
		{
			Uploads:            []s3types.MultipartUpload{multipartUpload("exports/r1/a.csv", "u1")},
			IsTruncated:        aws.Bool(true),
			NextKeyMarker:      aws.String("k"),
			NextUploadIdMarker: aws.String("m"),
		},
		{Uploads: []s3types.MultipartUpload{multipartUpload("exports/r1/a.csv", "u2")}},
	}}
	abortIncompleteUploads(context.Background(), fake, "bucket", "exports/r1/")
	if len(fake.aborted) != 2 {
		t.Fatalf("expected 2 aborts across pages, got %v", fake.aborted)
	}
}

func TestAbortIncompleteUploads_ToleratesErrors(t *testing.T) {
	// List error: must return without panicking.
	abortIncompleteUploads(context.Background(), &fakeMultipartAPI{listErr: errors.New("boom")}, "bucket", "exports/r1/")

	// Abort error: must not panic and must not fail the caller.
	fake := &fakeMultipartAPI{
		abortErr: errors.New("denied"),
		pages: []*s3.ListMultipartUploadsOutput{
			{Uploads: []s3types.MultipartUpload{multipartUpload("exports/r1/a.csv", "u1")}},
		},
	}
	abortIncompleteUploads(context.Background(), fake, "bucket", "exports/r1/")
	if len(fake.aborted) != 0 {
		t.Fatalf("expected no successful aborts, got %v", fake.aborted)
	}
}
