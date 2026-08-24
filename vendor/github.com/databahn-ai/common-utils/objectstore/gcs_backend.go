package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type gcsBackend struct {
	client *storage.Client
}

var _ ObjectStore = (*gcsBackend)(nil)

// NewGcsBackend creates a Google Cloud Storage-backed ObjectStore.
// Auth modes:
//  1. Application Default Credentials when credentialsPath is empty (GCE, GKE workload identity, gcloud ADC, etc.)
//  2. Service account JSON file via credentialsPath (or GOOGLE_APPLICATION_CREDENTIALS env)
func NewGcsBackend(ctx context.Context, _ string, credentialsPath string) (ObjectStore, error) {
	var opts []option.ClientOption

	if credentialsPath == "" {
		credentialsPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}
	if credentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsPath))
		logging.GetLoggerWithContext(ctx).Info("creating GCS object store with credentials file")
	} else {
		logging.GetLoggerWithContext(ctx).Info("creating GCS object store with application default credentials")
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("failed to create GCS client", zap.Error(err))
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &gcsBackend{client: client}, nil
}

// NewGcsBackendWithClient creates a GCS-backed ObjectStore with an existing client.
func NewGcsBackendWithClient(client *storage.Client) ObjectStore {
	return &gcsBackend{client: client}
}

func (g *gcsBackend) Get(ctx context.Context, container, key string) ([]byte, error) {
	rc, err := g.client.Bucket(container).Object(key).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, fmt.Errorf("gcs get object: object not found: %s", key)
		}
		return nil, fmt.Errorf("gcs get object: %w", err)
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return nil, fmt.Errorf("gcs read body: %w", err)
	}
	return buf.Bytes(), nil
}

func (g *gcsBackend) Put(ctx context.Context, container, key string, data []byte, opts ...*PutOptions) error {
	w := g.client.Bucket(container).Object(key).NewWriter(ctx)
	g.applyPutOptions(w, opts)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs put object write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs put object: %w", err)
	}
	return nil
}

func (g *gcsBackend) PutStream(ctx context.Context, container, key string, reader io.Reader, opts ...*PutOptions) error {
	w := g.client.Bucket(container).Object(key).NewWriter(ctx)
	g.applyPutOptions(w, opts)
	if _, err := io.Copy(w, reader); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs put stream write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs put stream: %w", err)
	}
	return nil
}

func (g *gcsBackend) applyPutOptions(w *storage.Writer, opts []*PutOptions) {
	if len(opts) == 0 || opts[0] == nil {
		return
	}
	if opts[0].ContentType != "" {
		w.ContentType = opts[0].ContentType
	}
	if opts[0].ContentEncoding != "" {
		w.ContentEncoding = opts[0].ContentEncoding
	}
}

func (g *gcsBackend) Post(ctx context.Context, container, key string, data []byte) error {
	return g.Put(ctx, container, key, data)
}

func (g *gcsBackend) Delete(ctx context.Context, container, key string) error {
	if err := g.client.Bucket(container).Object(key).Delete(ctx); err != nil {
		if err == storage.ErrObjectNotExist {
			return nil
		}
		return fmt.Errorf("gcs delete object: %w", err)
	}
	return nil
}

func (g *gcsBackend) List(ctx context.Context, container, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	it := g.client.Bucket(container).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs list objects: %w", err)
		}
		objects = append(objects, ObjectInfo{
			Key:          attrs.Name,
			LastModified: attrs.Updated,
		})
	}
	return objects, nil
}

func (g *gcsBackend) GetPresignedURL(ctx context.Context, container, key string, expiry time.Duration) (string, error) {
	_ = ctx
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(expiry),
	}
	url, err := g.client.Bucket(container).SignedURL(key, opts)
	if err != nil {
		return "", fmt.Errorf("gcs signed URL: %w", err)
	}
	return url, nil
}

func (g *gcsBackend) DeleteBatch(ctx context.Context, container string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var errs []string
	for _, key := range keys {
		if err := g.client.Bucket(container).Object(key).Delete(ctx); err != nil {
			if err == storage.ErrObjectNotExist {
				continue
			}
			errs = append(errs, fmt.Sprintf("key=%s: %v", key, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("gcs batch delete partial failure: %s", strings.Join(errs, "; "))
	}
	return nil
}
