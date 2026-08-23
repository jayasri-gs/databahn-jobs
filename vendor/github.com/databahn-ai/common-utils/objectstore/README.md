# Object Service

A unified object storage service supporting **S3**, **Azure Blob Storage**, and **Google Cloud Storage** as backends. Use a single interface for Get, Put, Post, and Delete operations regardless of the underlying storage.

## Native Authentication

When keys are not provided in config, both backends use native/cloud auth:

- **S3**: Uses AWS default credential chain (IAM role, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env, `~/.aws/credentials`, etc.)
- **Blob**: Uses `DefaultAzureCredential` (managed identity, Azure CLI, service principal env vars, etc.)
- **GCS**: Uses Application Default Credentials (GCE/GKE workload identity, `GOOGLE_APPLICATION_CREDENTIALS`, gcloud ADC, etc.)

For Blob, `account_name` (or `AZURE_STORAGE_ACCOUNT` env) is still required to build the service URL. Omit `account_key` and `connection_string` to use native auth.

## Configuration

Set `object.backend` to choose the backend:

- `s3` - Amazon S3 or S3-compatible storage (MinIO, LocalStack, etc.)
- `blob` - Azure Blob Storage
- `gcs` - Google Cloud Storage

### S3 Configuration

| Key | Description | Default |
|-----|-------------|---------|
| `object.events.collection` | Bucket/container for events | - |
| `object.artifacts.collection` | Bucket/container for artifacts | - |
| `object.s3.region` | AWS region | `us-east-1` or `AWS_REGION`/`AWS_DEFAULT_REGION` env |
| `object.s3.endpoint` | Custom endpoint URL | - |
| `object.s3.access_key` | Access key (omit for **native auth**: IAM role, env vars, etc.) | - |
| `object.s3.secret_key` | Secret key (omit for native auth) | - |
| `object.s3.force_path_style` | Use path-style URLs | `false` |

### Azure Blob Configuration

| Key | Description |
|-----|-------------|
| `object.blob.account_name` | Storage account name (or `AZURE_STORAGE_ACCOUNT` env) | - |
| `object.blob.account_key` | Account key (omit for **native auth**: managed identity, Azure CLI, etc.) | - |
| `object.blob.connection_string` | Full connection string (or `AZURE_STORAGE_CONNECTION_STRING` env) | - |

### Google Cloud Storage Configuration

| Key | Description |
|-----|-------------|
| `object.gcs.project_id` | GCP project id (or `GOOGLE_CLOUD_PROJECT` env) | - |
| `object.gcs.credentials_path` | Service account JSON path (or `GOOGLE_APPLICATION_CREDENTIALS` env); omit for ADC | - |
| `object.gcs.emulator_host` | GCS emulator host (or `STORAGE_EMULATOR_HOST` env), e.g. `localhost:4443` | - |

## Usage

### From Configuration

```go
import (
    "context"
    "github.com/databahn-ai/common-utils/configuration"
    "github.com/databahn-ai/common-utils/objectstore"
)

func main() {
    ctx := context.Background()
    cfg, _ := configuration.NewAppConfig()

    store, err := objectstore.NewObjectStore(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }

    container := "my-bucket"  // or container name for blob
    key := "path/to/object.txt"

    // Put (upload)
    store.Put(ctx, container, key, []byte("hello"))

    // Get (download)
    data, err := store.Get(ctx, container, key)

    // Post (create/append - same as Put for both backends)
    store.Post(ctx, container, key, []byte("more data"))

    // Delete
    store.Delete(ctx, container, key)
}
```

### Direct Backend Creation

```go
// S3
store, err := objectstore.NewS3Backend(ctx, "us-east-1", "", "accessKey", "secretKey", false)

// Azure Blob (connection string)
store, err := objectstore.NewBlobBackend(ctx, "DefaultEndpointsProtocol=https;...", "", "")

// Azure Blob (account + key)
store, err := objectstore.NewBlobBackend(ctx, "", "mystorageaccount", "accountKey")

// Azure Blob (DefaultAzureCredential)
store, err := objectstore.NewBlobBackend(ctx, "", "mystorageaccount", "")

// GCS (ADC)
store, err := objectstore.NewGcsBackend(ctx, "my-gcp-project", "", "")

// GCS (emulator)
store, err := objectstore.NewGcsBackend(ctx, "test-project", "", "localhost:4443")
```

### Mock Store for Testing

```go
store := objectstore.NewMockStore()
store.Put(ctx, "bucket", "key", []byte("test"))
data, _ := store.Get(ctx, "bucket", "key")
```

## Operations

| Method | Description |
|--------|-------------|
| `Get(ctx, container, key)` | Retrieve object content |
| `Put(ctx, container, key, data)` | Upload or overwrite object |
| `Post(ctx, container, key, data)` | Create or append (same as Put for both backends) |
| `Delete(ctx, container, key)` | Remove object |

## Example

Complete example using config or mock store for testing:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/objectstore"
)

func main() {
	ctx := context.Background()

	// Use mock store for quick demo without real S3/Blob (USE_MOCK_STORE=1)
	if os.Getenv("USE_MOCK_STORE") == "1" {
		store := objectstore.NewMockStore()
		store.Put(ctx, "demo-bucket", "sample/hello.txt", []byte("Hello!\n"))
		data, _ := store.Get(ctx, "demo-bucket", "sample/hello.txt")
		fmt.Println(string(data))
		return
	}

	// Create from config (app.yaml with object.backend, object.s3.* or object.blob.*)
	cfg, err := configuration.NewAppConfig()
	if err != nil {
		cfg = configuration.NewMockConfigReader(map[string]any{
			"object.backend":             "s3",
			"object.events.collection":   "my-events-bucket",
			"object.artifacts.collection": "my-artifacts-bucket",
			"object.s3.region":           "us-east-1",
		})
	}

	store, err := objectstore.NewObjectStore(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	eventsCollection := cfg.GetStringOrDefault(configuration.ObjectEventCollection, "my-events-bucket")
	artifactsCollection := cfg.GetStringOrDefault(configuration.ObjectArtifactCollection, "my-artifacts-bucket")

	// Put, Get, Post, Delete (example uses events collection)
	store.Put(ctx, eventsCollection, "path/hello.txt", []byte("Hello from objectstore!\n"))
	data, _ := store.Get(ctx, eventsCollection, "path/hello.txt")
	fmt.Println(string(data))
	store.Post(ctx, artifactsCollection, "path/append.txt", []byte("data"))
	store.Delete(ctx, artifactsCollection, "path/append.txt")
}
```

Sample config (app.yaml or object_example.yaml):

```yaml
object:
  backend: gcs  # or "s3" / "blob"
  events:
    collection: my-events-bucket  # bucket (S3/GCS) or container (Blob)
  artifacts:
    collection: my-artifacts-bucket

  gcs:
    project_id: my-gcp-project
    # credentials_path: /path/to/sa.json  # omit for workload identity / ADC

  s3:
    region: us-east-1
    # access_key: ...
    # secret_key: ...

  blob:
    account_name: mystorageaccount
    # account_key: ...  # omit for az login / DefaultAzureCredential
```
