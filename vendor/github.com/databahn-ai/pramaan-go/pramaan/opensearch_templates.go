package pramaan

import (
	"bytes"
	"context"
	"embed"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	osclient "github.com/opensearch-project/opensearch-go/v2"
)

//go:embed templates/*.json
var defaultOpenSearchIndexTemplates embed.FS

const defaultOpenSearchTemplatesDir = "templates"

func applyIndexTemplates(ctx context.Context, t TestLogger, client *osclient.Client, extraTemplateDirs ...string) {
	applyOpenSearchIndexTemplatesFromFS(ctx, t, client, defaultOpenSearchIndexTemplates, defaultOpenSearchTemplatesDir)
	for _, dir := range extraTemplateDirs {
		applyOpenSearchIndexTemplatesFromDir(ctx, t, client, dir)
	}
}

func applyOpenSearchIndexTemplatesFromDir(ctx context.Context, t TestLogger, client *osclient.Client, dir string) {
	applyOpenSearchIndexTemplatesFromFS(ctx, t, client, os.DirFS(dir), ".")
}

func applyOpenSearchIndexTemplatesFromFS(
	ctx context.Context,
	t TestLogger,
	client *osclient.Client,
	templateFS fs.FS,
	root string,
) {
	entries, err := fs.ReadDir(templateFS, root)
	if err != nil {
		t.Fatalf("failed to read opensearch templates from %q: %v", root, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		templateName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		body, err := fs.ReadFile(templateFS, filepath.Join(root, entry.Name()))
		if err != nil {
			t.Fatalf("failed to read opensearch template %q: %v", entry.Name(), err)
		}

		putOpenSearchIndexTemplate(ctx, t, client, templateName, body)
	}
}

func putOpenSearchIndexTemplate(ctx context.Context, t TestLogger, client *osclient.Client, templateName string, body []byte) {
	resp, err := client.Indices.PutIndexTemplate(
		templateName,
		bytes.NewReader(body),
		client.Indices.PutIndexTemplate.WithContext(ctx),
	)
	if err != nil {
		t.Fatalf("failed to apply opensearch index template %q: %v", templateName, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("failed to apply opensearch index template %q, status %d: %s", templateName, resp.StatusCode, string(respBody))
	}

	t.Logf("applied OpenSearch index template %q", templateName)
}
