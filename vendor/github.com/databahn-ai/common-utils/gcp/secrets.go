package gcp

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

func ReadSecretByName(ctx context.Context, projectId, secretName string) (string, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create GCP Secret Manager client: %w", err)
	}
	defer client.Close()

	name := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectId, secretName)
	resp, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("failed to access secret %s: %w", secretName, err)
	}
	if resp.Payload == nil || len(resp.Payload.Data) == 0 {
		return "", fmt.Errorf("secret value is empty for %s", secretName)
	}
	return string(resp.Payload.Data), nil
}
