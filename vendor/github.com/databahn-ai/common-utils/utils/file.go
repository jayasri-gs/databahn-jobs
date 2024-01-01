package utils

import (
	"context"
	"os"
)

const defaultFilePath = "/opt/"

func ReadFile(ctx context.Context, filePath string) (contents []byte, err error) {
	return os.ReadFile(defaultFilePath + filePath)
}
