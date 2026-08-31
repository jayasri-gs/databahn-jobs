package utils

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/databahn-ai/common-utils/constants"

	"github.com/google/uuid"
)

// Define a list of valid AWS regions
var validRegions = []string{
	"us-east-1",
	"us-east-2",
	"us-west-1",
	"us-west-2",
	// Add more regions as needed
}

// IsValidAWSRegion checks if the provided region is valid.
func IsValidAWSRegion(region string) bool {
	// Convert the provided region to lowercase for case-insensitive comparison
	region = strings.ToLower(region)

	// Check if the region is in the list of valid regions
	for _, validRegion := range validRegions {
		if region == validRegion {
			return true
		}
	}
	return false
}

func GetTenantFromContext(ctx context.Context) string {
	return GetValueFromContext(ctx, constants.TenantUuid)
}

func GetValueFromContext(ctx context.Context, key string) string {
	value := ctx.Value(key)
	if value != nil {
		return value.(string)
	}
	return ""
}

func GenerateRandomUUID() uuid.UUID {
	return uuid.New()
}

func GenerateRandomString(n int) (string, error) {
	if n < 0 {
		return "", errors.New("length should be grater than 0")
	}
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret), nil
}

func MakeJSONStringpretty(JSONString string) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(JSONString), "", "    "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func RemoveSuffix(name, suffix string) string {
	suffixRegex := ".*" + suffix + "$"
	regex, err := regexp.Compile(suffixRegex)
	// if we run into an error, just log it and return original string.
	if err != nil {
		fmt.Printf("error compiling suffix regex %s: %s\n", suffix, err.Error())
		return name
	}
	if regex.Match([]byte(name)) {
		name = name[:len(name)-len(suffix)]
	}
	return name
}

func ListContains(list []string, s string) bool {
	for i := range list {
		if list[i] == s {
			return true
		}
	}
	return false
}

func GetKeysFromMap(m map[string]bool) []string {
	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func UUIDFromStringOrNil(id string) uuid.UUID {
	id1, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil
	}
	return id1
}
