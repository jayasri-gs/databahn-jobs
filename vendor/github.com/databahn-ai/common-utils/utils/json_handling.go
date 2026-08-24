package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/itchyny/gojq"
	"go.uber.org/zap"
)

func IsJSONObject(s string) bool {
	var v any

	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return false
	}

	_, ok := v.(map[string]any)
	return ok
}

func SortTopLevelKeys(input []byte) ([]byte, error) {
	log := logger.GetLogger()
	var obj map[string]json.RawMessage

	if err := json.Unmarshal(input, &obj); err != nil {
		log.Error("error unmarshalling input", zap.Error(err))
		return nil, err
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')

	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}

		keyJSON, _ := json.Marshal(k)

		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(obj[k]) // preserve nested JSON exactly as-is
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

func RemoveJsonWhitespaces(jsonInp []byte) string {
	log := logger.GetLogger()
	var buf bytes.Buffer
	if err := json.Compact(&buf, jsonInp); err != nil {
		log.Error("error compacting json", zap.Error(err))
		return string(jsonInp)
	}
	return buf.String()
}

func ApplyJQExpressionToJSONString(eventJSON string, expression string) (string, error) {
	query, err := gojq.Parse(expression)
	if err != nil {
		return "", err
	}

	var input any
	if err := json.Unmarshal([]byte(eventJSON), &input); err != nil {
		return "", err
	}

	iter := query.Run(input)
	value, ok := iter.Next()
	if !ok {
		return "", errors.New("jq expression returned no value")
	}
	if err, ok := value.(error); ok {
		return "", err
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
