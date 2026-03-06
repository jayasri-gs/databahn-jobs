package insights

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// Fixed column names for search output (same as S3 JSON).
const (
	colId         = "id"
	colSourceId   = "source_id"
	colSourceName = "source_name"
	colTimestamp  = "timestamp"
	colCount      = "count"
)

// sanitizeFieldName converts any field name into a valid Go identifier (same as azure-blob-parquet-dispenser).
// Returns empty string if field should be skipped. Parquet column name is set via the struct tag.
func sanitizeFieldName(fieldName string) string {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return ""
	}
	result := capitalize(fieldName)
	if result == "" || !isLetter(rune(result[0])) {
		result = "Field_" + result
	}
	runes := []rune(result)
	for i, r := range runes {
		if i == 0 {
			if !isLetter(r) {
				runes[i] = '_'
			}
		} else {
			if !isLetter(r) && !unicode.IsDigit(r) {
				runes[i] = '_'
			}
		}
	}
	result = string(runes)
	if !isValidFieldName(result) {
		result = "Field_" + strings.ReplaceAll(fieldName, ".", "_")
	}
	return result
}

func capitalize(s string) string {
	if s == "" {
		return ""
	}
	trimmed := strings.TrimLeft(s, "_")
	if trimmed == "" {
		return "Field" + s
	}
	parts := strings.Split(trimmed, ".")
	capitalizedParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		if len(runes) > 0 {
			runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		}
		capitalizedParts[i] = strings.ReplaceAll(string(runes), "-", "")
	}
	return strings.Join(capitalizedParts, "__DOT__")
}

func isValidFieldName(fieldName string) bool {
	for i, c := range fieldName {
		if i == 0 && !isLetter(c) {
			return false
		}
		if !(isLetter(c) || unicode.IsDigit(c)) {
			return false
		}
	}
	return len(fieldName) > 0
}

func isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch >= utf8.RuneSelf && unicode.IsLetter(ch)
}

// insightParquetFields returns ordered (parquet column name, parquet tag) for dynamic schema:
// id, source_id, source_name, timestamp, count, then attribute columns from attMap (key1..key5).
// Parquet column names match S3 JSON (attribute names from attMap).
func insightParquetFields(attMap map[string]string) []struct{ name, tag string } {
	out := []struct{ name, tag string }{
		{colId, "name=id, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true"},
		{colSourceId, "name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true"},
		{colSourceName, "name=source_name, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true"},
		{colTimestamp, "name=timestamp, type=INT64, omitstats=true"},
		{colCount, "name=count, type=INT64, omitstats=true"},
	}
	for _, k := range []string{"key1", "key2", "key3", "key4", "key5"} {
		if attrName, ok := attMap[k]; ok && attrName != "" {
			// Parquet column name = attribute name (same as S3 JSON key)
			out = append(out, struct{ name, tag string }{
				attrName,
				fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true", attrName),
			})
		}
	}
	return out
}

// createDynamicInsightStructType builds a struct type at runtime with columns matching S3 JSON (id, source_id, source_name, timestamp, count + attMap attribute names).
func createDynamicInsightStructType(attMap map[string]string) (reflect.Type, []string, error) {
	fields := insightParquetFields(attMap)
	if len(fields) == 0 {
		return nil, nil, fmt.Errorf("no parquet fields")
	}
	names := make([]string, 0, len(fields))
	reflectFields := make([]reflect.StructField, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.name)
		var goType reflect.Type
		if f.name == colTimestamp || f.name == colCount {
			goType = reflect.TypeOf(int64(0))
		} else {
			goType = reflect.TypeOf("")
		}
		fieldName := sanitizeFieldName(f.name)
		if fieldName == "" {
			fieldName = "Field"
		}
		reflectFields = append(reflectFields, reflect.StructField{
			Name: fieldName,
			Type: goType,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s"`, f.tag)),
		})
	}
	return reflect.StructOf(reflectFields), names, nil
}

// docToDynamicRowFixed builds row when we know orderedNames: first 5 are fixed, rest are key1..key5 in order.
func docToDynamicRowFixed(doc Doc, sourceIdToNameMap map[string]string, orderedNames []string, dynamicType reflect.Type) (interface{}, error) {
	sourceName := "unknown_source_name"
	if n, ok := sourceIdToNameMap[doc.SourceId]; ok {
		sourceName = n
	}
	row := reflect.New(dynamicType).Elem()
	for i, colName := range orderedNames {
		var v interface{}
		switch colName {
		case colId:
			v = doc.Id
		case colSourceId:
			v = doc.SourceId
		case colSourceName:
			v = sourceName
		case colTimestamp:
			v = doc.Timestamp
		case colCount:
			v = int64(doc.Count)
		default:
			// Attribute columns come after the 5 fixed; order is key1, key2, key3, key4, key5
			if i >= 5 && i-5 < 5 {
				vals := []string{doc.Key1, doc.Key2, doc.Key3, doc.Key4, doc.Key5}
				v = vals[i-5]
			} else {
				v = ""
			}
		}
		f := row.Field(i)
		if f.CanSet() && v != nil {
			rv := reflect.ValueOf(v)
			if f.Kind() == reflect.Int64 && rv.Kind() == reflect.Float64 {
				f.SetInt(int64(rv.Float()))
			} else if rv.Type().AssignableTo(f.Type()) {
				f.Set(rv)
			} else if rv.Type().ConvertibleTo(f.Type()) {
				f.Set(rv.Convert(f.Type()))
			} else {
				f.Set(reflect.ValueOf(fmt.Sprint(v)))
			}
		}
	}
	return row.Addr().Interface(), nil
}

// WriteDocsToParquetFile writes the given docs to a parquet file at filePath.
// Uses a dynamic schema from attMap so parquet columns match S3 JSON (id, source_id, source_name, timestamp, count, then attribute names from attMap).
// attMap is the insight rule's attribute map (key1->name, etc.); same as used for S3 JSON. Can be nil (writes only fixed columns).
func WriteDocsToParquetFile(filePath string, docs []Doc, sourceIdToNameMap, attMap map[string]string) error {
	if len(docs) == 0 {
		return nil
	}
	if attMap == nil {
		attMap = make(map[string]string)
	}
	pFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return err
	}
	defer pFile.Close()

	dynamicType, orderedNames, err := createDynamicInsightStructType(attMap)
	if err != nil {
		return err
	}
	instance := reflect.New(dynamicType).Interface()
	pw, err := writer.NewParquetWriter(pFile, instance, 4)
	if err != nil {
		return err
	}
	for i := range docs {
		row, err := docToDynamicRowFixed(docs[i], sourceIdToNameMap, orderedNames, dynamicType)
		if err != nil {
			return err
		}
		if err := pw.Write(row); err != nil {
			return err
		}
	}
	return pw.WriteStop()
}
