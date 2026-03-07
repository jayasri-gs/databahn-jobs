package insights

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
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

// StreamingParquetWriter writes docs to a parquet file in batches so only one batch is held in memory
// (same memory profile as the JSON path, which writes per batch). Call OpenStreamingParquetWriter,
// then WriteDocs for each batch, then Close.
type StreamingParquetWriter struct {
	FilePath          string // path passed to OpenStreamingParquetWriter, for upload after Close
	pFile             source.ParquetFile
	pw                *writer.ParquetWriter
	dynamicType       reflect.Type
	orderedNames      []string
	orderedKeys       []string
	sourceIdToNameMap map[string]string
}

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

// insightParquetField holds parquet column info; key is "key1".."key5" for attribute columns, empty for fixed.
type insightParquetField struct {
	name, tag string
	key       string // doc key for attribute columns: "key1".."key5"
}

// insightParquetFields returns ordered (parquet column name, tag, doc key) for dynamic schema:
// id, source_id, source_name, timestamp, count, then attribute columns from attMap (key1..key5).
// Parquet column names match S3 JSON (attribute names from attMap). Skipped keys are omitted.
func insightParquetFields(attMap map[string]string) []insightParquetField {
	out := []insightParquetField{
		{colId, "name=id, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true", ""},
		{colSourceId, "name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true", ""},
		{colSourceName, "name=source_name, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true", ""},
		{colTimestamp, "name=timestamp, type=INT64, omitstats=true", ""},
		{colCount, "name=count, type=INT64, omitstats=true", ""},
	}
	for _, k := range []string{"key1", "key2", "key3", "key4", "key5"} {
		if attrName, ok := attMap[k]; ok && attrName != "" {
			out = append(out, insightParquetField{
				name: attrName,
				tag:  fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true", attrName),
				key:  k,
			})
		}
	}
	return out
}

// createDynamicInsightStructType builds a struct type at runtime with columns matching S3 JSON (id, source_id, source_name, timestamp, count + attMap attribute names).
// orderedKeys is the doc key for each attribute column ("key1".."key5"), same length as names minus 5; used for key-based row mapping.
func createDynamicInsightStructType(attMap map[string]string) (reflect.Type, []string, []string, error) {
	fields := insightParquetFields(attMap)
	if len(fields) == 0 {
		return nil, nil, nil, fmt.Errorf("no parquet fields")
	}
	names := make([]string, 0, len(fields))
	orderedKeys := make([]string, 0, len(fields)-5)
	reflectFields := make([]reflect.StructField, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.name)
		if f.key != "" {
			orderedKeys = append(orderedKeys, f.key)
		}
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
	return reflect.StructOf(reflectFields), names, orderedKeys, nil
}

// docKeyValue returns the Doc field value for the given key ("key1".."key5"); same mapping as SearchMap.
func docKeyValue(doc Doc, key string) string {
	switch key {
	case "key1":
		return doc.Key1
	case "key2":
		return doc.Key2
	case "key3":
		return doc.Key3
	case "key4":
		return doc.Key4
	case "key5":
		return doc.Key5
	default:
		return ""
	}
}

// docToDynamicRowFixed builds row when we know orderedNames (first 5 fixed) and orderedKeys (doc key per attribute column).
// Uses key-based lookup for attribute columns so skipped keys (key2, key4, etc.) do not misalign data.
func docToDynamicRowFixed(doc Doc, sourceIdToNameMap map[string]string, orderedNames []string, orderedKeys []string, dynamicType reflect.Type) (interface{}, error) {
	sourceName := "unknown_source_name"
	if n, ok := sourceIdToNameMap[doc.SourceId]; ok {
		sourceName = n
	}
	row := reflect.New(dynamicType).Elem()
	for i, colName := range orderedNames {
		var v interface{}
		if i < 5 {
			// Fixed columns by position; attribute names must not override these.
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
				v = ""
			}
		} else {
			// Attribute columns: use key-based lookup so skipped keys don't misalign columns.
			attrIdx := i - 5
			if attrIdx < len(orderedKeys) {
				v = docKeyValue(doc, orderedKeys[attrIdx])
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
	sw, err := OpenStreamingParquetWriter(filePath, sourceIdToNameMap, attMap)
	if err != nil {
		return err
	}
	defer sw.Close()
	return sw.WriteDocs(docs)
}

// OpenStreamingParquetWriter creates a parquet file and returns a writer that accepts batches of docs.
// Call WriteDocs for each batch and Close when done. This avoids holding all docs in memory.
func OpenStreamingParquetWriter(filePath string, sourceIdToNameMap, attMap map[string]string) (*StreamingParquetWriter, error) {
	if attMap == nil {
		attMap = make(map[string]string)
	}
	pFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return nil, err
	}
	dynamicType, orderedNames, orderedKeys, err := createDynamicInsightStructType(attMap)
	if err != nil {
		pFile.Close()
		return nil, err
	}
	instance := reflect.New(dynamicType).Interface()
	pw, err := writer.NewParquetWriter(pFile, instance, 4)
	if err != nil {
		pFile.Close()
		return nil, err
	}
	return &StreamingParquetWriter{
		FilePath:          filePath,
		pFile:             pFile,
		pw:                pw,
		dynamicType:       dynamicType,
		orderedNames:      orderedNames,
		orderedKeys:       orderedKeys,
		sourceIdToNameMap: sourceIdToNameMap,
	}, nil
}

// WriteDocs writes a batch of docs to the parquet file. Can be called multiple times before Close.
func (sw *StreamingParquetWriter) WriteDocs(docs []Doc) error {
	for i := range docs {
		row, err := docToDynamicRowFixed(docs[i], sw.sourceIdToNameMap, sw.orderedNames, sw.orderedKeys, sw.dynamicType)
		if err != nil {
			return err
		}
		if err := sw.pw.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes the parquet footer and closes the file. Must be called when done.
func (sw *StreamingParquetWriter) Close() error {
	if sw.pw != nil {
		if err := sw.pw.WriteStop(); err != nil {
			if sw.pFile != nil {
				sw.pFile.Close()
			}
			return err
		}
		sw.pw = nil
	}
	if sw.pFile != nil {
		err := sw.pFile.Close()
		sw.pFile = nil
		return err
	}
	return nil
}
