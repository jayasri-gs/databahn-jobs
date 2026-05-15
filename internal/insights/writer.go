package insights

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type InsightsWriter interface {
	Init(index *IndexMetadata, attMap map[string]string) error
	WriteDocs(docs []Doc, attMap map[string]string, sourceIdToNameMap map[string]string) error
	Upload(ctx context.Context) error
	Cleanup()
}

// JSONLWriter writes newline-delimited JSON to S3 (legacy format).

type JSONLWriter struct {
	file   *os.File
	index  *IndexMetadata
	closed bool
}

func (w *JSONLWriter) Init(index *IndexMetadata, attMap map[string]string) error {
	filePath := getS3FileName(*index)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.index = index
	return nil
}

func (w *JSONLWriter) WriteDocs(docs []Doc, attMap map[string]string, sourceIdToNameMap map[string]string) error {
	return writeToSearchFile(w.file, docs, attMap, sourceIdToNameMap)
}

func (w *JSONLWriter) Upload(ctx context.Context) error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.closed = true
	return uploadFileToS3ForSearch(ctx, w.index, w.file.Name())
}

func (w *JSONLWriter) Cleanup() {
	if !w.closed && w.file != nil {
		w.file.Close()
	}
	if w.file != nil {
		os.Remove(w.file.Name())
	}
}

// ParquetWriter writes Apache Parquet to S3 with a dynamic schema derived from attMap.
// Column types: timestamp and count as INT64, all others as string.

type ParquetWriter struct {
	pw            *writer.ParquetWriter
	fw            source.ParquetFile
	structType    reflect.Type
	keyIdx        map[string]int
	sourceIdIdx   int
	sourceNameIdx int
	idIdx         int
	timestampIdx  int
	countIdx      int
	index         *IndexMetadata
	filePath      string
	fwClosed      bool
}

func (w *ParquetWriter) Init(index *IndexMetadata, attMap map[string]string) error {
	w.index = index
	w.filePath = os.TempDir() + "/" + index.String() + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + ".parquet"

	var fields []reflect.StructField
	w.keyIdx = make(map[string]int)
	idx := 0

	for _, k := range []string{"key1", "key2", "key3", "key4", "key5"} {
		if attrName, ok := attMap[k]; ok {
			fields = append(fields, parquetStringField(idx, attrName))
			w.keyIdx[k] = idx
			idx++
		}
	}

	fields = append(fields, parquetStringField(idx, "source_id"))
	w.sourceIdIdx = idx
	idx++

	fields = append(fields, parquetStringField(idx, "source_name"))
	w.sourceNameIdx = idx
	idx++

	fields = append(fields, parquetStringField(idx, "id"))
	w.idIdx = idx
	idx++

	fields = append(fields, parquetInt64Field(idx, "timestamp"))
	w.timestampIdx = idx
	idx++

	fields = append(fields, parquetInt64Field(idx, "count"))
	w.countIdx = idx

	w.structType = reflect.StructOf(fields)

	fw, err := local.NewLocalFileWriter(w.filePath)
	if err != nil {
		return err
	}
	w.fw = fw

	pw, err := writer.NewParquetWriter(fw, reflect.New(w.structType).Interface(), 4)
	if err != nil {
		fw.Close()
		return err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	w.pw = pw

	return nil
}

func (w *ParquetWriter) WriteDocs(docs []Doc, attMap map[string]string, sourceIdToNameMap map[string]string) error {
	for _, doc := range docs {
		rowPtr := reflect.New(w.structType)
		rowVal := rowPtr.Elem()

		for k, fieldIdx := range w.keyIdx {
			val := docKeyValue(doc, k)
			rowVal.Field(fieldIdx).Set(reflect.ValueOf(&val))
		}

		sourceId := doc.SourceId
		rowVal.Field(w.sourceIdIdx).Set(reflect.ValueOf(&sourceId))

		sourceName := "unknown_source_name"
		if name, ok := sourceIdToNameMap[doc.SourceId]; ok {
			sourceName = name
		}
		rowVal.Field(w.sourceNameIdx).Set(reflect.ValueOf(&sourceName))

		id := doc.Id
		rowVal.Field(w.idIdx).Set(reflect.ValueOf(&id))

		ts := doc.Timestamp
		rowVal.Field(w.timestampIdx).Set(reflect.ValueOf(&ts))

		count := int64(doc.Count)
		rowVal.Field(w.countIdx).Set(reflect.ValueOf(&count))

		if err := w.pw.Write(rowPtr.Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (w *ParquetWriter) Upload(ctx context.Context) error {
	if err := w.pw.WriteStop(); err != nil {
		return err
	}
	if err := w.fw.Close(); err != nil {
		return err
	}
	w.fwClosed = true
	objectKey := fmt.Sprintf("tenant_id=%s/insight_rule_id=%s/year=%04d/month=%02d/date=%02d/%s",
		w.index.TenantId, w.index.Type, w.index.Year, w.index.Month, w.index.Day, filepath.Base(w.filePath))
	return util.UploadFileToS3Parquet(ctx, objectKey, w.filePath)
}

func (w *ParquetWriter) Cleanup() {
	if !w.fwClosed && w.fw != nil {
		w.fw.Close()
	}
	if w.filePath != "" {
		os.Remove(w.filePath)
	}
}

func parquetStringField(idx int, name string) reflect.StructField {
	return reflect.StructField{
		Name: "F" + strconv.Itoa(idx),
		Type: reflect.TypeOf((*string)(nil)),
		Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL, encoding=PLAIN, omitstats=true"`, name)),
	}
}

func parquetInt64Field(idx int, name string) reflect.StructField {
	return reflect.StructField{
		Name: "F" + strconv.Itoa(idx),
		Type: reflect.TypeOf((*int64)(nil)),
		Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL, omitstats=true"`, name)),
	}
}

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
	}
	return ""
}
