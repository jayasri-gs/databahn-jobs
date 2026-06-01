package processor

import (
	"encoding/binary"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

var zstdFrameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// isTruncatedParquetError reports footer/metadata read failures typical of partial downloads.
func isTruncatedParquetError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "seek") && strings.Contains(msg, "invalid argument") {
		return true
	}
	if strings.Contains(msg, "eof") && strings.Contains(msg, "footer") {
		return true
	}
	return errors.Is(err, ErrTruncatedParquet)
}

// ErrTruncatedParquet indicates the file is missing a valid Parquet footer.
var ErrTruncatedParquet = errors.New("truncated parquet file")

// SalvageParquetFile extracts message payloads from a truncated raw/parsed message parquet file.
// Schema assumption: optional BYTE_ARRAY column (message/rawevent), PLAIN encoding, ZSTD page compression.
func SalvageParquetFile(data []byte) ([]string, error) {
	if len(data) < 8 {
		return nil, ErrTruncatedParquet
	}

	dctx, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dctx.Close()

	var out []string

	for off := 0; off+len(zstdFrameMagic) < len(data); off++ {
		if data[off] != zstdFrameMagic[0] || data[off+1] != zstdFrameMagic[1] ||
			data[off+2] != zstdFrameMagic[2] || data[off+3] != zstdFrameMagic[3] {
			continue
		}
		decompressed, err := dctx.DecodeAll(data[off:], nil)
		if err != nil || len(decompressed) < 8 {
			continue
		}
		for _, msg := range extractPlainUTF8Messages(decompressed) {
			out = append(out, msg)
		}
	}
	return out, nil
}

func extractPlainUTF8Messages(page []byte) []string {
	var msgs []string
	for i := 0; i+4 < len(page); i++ {
		ln := int(binary.LittleEndian.Uint32(page[i:]))
		if ln < 8 || ln > 2_000_000 || i+4+ln > len(page) {
			continue
		}
		payload := page[i+4 : i+4+ln]
		if !utf8.Valid(payload) {
			continue
		}
		if plainTextRatio(payload) < 0.88 {
			continue
		}
		msgs = append(msgs, string(payload))
		i += 4 + ln - 1
	}
	return msgs
}

func plainTextRatio(b []byte) float64 {
	if len(b) == 0 {
		return 0
	}
	good := 0
	for _, c := range b {
		if c >= 0x20 && c <= 0x7e || c == '\n' || c == '\r' || c == '\t' {
			good++
		}
	}
	return float64(good) / float64(len(b))
}
