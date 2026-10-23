package format

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Version is this module's version, as it identifies itself in file
// headers and User-Agent headers.
const Version = "0.1.0"

// ImplementationName is written into the header of every file this module
// produces ("<name>-<version>", like upstream's "ic-2.3.0").
const ImplementationName = "icechunk_go-" + Version

var (
	encoderOnce sync.Once
	encoder     *zstd.Encoder
	encoderErr  error
)

// Encode wraps a finished flatbuffer in the Icechunk file envelope: the
// binary header followed by the zstd-compressed payload. The zstd frame
// records its content size, as upstream's writer does since 2.3.0 (whose
// reader then requires it to be exact).
func Encode(fileType FileType, specVersion uint8, payload []byte) ([]byte, error) {
	encoderOnce.Do(func() {
		encoder, encoderErr = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	})
	if encoderErr != nil {
		return nil, encoderErr
	}
	header := make([]byte, 0, HeaderLen+len(payload)/2+64)
	header = append(header, Magic...)
	name := fmt.Sprintf("%-*s", implNameLen, ImplementationName)[:implNameLen]
	header = append(header, name...)
	header = append(header, specVersion, byte(fileType), CompressionZstd)
	return encoder.EncodeAll(payload, header), nil
}
