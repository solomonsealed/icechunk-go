package format

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ImplementationName is written into the header of every file this module
// produces ("<name>-<version>", like upstream's "ic-2.2.2").
const ImplementationName = "icechunk_go-0.1.0"

var (
	encoderOnce sync.Once
	encoder     *zstd.Encoder
	encoderErr  error
)

// Encode wraps a finished flatbuffer in the Icechunk file envelope: the
// binary header followed by the zstd-compressed payload. The zstd frame
// records its content size, as upstream's writer does.
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
