package format

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// CompatibleVersion is the upstream Icechunk release this module is aligned
// with.
const CompatibleVersion = "2.3.0"

// ImplementationName is the client name written into the header of every
// file this module produces, and stamped on objects as their icclient
// metadata: upstream's, at the release this module is aligned with.
const ImplementationName = "ic-" + CompatibleVersion

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
