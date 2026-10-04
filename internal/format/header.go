// Package format implements the Icechunk binary file envelope (header +
// compressed flatbuffer payload) and the small self-describing encodings used
// for metadata values (MessagePack in spec v1, FlexBuffers in spec v2).
package format

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Magic is the UTF-8 encoding of "ICE🧊CHUNK" that starts every metadata file.
var Magic = []byte("ICE🧊CHUNK")

const (
	implNameLen = 24
	// HeaderLen is the size of the fixed binary header.
	HeaderLen = 12 + implNameLen + 3
)

// FileType is the kind of metadata file announced by the header.
type FileType uint8

const (
	FileTypeSnapshot       FileType = 1
	FileTypeManifest       FileType = 2
	FileTypeTransactionLog FileType = 4
	FileTypeRepoInfo       FileType = 6
)

func (t FileType) String() string {
	switch t {
	case FileTypeSnapshot:
		return "snapshot"
	case FileTypeManifest:
		return "manifest"
	case FileTypeTransactionLog:
		return "transaction log"
	case FileTypeRepoInfo:
		return "repo info"
	}
	return fmt.Sprintf("file type %d", uint8(t))
}

// Compression algorithms for the payload.
const (
	CompressionNone uint8 = 0
	CompressionZstd uint8 = 1
)

// Header is the parsed fixed-size header of a metadata file.
type Header struct {
	// Implementation is the writer's self-reported name, e.g. "ic-2.2.2".
	Implementation string
	SpecVersion    uint8
	FileType       FileType
	Compression    uint8
}

// ErrFormat wraps all malformed-file errors.
var ErrFormat = errors.New("icechunk format error")

// ParseHeader parses and validates the header at the start of buf.
func ParseHeader(buf []byte) (Header, error) {
	var h Header
	if len(buf) < HeaderLen {
		return h, fmt.Errorf("%w: file is %d bytes, shorter than the %d byte header", ErrFormat, len(buf), HeaderLen)
	}
	if !bytes.HasPrefix(buf, Magic) {
		return h, fmt.Errorf("%w: bad magic bytes, not an icechunk file", ErrFormat)
	}
	h.Implementation = strings.TrimRight(string(buf[len(Magic):len(Magic)+implNameLen]), " \x00")
	h.SpecVersion = buf[len(Magic)+implNameLen]
	h.FileType = FileType(buf[len(Magic)+implNameLen+1])
	h.Compression = buf[len(Magic)+implNameLen+2]
	if h.SpecVersion != 1 && h.SpecVersion != 2 {
		return h, fmt.Errorf("%w: unsupported spec version %d", ErrFormat, h.SpecVersion)
	}
	return h, nil
}

// zstd decoders are expensive to build; share one for whole-buffer decodes.
var (
	decoderOnce sync.Once
	decoder     *zstd.Decoder
	decoderErr  error
)

func sharedDecoder() (*zstd.Decoder, error) {
	decoderOnce.Do(func() {
		// Concurrency 0 = GOMAXPROCS parallel DecodeAll calls.
		decoder, decoderErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	})
	return decoder, decoderErr
}

// Decompress zstd-decodes a whole frame.
func Decompress(src []byte) ([]byte, error) {
	d, err := sharedDecoder()
	if err != nil {
		return nil, err
	}
	return d.DecodeAll(src, nil)
}

// Decode validates the header of an icechunk metadata file, checks that it
// has the expected type, and returns the header and decompressed flatbuffer.
func Decode(buf []byte, want FileType) (Header, []byte, error) {
	h, err := ParseHeader(buf)
	if err != nil {
		return h, nil, err
	}
	if h.FileType != want {
		return h, nil, fmt.Errorf("%w: expected a %s file, found a %s file", ErrFormat, want, h.FileType)
	}
	body := buf[HeaderLen:]
	switch h.Compression {
	case CompressionNone:
		return h, body, nil
	case CompressionZstd:
		out, err := Decompress(body)
		if err != nil {
			return h, nil, fmt.Errorf("%w: decompressing %s: %v", ErrFormat, want, err)
		}
		return h, out, nil
	}
	return h, nil, fmt.Errorf("%w: unknown compression algorithm %d", ErrFormat, h.Compression)
}

// DictDecoder decompresses zstd frames that were compressed with a manifest's
// location dictionary.
type DictDecoder struct {
	mu  sync.Mutex
	dec *zstd.Decoder
}

// NewDictDecoder builds a decoder from a dictionary in zstd's standard format
// (as produced by ZDICT_trainFromBuffer). Raw-content dictionaries, which
// lack the zstd dictionary magic, are also accepted.
func NewDictDecoder(dict []byte) (*DictDecoder, error) {
	var opt zstd.DOption
	if len(dict) >= 4 && bytes.Equal(dict[:4], []byte{0x37, 0xA4, 0x30, 0xEC}) {
		opt = zstd.WithDecoderDicts(dict)
	} else {
		opt = zstd.WithDecoderDictRaw(0, dict)
	}
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), opt)
	if err != nil {
		return nil, fmt.Errorf("%w: loading location dictionary: %v", ErrFormat, err)
	}
	return &DictDecoder{dec: d}, nil
}

// Decode decompresses one frame.
func (d *DictDecoder) Decode(src []byte) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dec.DecodeAll(src, nil)
}
