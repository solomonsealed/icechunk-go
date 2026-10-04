package icechunk

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
)

// ChunkRefKind says where a chunk's bytes live.
type ChunkRefKind uint8

const (
	// InlineChunk bytes are stored in the manifest itself.
	InlineChunk ChunkRefKind = iota + 1
	// NativeChunk bytes live in the repository under chunks/.
	NativeChunk
	// VirtualChunk bytes live in an external object, addressed by URL.
	VirtualChunk
)

func (k ChunkRefKind) String() string {
	switch k {
	case InlineChunk:
		return "inline"
	case NativeChunk:
		return "native"
	case VirtualChunk:
		return "virtual"
	}
	return "unknown"
}

// ChunkRef locates the encoded bytes of one chunk.
type ChunkRef struct {
	Kind ChunkRefKind
	// Inline holds the chunk bytes for inline refs.
	Inline []byte
	// ID is the chunk file for native refs.
	ID ChunkID
	// Location is the absolute URL of the containing object for virtual refs.
	Location string
	// Offset and Length locate the chunk within its object (native, virtual).
	Offset, Length uint64
	// ETag, when set, must match the virtual object's current ETag.
	ETag string
	// LastModified, when non-zero, is the latest acceptable modification time
	// of the virtual object.
	LastModified time.Time
}

// Size returns the encoded size of the chunk in bytes.
func (c *ChunkRef) Size() uint64 {
	if c.Kind == InlineChunk {
		return uint64(len(c.Inline))
	}
	return c.Length
}

// Manifest is a decoded manifest file. Chunk refs are looked up by binary
// search directly in the flatbuffer; they are never materialized in bulk.
type Manifest struct {
	header format.Header
	buf    []byte
	root   *fbs.Manifest
	id     ManifestID

	dictOnce sync.Once
	dict     *format.DictDecoder
	dictErr  error
}

func parseManifest(raw []byte) (m *Manifest, err error) {
	defer recoverFormat(&err)
	h, buf, err := format.Decode(raw, format.FileTypeManifest)
	if err != nil {
		return nil, err
	}
	m = &Manifest{header: h, buf: buf, root: fbs.GetRootAsManifest(buf, 0)}
	m.id = idFrom12(m.root.Id(nil))
	_ = m.root.ArraysLength()
	return m, nil
}

// ID returns the manifest id.
func (m *Manifest) ID() ManifestID { return m.id }

func (m *Manifest) arrayManifest(node NodeID) (*fbs.ArrayManifest, bool) {
	n := m.root.ArraysLength()
	var am fbs.ArrayManifest
	i := sort.Search(n, func(i int) bool {
		m.root.Arrays(&am, i)
		return bytes.Compare(am.NodeId(nil).Bytes(), node[:]) >= 0
	})
	if i >= n {
		return nil, false
	}
	m.root.Arrays(&am, i)
	if !bytes.Equal(am.NodeId(nil).Bytes(), node[:]) {
		return nil, false
	}
	return &am, true
}

func compareIndex(ref *fbs.ChunkRef, coords []uint32) int {
	n := ref.IndexLength()
	for i := 0; i < n && i < len(coords); i++ {
		a, b := ref.Index(i), coords[i]
		if a != b {
			if a < b {
				return -1
			}
			return 1
		}
	}
	switch {
	case n < len(coords):
		return -1
	case n > len(coords):
		return 1
	}
	return 0
}

// lookup finds the chunk ref of node at coords.
func (m *Manifest) lookup(node NodeID, coords []uint32) (ref *ChunkRef, found bool, err error) {
	defer recoverFormat(&err)
	am, ok := m.arrayManifest(node)
	if !ok {
		return nil, false, nil
	}
	n := am.RefsLength()
	var cr fbs.ChunkRef
	i := sort.Search(n, func(i int) bool {
		am.Refs(&cr, i)
		return compareIndex(&cr, coords) >= 0
	})
	if i >= n {
		return nil, false, nil
	}
	am.Refs(&cr, i)
	if compareIndex(&cr, coords) != 0 {
		return nil, false, nil
	}
	ref, err = m.decodeRef(&cr)
	return ref, err == nil, err
}

// forEach calls fn for every chunk ref of node, in coordinate order.
func (m *Manifest) forEach(node NodeID, fn func(coords []uint32, ref *ChunkRef) error) (err error) {
	defer recoverFormat(&err)
	am, ok := m.arrayManifest(node)
	if !ok {
		return nil
	}
	var cr fbs.ChunkRef
	for i := 0; i < am.RefsLength(); i++ {
		am.Refs(&cr, i)
		coords := make([]uint32, vecLen(cr.IndexLength(), m.buf))
		for j := range coords {
			coords[j] = cr.Index(j)
		}
		ref, err := m.decodeRef(&cr)
		if err != nil {
			return err
		}
		if err := fn(coords, ref); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manifest) decodeRef(cr *fbs.ChunkRef) (*ChunkRef, error) {
	if id := cr.ChunkId(nil); id != nil {
		return &ChunkRef{Kind: NativeChunk, ID: idFrom12(id), Offset: cr.Offset(), Length: cr.Length()}, nil
	}
	ref := &ChunkRef{Kind: VirtualChunk, Offset: cr.Offset(), Length: cr.Length()}
	if comp := cr.CompressedLocationBytes(); comp != nil {
		loc, err := m.decompressLocation(comp)
		if err != nil {
			return nil, err
		}
		ref.Location = loc
	} else if loc := cr.Location(); loc != nil {
		ref.Location = string(loc)
	} else if inline := cr.InlineBytes(); inline != nil {
		return &ChunkRef{Kind: InlineChunk, Inline: append([]byte{}, inline...)}, nil
	} else {
		return nil, fmt.Errorf("%w: chunk ref is neither inline, native nor virtual", ErrFormat)
	}
	if etag := cr.ChecksumEtag(); etag != nil {
		ref.ETag = string(etag)
	} else if lm := cr.ChecksumLastModified(); lm > 0 {
		ref.LastModified = time.Unix(int64(lm), 0).UTC()
	}
	return ref, nil
}

// compressionZstdDict is Manifest.compression_algorithm for dictionary zstd.
const compressionZstdDict = 1

func (m *Manifest) decompressLocation(comp []byte) (string, error) {
	m.dictOnce.Do(func() {
		dict := m.root.LocationDictionaryBytes()
		if m.root.CompressionAlgorithm() != compressionZstdDict || dict == nil {
			m.dictErr = fmt.Errorf("%w: manifest has compressed virtual locations but no location dictionary", ErrFormat)
			return
		}
		m.dict, m.dictErr = format.NewDictDecoder(dict)
	})
	if m.dictErr != nil {
		return "", m.dictErr
	}
	out, err := m.dict.Decode(comp)
	if err != nil {
		return "", fmt.Errorf("%w: decompressing virtual chunk location: %v", ErrFormat, err)
	}
	return string(out), nil
}
