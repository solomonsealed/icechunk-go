package icechunk

import (
	"bytes"
	"fmt"
)

// crockford is the Crockford base32 alphabet used for object ids in paths.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var crockfordDecode = func() [256]int8 {
	var t [256]int8
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(crockford); i++ {
		t[crockford[i]] = int8(i)
		t[crockford[i]|0x20] = int8(i) // lowercase
	}
	// Crockford aliases for commonly confused symbols.
	for _, c := range []byte{'O', 'o'} {
		t[c] = 0
	}
	for _, c := range []byte{'I', 'i', 'L', 'l'} {
		t[c] = 1
	}
	return t
}()

// encodeCrockford encodes b as uppercase Crockford base32 without padding.
// When the bit count is not a multiple of 5, zero bits are appended.
func encodeCrockford(b []byte) string {
	out := make([]byte, 0, (len(b)*8+4)/5)
	var acc uint32
	bits := 0
	for _, x := range b {
		acc = acc<<8 | uint32(x)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out = append(out, crockford[(acc>>uint(bits))&31])
		}
	}
	if bits > 0 {
		out = append(out, crockford[(acc<<uint(5-bits))&31])
	}
	return string(out)
}

func decodeCrockford(s string, n int) ([]byte, error) {
	if len(s) != (n*8+4)/5 {
		return nil, fmt.Errorf("icechunk: invalid id %q: expected %d characters", s, (n*8+4)/5)
	}
	out := make([]byte, 0, n)
	var acc uint32
	bits := 0
	for i := 0; i < len(s); i++ {
		v := crockfordDecode[s[i]]
		if v < 0 {
			return nil, fmt.Errorf("icechunk: invalid id %q: bad character %q", s, s[i])
		}
		acc = acc<<5 | uint32(v)
		bits += 5
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(acc>>uint(bits)))
		}
	}
	return out[:n], nil
}

// ObjectID12 is a 12-byte id: snapshots, manifests, chunks and transaction logs.
type ObjectID12 [12]byte

// String returns the Crockford base32 form used in storage paths.
func (id ObjectID12) String() string { return encodeCrockford(id[:]) }

// IsZero reports whether the id is all zeros.
func (id ObjectID12) IsZero() bool { return id == ObjectID12{} }

// Compare orders ids by their raw bytes, the sort order used on disk.
func (id ObjectID12) Compare(o ObjectID12) int { return bytes.Compare(id[:], o[:]) }

// SnapshotID identifies a snapshot (a commit).
type SnapshotID = ObjectID12

// ManifestID identifies a chunk manifest file.
type ManifestID = ObjectID12

// ChunkID identifies a chunk file under chunks/.
type ChunkID = ObjectID12

// NodeID identifies an array or group. It is stable across snapshots.
type NodeID [8]byte

// String returns the Crockford base32 form of the node id.
func (id NodeID) String() string { return encodeCrockford(id[:]) }

// ParseObjectID12 parses the 20-character Crockford base32 form of an id.
func ParseObjectID12(s string) (ObjectID12, error) {
	var id ObjectID12
	b, err := decodeCrockford(s, 12)
	if err != nil {
		return id, err
	}
	copy(id[:], b)
	return id, nil
}

// ParseSnapshotID parses a snapshot id such as "1CECHNKREP0F1RSTCMT0".
func ParseSnapshotID(s string) (SnapshotID, error) { return ParseObjectID12(s) }

// ParseNodeID parses the 13-character Crockford base32 form of a node id.
func ParseNodeID(s string) (NodeID, error) {
	var id NodeID
	b, err := decodeCrockford(s, 8)
	if err != nil {
		return id, err
	}
	copy(id[:], b)
	return id, nil
}
