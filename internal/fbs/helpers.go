package fbs

// The upstream schema declares object ids as fixed-size byte arrays, which the
// flatc Go backend cannot generate. gen.sh rewrites them into N single-byte
// fields with the identical layout; these helpers expose the raw bytes.

// Bytes returns the 12 id bytes. The slice aliases the flatbuffer.
func (rcv *ObjectId12) Bytes() []byte {
	return rcv._tab.Bytes[rcv._tab.Pos : rcv._tab.Pos+12]
}

// Bytes returns the 8 id bytes. The slice aliases the flatbuffer.
func (rcv *ObjectId8) Bytes() []byte {
	return rcv._tab.Bytes[rcv._tab.Pos : rcv._tab.Pos+8]
}

// HasShapeV2 reports whether the optional shape_v2 field (vtable slot 10) is
// present. It is absent in spec v1 snapshots, where `shape` holds the shape.
func (rcv *ArrayNodeData) HasShapeV2() bool {
	return rcv._tab.Offset(10) != 0
}
