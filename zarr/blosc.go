package zarr

import (
	"encoding/binary"
	"fmt"
)

// Blosc (c-blosc 1.x frame format) decoding, see
// https://github.com/Blosc/c-blosc/blob/main/README_HEADER.rst.
//
// Header (16 bytes): version, versionlz, flags, typesize, nbytes (uint32),
// blocksize (uint32), cbytes (uint32). Flags: bit 0 byte shuffle, bit 1
// memcpyed (stored uncompressed), bit 2 bit shuffle, bit 4 blocks not split,
// bits 5-7 compressor (0 blosclz, 1 lz4/lz4hc, 2 snappy, 3 zlib, 4 zstd).

const (
	bloscHeaderLen   = 16
	bloscDoShuffle   = 0x1
	bloscMemcpyed    = 0x2
	bloscBitShuffle  = 0x4
	bloscDontSplit   = 0x10
	bloscMaxSplits   = 16
	bloscMinBuffer   = 128
	bloscMaxDistance = 8191
)

func bloscDecompress(in []byte) ([]byte, error) {
	if len(in) < bloscHeaderLen {
		return nil, fmt.Errorf("zarr: blosc buffer too short")
	}
	version := in[0]
	flags := in[2]
	typesize := int(in[3])
	nbytes := int(binary.LittleEndian.Uint32(in[4:]))
	blocksize := int(binary.LittleEndian.Uint32(in[8:]))
	cbytes := int(binary.LittleEndian.Uint32(in[12:]))
	if cbytes > len(in) || nbytes < 0 {
		return nil, fmt.Errorf("zarr: blosc header claims %d compressed bytes, have %d", cbytes, len(in))
	}
	in = in[:cbytes]
	out := make([]byte, nbytes)
	if nbytes == 0 {
		return out, nil
	}
	if flags&bloscMemcpyed != 0 {
		if bloscHeaderLen+nbytes > len(in) {
			return nil, fmt.Errorf("zarr: truncated memcpyed blosc buffer")
		}
		copy(out, in[bloscHeaderLen:])
		return out, nil
	}
	if blocksize <= 0 || typesize <= 0 {
		return nil, fmt.Errorf("zarr: invalid blosc header")
	}
	nblocks := (nbytes + blocksize - 1) / blocksize
	if bloscHeaderLen+4*nblocks > len(in) {
		return nil, fmt.Errorf("zarr: truncated blosc block table")
	}
	compformat := int(flags >> 5)
	doShuffle := flags&bloscDoShuffle != 0 && typesize > 1
	tmp := make([]byte, blocksize)
	for b := 0; b < nblocks; b++ {
		start := int(binary.LittleEndian.Uint32(in[bloscHeaderLen+4*b:]))
		bsize := blocksize
		leftover := false
		if b == nblocks-1 && nbytes%blocksize != 0 {
			bsize = nbytes % blocksize
			leftover = true
		}
		doBitShuffle := flags&bloscBitShuffle != 0 && bsize >= typesize
		dest := out[b*blocksize : b*blocksize+bsize]
		target := dest
		if doShuffle || doBitShuffle {
			target = tmp[:bsize]
		}

		nsplits := 1
		if flags&bloscDontSplit == 0 && typesize <= bloscMaxSplits && blocksize/typesize >= bloscMinBuffer && !leftover {
			nsplits = typesize
		}
		neblock := bsize / nsplits
		pos := start
		for s := 0; s < nsplits; s++ {
			if pos < 0 || pos+4 > len(in) {
				return nil, fmt.Errorf("zarr: truncated blosc block")
			}
			csize := int(binary.LittleEndian.Uint32(in[pos:]))
			pos += 4
			if csize < 0 || pos+csize > len(in) {
				return nil, fmt.Errorf("zarr: truncated blosc split")
			}
			src := in[pos : pos+csize]
			dst := target[s*neblock : (s+1)*neblock]
			if csize == neblock {
				copy(dst, src)
			} else if err := bloscDecodeSplit(compformat, src, dst); err != nil {
				return nil, err
			}
			pos += csize
		}

		switch {
		case doShuffle:
			unshuffle(typesize, target, dest)
		case doBitShuffle:
			bitUnshuffle(typesize, target, dest, version)
		}
	}
	return out, nil
}

func bloscDecodeSplit(compformat int, src, dst []byte) error {
	switch compformat {
	case 0:
		n, err := blosclzDecompress(src, dst)
		if err != nil {
			return err
		}
		if n != len(dst) {
			return fmt.Errorf("zarr: blosclz produced %d bytes, expected %d", n, len(dst))
		}
		return nil
	case 1:
		return lz4Block(src, dst)
	case 3:
		out, err := unzlib(src)
		if err != nil {
			return err
		}
		if len(out) != len(dst) {
			return fmt.Errorf("zarr: blosc zlib split has %d bytes, expected %d", len(out), len(dst))
		}
		copy(dst, out)
		return nil
	case 4:
		out, err := unzstd(src)
		if err != nil {
			return err
		}
		if len(out) != len(dst) {
			return fmt.Errorf("zarr: blosc zstd split has %d bytes, expected %d", len(out), len(dst))
		}
		copy(dst, out)
		return nil
	case 2:
		return fmt.Errorf("%w: blosc snappy compression", ErrUnsupported)
	}
	return fmt.Errorf("%w: blosc compressor format %d", ErrUnsupported, compformat)
}

// lz4Block decodes one raw LZ4 block into exactly len(dst) bytes.
func lz4Block(src, dst []byte) error {
	si, di := 0, 0
	for si < len(src) {
		token := src[si]
		si++
		lit := int(token >> 4)
		if lit == 15 {
			for {
				if si >= len(src) {
					return fmt.Errorf("zarr: corrupt lz4 block")
				}
				b := src[si]
				si++
				lit += int(b)
				if b != 255 {
					break
				}
			}
		}
		if si+lit > len(src) || di+lit > len(dst) {
			return fmt.Errorf("zarr: corrupt lz4 block (literals)")
		}
		copy(dst[di:], src[si:si+lit])
		si += lit
		di += lit
		if si >= len(src) {
			break
		}
		if si+2 > len(src) {
			return fmt.Errorf("zarr: corrupt lz4 block (offset)")
		}
		off := int(src[si]) | int(src[si+1])<<8
		si += 2
		ml := int(token & 15)
		if ml == 15 {
			for {
				if si >= len(src) {
					return fmt.Errorf("zarr: corrupt lz4 block")
				}
				b := src[si]
				si++
				ml += int(b)
				if b != 255 {
					break
				}
			}
		}
		ml += 4
		if off == 0 || off > di || di+ml > len(dst) {
			return fmt.Errorf("zarr: corrupt lz4 block (match)")
		}
		m := di - off
		for k := 0; k < ml; k++ {
			dst[di+k] = dst[m+k]
		}
		di += ml
	}
	if di != len(dst) {
		return fmt.Errorf("zarr: lz4 block produced %d bytes, expected %d", di, len(dst))
	}
	return nil
}

// blosclzDecompress decodes BloscLZ (a FastLZ level-2 derivative) and
// returns the number of bytes written.
func blosclzDecompress(src, dst []byte) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}
	corrupt := fmt.Errorf("zarr: corrupt blosclz stream")
	ip, op := 0, 0
	ctrl := int(src[ip]) & 31
	ip++
	for {
		if ctrl >= 32 {
			length := (ctrl >> 5) - 1
			ofs := (ctrl & 31) << 8
			if length == 6 {
				for {
					if ip >= len(src) {
						return 0, corrupt
					}
					code := int(src[ip])
					ip++
					length += code
					if code != 255 {
						break
					}
				}
			}
			if ip >= len(src) {
				return 0, corrupt
			}
			code := int(src[ip])
			ip++
			length += 3
			ref := op - ofs - code
			if code == 255 && ofs == 31<<8 {
				if ip+2 > len(src) {
					return 0, corrupt
				}
				ofs = int(src[ip])<<8 | int(src[ip+1])
				ip += 2
				ref = op - ofs - bloscMaxDistance
			}
			ref--
			if ref < 0 || op+length > len(dst) {
				return 0, corrupt
			}
			for k := 0; k < length; k++ {
				dst[op+k] = dst[ref+k]
			}
			op += length
		} else {
			ctrl++
			if ip+ctrl > len(src) || op+ctrl > len(dst) {
				return 0, corrupt
			}
			copy(dst[op:], src[ip:ip+ctrl])
			ip += ctrl
			op += ctrl
		}
		if ip >= len(src) {
			break
		}
		ctrl = int(src[ip])
		ip++
	}
	return op, nil
}

// unshuffle reverses the byte shuffle: src groups byte j of every element
// together. Trailing bytes that do not form a whole element are copied.
func unshuffle(typesize int, src, dst []byte) {
	n := len(src) / typesize
	for j := 0; j < typesize; j++ {
		row := src[j*n : (j+1)*n]
		for i, b := range row {
			dst[i*typesize+j] = b
		}
	}
	copy(dst[n*typesize:], src[n*typesize:])
}

// bitUnshuffle reverses bitshuffle: bit-row r = 8*byte + bit holds that bit
// of every element, packed LSB first. Bitshuffle only handles multiples of 8
// elements: format version 2 (c-blosc 1.x) then stores the whole block
// unshuffled, version 3+ shuffles the largest multiple of 8 and stores the
// remaining elements verbatim.
func bitUnshuffle(typesize int, src, dst []byte, version byte) {
	n := len(src) / typesize
	if n%8 != 0 {
		if version <= 2 {
			copy(dst, src)
			return
		}
		n -= n % 8
	}
	rowBytes := n / 8
	for i := range dst[:n*typesize] {
		dst[i] = 0
	}
	for j := 0; j < typesize; j++ {
		for k := 0; k < 8; k++ {
			row := src[(j*8+k)*rowBytes : (j*8+k+1)*rowBytes]
			for bi, bits := range row {
				if bits == 0 {
					continue
				}
				for m := 0; m < 8; m++ {
					if bits&(1<<m) != 0 {
						dst[(bi*8+m)*typesize+j] |= 1 << k
					}
				}
			}
		}
	}
	copy(dst[n*typesize:], src[n*typesize:])
}
