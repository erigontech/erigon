package page

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/erigontech/erigon-lib/common/compress"
)

func NewWriter(parent io.Writer, limit int, snappyEnabled bool) *Writer {
	return &Writer{parent: parent, limit: limit, snappyEnabled: snappyEnabled}
}

type Writer struct {
	parent             io.Writer
	i, limit           int
	keys, vals         []byte
	kLengths, vLengths []int

	snappyBuf     []byte
	snappyEnabled bool
}

func (c *Writer) Empty() bool { return c.i == 0 }
func (c *Writer) Add(k, v []byte) (err error) {
	c.i++
	c.kLengths = append(c.kLengths, len(k))
	c.vLengths = append(c.vLengths, len(v))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.i%c.limit == 0
	if isFull {
		bts := c.bytesAndReset()
		_, err = c.parent.Write(bts)
		return err
	}
	return nil
}

func (c *Writer) Reset() {
	c.i = 0
	c.kLengths, c.vLengths = c.kLengths[:0], c.vLengths[:0]
	c.keys, c.vals = c.keys[:0], c.vals[:0]
}
func (c *Writer) Flush() error {
	defer c.Reset()
	if !c.Empty() {
		bts := c.bytesAndReset()
		_, err := c.parent.Write(bts)
		return err
	}
	return nil
}

func (c *Writer) bytesAndReset() []byte {
	v := c.bytes()
	c.Reset()
	return v
}

func (c *Writer) bytes() []byte {
	//TODO: alignment,compress+alignment

	c.keys = append(c.keys, c.vals...)
	keysAndVals := c.keys

	c.vals = growslice(c.vals[:0], 1+len(c.kLengths)*2*4)
	for i := range c.vals {
		c.vals[i] = 0
	}
	c.vals[0] = uint8(len(c.kLengths)) // first byte is amount of vals
	lensBuf := c.vals[1:]
	for i, l := range c.kLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}
	lensBuf = lensBuf[len(c.kLengths)*4:]
	for i, l := range c.vLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}

	c.vals = append(c.vals, keysAndVals...)
	lengthsAndKeysAndVals := c.vals

	c.snappyBuf, lengthsAndKeysAndVals = compress.EncodeSnappyIfNeed(c.snappyBuf, lengthsAndKeysAndVals, c.snappyEnabled)

	return lengthsAndKeysAndVals
}

var be = binary.BigEndian

func Get(key, buf []byte, snappyEnabled bool) []byte {
	var err error
	_, buf, err = compress.DecodeSnappyIfNeed(nil, buf, snappyEnabled)
	if err != nil {
		panic(err)
	}

	cnt := int(buf[0])
	if cnt == 0 {
		return nil
	}
	kLens, vLens, data := buf[1:1+cnt*4], buf[1+cnt*4:1+cnt*4*2], buf[1+cnt*4*2:]
	var kOffset, vOffset uint32
	for i := 0; i < cnt*4; i += 4 {
		vOffset += be.Uint32(kLens[i:])
	}

	for i := 0; i < cnt*4; i += 4 {
		kLen, vLen := be.Uint32(kLens[i:]), be.Uint32(vLens[i:])
		if bytes.Equal(key, data[kOffset:kOffset+kLen]) {
			return data[vOffset : vOffset+vLen]
		} else {
			_ = data
		}
		kOffset += kLen
		vOffset += vLen
	}

	from := be.Uint32(kLens[(cnt-1)*4:])
	if bytes.Equal(key, data[from:]) {
		from = be.Uint32(vLens[(cnt-1)*4:])
		return data[from:]
	}

	return nil
}

type Reader struct {
	i, limit           int
	kLens, vLens, data []byte
	kOffset, vOffset   uint32

	snappyBuf []byte
}

func FromBytes(buf []byte, snappyEnabled bool) *Reader {
	r := &Reader{}
	r.Reset(buf, snappyEnabled)
	return r
}

func (r *Reader) Reset(v []byte, snappyEnabled bool) (n int) {
	var err error
	vcpu := v
	_ = vcpu
	r.snappyBuf, v, err = compress.DecodeSnappyIfNeed(r.snappyBuf, v, snappyEnabled)
	if err != nil {
		panic(err)
	}

	r.i, r.kOffset, r.vOffset = 0, 0, 0
	r.limit = int(v[0])
	meta, data := v[1:1+r.limit*4*2], v[1+r.limit*4*2:]
	r.kLens, r.vLens, r.data = meta[:r.limit*4], meta[r.limit*4:r.limit*4*2], data

	for i := 0; i < r.limit*4; i += 4 {
		r.vOffset += be.Uint32(r.kLens[i:])
	}
	return
}
func (r *Reader) HasNext() bool { return r.limit > r.i }
func (r *Reader) Next() (k, v []byte) {
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	r.i++
	r.kOffset += kLen
	r.vOffset += vLen
	return k, v
}

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength)
}
