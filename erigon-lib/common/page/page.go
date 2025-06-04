package page

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/compress"
)

var be = binary.BigEndian

func Get(key, compressedPage []byte, compressionBuf []byte, compressionEnabled bool) (v []byte, compressionBufOut []byte) {
	var err error
	var page []byte
	compressionBuf, page, err = compress.DecodeZstdIfNeed(compressionBuf, compressedPage, compressionEnabled)
	if err != nil {
		panic(err)
	}

	cnt := int(page[0])
	if cnt == 0 {
		return nil, compressionBuf
	}
	meta, data := page[1:1+cnt*4*2], page[1+cnt*4*2:]
	kLens, vLens := meta[:cnt*4], meta[cnt*4:]
	var kOffset, vOffset uint32
	for i := 0; i < cnt*4; i += 4 {
		vOffset += be.Uint32(kLens[i:])
	}
	keys := data[:vOffset]
	vals := data[vOffset:]
	vOffset = 0
	//fmt.Printf("[dbg] see(%x): %x, %x\n", key, keys, vals)

	for i := 0; i < cnt*4; i += 4 {
		kLen, vLen := be.Uint32(kLens[i:]), be.Uint32(vLens[i:])
		foundKey := keys[kOffset : kOffset+kLen]
		if bytes.Equal(key, foundKey) {
			return vals[vOffset : vOffset+vLen], compressionBuf
		} else {
			_ = data
		}
		kOffset += kLen
		vOffset += vLen
	}
	return nil, compressionBuf
}

type Page struct {
	i, limit           int
	kLens, vLens, data []byte
	kOffset, vOffset   uint32

	compressionBuf []byte
}

func FromBytes(buf []byte, compressionEnabled bool) *Page {
	r := &Page{}
	r.Reset(buf, compressionEnabled)
	return r
}

func (r *Page) Reset(v []byte, compressionEnabled bool) {
	var err error
	r.compressionBuf, v, err = compress.DecodeZstdIfNeed(r.compressionBuf, v, compressionEnabled)
	if err != nil {
		panic(fmt.Errorf("len(v): %d, %w", len(v), err))
	}

	r.limit = int(v[0])
	meta, data := v[1:1+r.limit*4*2], v[1+r.limit*4*2:]
	r.kLens, r.vLens, r.data = meta[:r.limit*4], meta[r.limit*4:r.limit*4*2], data
	r.reset()
}
func (r *Page) reset() {
	r.i, r.kOffset, r.vOffset = 0, 0, 0
	for i := 0; i < r.limit*4; i += 4 {
		r.vOffset += be.Uint32(r.kLens[i:])
	}
}

func (r *Page) HasNext() bool { return r.limit > r.i }
func (r *Page) Current() (k, v []byte) {
	if r.i >= r.limit {
		return nil, nil
	}
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	return k, v
}
func (r *Page) Next() (k, v []byte) {
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	r.i++
	r.kOffset += kLen
	r.vOffset += vLen
	return k, v
}
func (r *Page) Seek(seekKey []byte) (k, v []byte) {
	r.reset()
	for r.HasNext() {
		k, v = r.Next()
		if bytes.Compare(k, seekKey) >= 0 {
			return k, v
		}
	}
	return nil, nil
}
func (r *Page) Get(getKey []byte) (v []byte) {
	r.reset()

	var k []byte
	for r.HasNext() {
		k, v = r.Next()
		if bytes.Equal(getKey, k) {
			return v
		}
	}
	return nil
}
func (r *Page) First() (k, v []byte) {
	r.reset()
	if r.HasNext() {
		return r.Next()
	}
	return nil, nil
}
func (r *Page) Last() (k, v []byte) {
	r.reset()
	for r.HasNext() {
		k, v = r.Next()
	}
	return k, v
}

func WordsAmount2PagesAmount(wordsAmount int, pageSize int) (pagesAmount int) {
	pagesAmount = wordsAmount
	if wordsAmount == 0 {
		return 0
	}
	if pageSize > 0 {
		pagesAmount = (wordsAmount-1)/pageSize + 1 //amount of pages
	}
	return pagesAmount
}
