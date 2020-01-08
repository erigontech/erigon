package typedbucket

import (
	"bytes"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
)

type Uint64 struct {
	b *bolt.Bucket
}

func NewUint64(b *bolt.Bucket) *Uint64 {
	return &Uint64{b: b}
}

func (b *Uint64) Get(key []byte) (uint64, bool) {
	value, _ := b.b.Get(key)
	if value == nil {
		return 0, false
	}
	var v uint64
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return v, true
}

func (b *Uint64) Put(key []byte, value uint64) error {
	var buf bytes.Buffer

	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)

	encoder.MustEncode(&value)
	return b.b.Put(key, buf.Bytes())
}

func (b *Uint64) ForEach(fn func([]byte, uint64) error) error {
	return b.b.ForEach(func(k, v []byte) error {
		var value uint64
		decoder := codecpool.Decoder(bytes.NewReader(v))
		defer codecpool.Return(decoder)

		decoder.MustDecode(&value)
		return fn(k, value)
	})
}

type MapHash2Uint64 struct {
	b *bolt.Bucket
}

func NewMapHash2Uint64(b *bolt.Bucket) *MapHash2Uint64 {
	return &MapHash2Uint64{b: b}
}

func (b *MapHash2Uint64) Get(key []byte) (map[common.Hash]uint64, bool) {
	value, _ := b.b.Get(key)
	if value == nil {
		return nil, false
	}
	var v map[common.Hash]uint64

	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return v, true
}

func (b *MapHash2Uint64) Put(key []byte, value map[common.Hash]uint64) error {
	var buf bytes.Buffer

	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)

	encoder.MustEncode(&value)
	return b.b.Put(key, buf.Bytes())
}

func (b *MapHash2Uint64) ForEach(fn func([]byte, map[common.Hash]uint64) error) error {
	return b.b.ForEach(func(k, v []byte) error {
		var value map[common.Hash]uint64

		decoder := codecpool.Decoder(bytes.NewReader(v))
		defer codecpool.Return(decoder)

		decoder.MustDecode(&value)
		return fn(k, value)
	})
}
