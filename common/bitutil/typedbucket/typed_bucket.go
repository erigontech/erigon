package typedbucket

import (
	"bytes"
	"errors"

	"github.com/ledgerwatch/bolt"
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

func (b *Uint64) Increment(key []byte) error {
	value, _ := b.b.Get(key)
	if value == nil {
		return b.Put(key, 1)
	}
	var v uint64
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return b.Put(key, v+1)
}

func (b *Uint64) Decrement(key []byte) error {
	value, _ := b.b.Get(key)
	if value == nil {
		// return ethdb.ErrNotFound
		return errors.New("not found key")
	}
	var v uint64
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	if v == 0 {
		return errors.New("could not decrement zero")
	}
	return b.Put(key, v-1)
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

type Int struct {
	b *bolt.Bucket
}

func NewInt(b *bolt.Bucket) *Int {
	return &Int{b: b}
}

func (b *Int) Get(key []byte) (int, bool) {
	value, _ := b.b.Get(key)
	if value == nil {
		return 0, false
	}
	var v int
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return v, true
}

func (b *Int) Increment(key []byte) error {
	value, _ := b.b.Get(key)
	if value == nil {
		return b.Put(key, 1)
	}
	var v int
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return b.Put(key, v+1)
}

func (b *Int) Decrement(key []byte) error {
	value, _ := b.b.Get(key)
	if value == nil {
		return b.Put(key, -1)
	}
	var v int
	decoder := codecpool.Decoder(bytes.NewReader(value))
	defer codecpool.Return(decoder)

	decoder.MustDecode(&v)
	return b.Put(key, v-1)
}

func (b *Int) Put(key []byte, value int) error {
	var buf bytes.Buffer

	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)

	encoder.MustEncode(&value)
	return b.b.Put(key, buf.Bytes())
}

func (b *Int) ForEach(fn func([]byte, int) error) error {
	return b.b.ForEach(func(k, v []byte) error {
		var value int
		decoder := codecpool.Decoder(bytes.NewReader(v))
		defer codecpool.Return(decoder)

		decoder.MustDecode(&value)
		return fn(k, value)
	})
}
