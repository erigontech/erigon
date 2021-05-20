package typedcursor

import (
	"bytes"
	"errors"

	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
)

type Uint64 struct {
	ethdb.RwCursor
}

func NewUint64(b ethdb.RwCursor) *Uint64 {
	return &Uint64{b}
}

func (b *Uint64) Get(key []byte) (uint64, bool) {
	_, value, _ := b.RwCursor.SeekExact(key)
	if value == nil {
		return 0, false
	}

	var v uint64
	decoder := cbor.Decoder(bytes.NewReader(value))
	defer cbor.Return(decoder)

	decoder.MustDecode(&v)
	return v, true
}

func (b *Uint64) Increment(key []byte) error {
	v, _ := b.Get(key)
	return b.Put(key, v+1)
}

func (b *Uint64) Decrement(key []byte) error {
	v, ok := b.Get(key)
	if !ok {
		// return ethdb.ErrNotFound
		return errors.New("not found key")
	}

	if v == 0 {
		return errors.New("could not decrement zero")
	}

	return b.Put(key, v-1)
}

func (b *Uint64) DecrementIfExist(key []byte) error {
	v, ok := b.Get(key)
	if !ok {
		return nil
	}

	if v == 0 {
		return errors.New("could not decrement zero")
	}

	return b.Put(key, v-1)
}

func (b *Uint64) Put(key []byte, value uint64) error {
	var buf bytes.Buffer

	encoder := cbor.Encoder(&buf)
	defer cbor.Return(encoder)

	encoder.MustEncode(&value)
	return b.RwCursor.Put(key, buf.Bytes())
}

func (b *Uint64) ForEach(fn func([]byte, uint64) error) error {
	return ethdb.ForEach(b.RwCursor, func(k, v []byte) (bool, error) {
		var value uint64
		decoder := cbor.Decoder(bytes.NewReader(v))
		defer cbor.Return(decoder)

		decoder.MustDecode(&value)
		return true, fn(k, value)
	})
}
