package typedtree

import (
	"github.com/armon/go-radix"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ugorji/go/codec"
)

// string -> map[common.Hash]uint64
type RadixMapHash2Uint64 struct {
	version string
	tree    *radix.Tree
}

func NewRadixMapHash2Uint64() *RadixMapHash2Uint64 {
	return &RadixMapHash2Uint64{
		version: "1",
		tree:    radix.New(),
	}
}

func (t *RadixMapHash2Uint64) Len() int {
	return t.tree.Len()
}

func (t *RadixMapHash2Uint64) Get(k string) (map[common.Hash]uint64, bool) {
	v, ok := t.tree.Get(k)
	if !ok {
		return nil, false
	}
	return v.(map[common.Hash]uint64), true
}

func (t *RadixMapHash2Uint64) Set(k string, v map[common.Hash]uint64) {
	t.tree.Insert(k, v)
}

func (t *RadixMapHash2Uint64) Walk(f func(string, map[common.Hash]uint64) bool) {
	t.tree.Walk(func(k string, v interface{}) bool {
		return f(k, v.(map[common.Hash]uint64))
	})
}

func (t *RadixMapHash2Uint64) CodecEncodeSelf(e *codec.Encoder) {
	e.MustEncode(t.version)
	e.MustEncode(t.tree.Len())
	t.tree.Walk(func(k string, v interface{}) bool {
		e.MustEncode(&k)
		e.MustEncode(&v)
		return false
	})
}

func (t *RadixMapHash2Uint64) CodecDecodeSelf(d *codec.Decoder) {
	var version string
	d.MustDecode(&version)
	if version != t.version {
		panic("unexpected version")
	}
	var amount int
	d.MustDecode(&amount)
	var k string
	var v map[common.Hash]uint64
	for i := 0; i < amount; i++ {
		d.MustDecode(&k)
		d.MustDecode(&v)
		t.tree.Insert(k, v)
	}
}

// string ->  map[uint64]int
type RadixMapUint642Int struct {
	version string
	tree    *radix.Tree
}

func NewRadixMapUint642Int() *RadixMapUint642Int {
	return &RadixMapUint642Int{
		version: "1",
		tree:    radix.New(),
	}
}

func (t *RadixMapUint642Int) Len() int {
	return t.tree.Len()
}

func (t *RadixMapUint642Int) Get(k string) (map[uint64]int, bool) {
	v, ok := t.tree.Get(k)
	if !ok {
		return nil, false
	}
	return v.(map[uint64]int), true
}

func (t *RadixMapUint642Int) Set(k string, v map[uint64]int) {
	t.tree.Insert(k, v)
}
func (t *RadixMapUint642Int) Walk(f func(string, map[uint64]int) bool) {
	t.tree.Walk(func(k string, v interface{}) bool {
		return f(k, v.(map[uint64]int))
	})
}

func (t *RadixMapUint642Int) CodecEncodeSelf(e *codec.Encoder) {
	e.MustEncode(t.version)
	e.MustEncode(t.tree.Len())
	t.tree.Walk(func(k string, v interface{}) bool {
		e.MustEncode(&k)
		e.MustEncode(&v)
		return false
	})
}

func (t *RadixMapUint642Int) CodecDecodeSelf(d *codec.Decoder) {
	var version string
	d.MustDecode(&version)
	if version != t.version {
		panic("unexpected version")
	}
	var amount int
	d.MustDecode(&amount)
	var k string
	var v map[uint64]int
	for i := 0; i < amount; i++ {
		d.MustDecode(&k)
		d.MustDecode(&v)
		t.tree.Insert(k, v)
	}
}

// string -> uint64
type RadixUint64 struct {
	version string
	tree    *radix.Tree
}

func NewRadixUint64() *RadixUint64 {
	return &RadixUint64{
		version: "1",
		tree:    radix.New(),
	}
}

func (t *RadixUint64) Len() int {
	return t.tree.Len()
}

func (t *RadixUint64) Get(k string) (uint64, bool) {
	v, ok := t.tree.Get(k)
	if !ok {
		return 0, false
	}
	return v.(uint64), true
}

func (t *RadixUint64) Set(k string, v uint64) {
	t.tree.Insert(k, v)
}

func (t *RadixUint64) Walk(f func(string, uint64) bool) {
	t.tree.Walk(func(k string, v interface{}) bool {
		return f(k, v.(uint64))
	})
}

func (t *RadixUint64) CodecEncodeSelf(e *codec.Encoder) {
	e.MustEncode(t.version)
	e.MustEncode(t.tree.Len())
	t.tree.Walk(func(k string, v interface{}) bool {
		e.MustEncode(&k)
		e.MustEncode(&v)
		return false
	})
}

func (t *RadixUint64) CodecDecodeSelf(d *codec.Decoder) {
	var version string
	d.MustDecode(&version)
	if version != t.version {
		panic("unexpected version")
	}

	var amount int
	d.MustDecode(&amount)
	var k string
	var v uint64
	for i := 0; i < amount; i++ {
		d.MustDecode(&k)
		d.MustDecode(&v)
		t.tree.Insert(k, v)
	}
}
