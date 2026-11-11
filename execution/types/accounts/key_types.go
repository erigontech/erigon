// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package accounts

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync/atomic"
	"unique"
	"weak"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

type Address struct{ p *common.Address }

var (
	ZeroAddress = InternAddress(common.Address{})
	NilAddress  = Address{}
)

// An interned value is dropped at the first GC after its last handle dies, so values the next request reads again are
// allocated and registered again, and unique.Make parks its caller while a GC cycle finishes marking. These
// direct-mapped tables keep the latest values alive and answer hits without the canonical index; a miss overwrites the slot.
const (
	addressTableSize = 256 * 1024
	keyTableSize     = 1024 * 1024
)

var (
	addressTable [addressTableSize]atomic.Value // Address
	keyTable     [keyTableSize]atomic.Value     // StorageKey
	_            = [1]struct{}{}[addressTableSize&(addressTableSize-1)]
	_            = [1]struct{}{}[keyTableSize&(keyTableSize-1)]
)

func addressSlot(a common.Address) *atomic.Value {
	return &addressTable[binary.BigEndian.Uint32(a[16:])&(addressTableSize-1)]
}

func InternAddress(a common.Address) Address {
	slot := addressSlot(a)
	if h, ok := slot.Load().(Address); ok && h.Value() == a {
		return h
	}
	h := Address{addressInterner.intern(a)}
	slot.Store(h)
	return h
}

func (a Address) IsNil() bool {
	return a == NilAddress
}

func (a Address) IsZero() bool {
	return a == NilAddress || a == ZeroAddress
}

func (a Address) Value() common.Address {
	if a == NilAddress {
		return common.Address{}
	}
	return *a.p
}

func (a Address) String() string {
	if a == NilAddress {
		return "<nil>"
	}
	return a.Value().String()
}

func (a Address) Format(s fmt.State, c rune) {
	if a == NilAddress {
		_, _ = s.Write([]byte("<nil>"))
		return
	}
	a.Value().Format(s, c)
}

// MarshalText returns the hex representation of a.
func (a Address) MarshalText() ([]byte, error) {
	if a.IsNil() {
		return nil, nil
	}
	return a.Value().MarshalText()
}

// UnmarshalText parses a hash in hex syntax.
func (a *Address) UnmarshalText(input []byte) error {
	var value common.Address
	if err := value.UnmarshalText(input); err != nil {
		return err
	}
	*a = InternAddress(value)
	return nil
}

// UnmarshalJSON parses a hash in hex syntax.
func (a *Address) UnmarshalJSON(input []byte) error {
	var value common.Address
	if err := value.UnmarshalJSON(input); err != nil {
		return err
	}
	*a = InternAddress(value)
	return nil
}

func (a Address) Cmp(o Address) int {
	switch {
	case a == NilAddress:
		switch {
		case o == NilAddress:
			return 0
		default:
			return -1
		}
	case o == NilAddress:
		return +1
	}

	return a.Value().Cmp(o.Value())
}

// slabLen makes an address slab exactly the 1280-byte size class.
const slabLen = 64

type slab[T comparable] [slabLen]T

type slabRef[T comparable] struct {
	slab weak.Pointer[slab[T]]
	idx  uint32
}

// slabInterner gives each value one canonical pointer while any handle to it lives, like unique.Make. unique.Make
// registers a weak pointer and a cleanup per value, and each registration walks the span's sorted list of specials:
// quadratic over a span of small values. A slab pays one registration per slabLen values and lives while any of its
// values has a handle.
type slabInterner[T comparable] struct {
	index *xsync.Map[T, slabRef[T]]
	cur   atomic.Pointer[openSlab[T]]
}

// openSlab is the slab new values go to; next hands out its slots without a lock.
type openSlab[T comparable] struct {
	slab   *slab[T]
	ref    weak.Pointer[slab[T]]
	values *slabValues[T]
	next   atomic.Uint32
}

// slabValues copies a slab's values, because the cleanup that removes them from the index cannot reach a dead slab.
// n counts the written values and publishes them to the cleanup goroutine.
type slabValues[T comparable] struct {
	n      atomic.Uint32
	values [slabLen]T
}

type deadSlab[T comparable] struct {
	index  *xsync.Map[T, slabRef[T]]
	slab   weak.Pointer[slab[T]]
	values *slabValues[T]
}

// The index holds at least the handles addressTable keeps alive. A resize blocks every writer, and a shrink after
// slabs die would resize again on the next burst of new addresses.
var addressInterner = newSlabInterner[common.Address](addressTableSize)

func newSlabInterner[T comparable](size int) *slabInterner[T] {
	t := &slabInterner[T]{index: xsync.NewMap[T, slabRef[T]](xsync.WithPresize(size), xsync.WithGrowOnly())}
	t.cur.Store(newOpenSlab(t.index))
	return t
}

func newOpenSlab[T comparable](index *xsync.Map[T, slabRef[T]]) *openSlab[T] {
	s := &openSlab[T]{slab: new(slab[T]), values: new(slabValues[T])}
	s.ref = weak.Make(s.slab)
	runtime.AddCleanup(s.slab, forgetSlab[T], deadSlab[T]{index, s.ref, s.values})
	return s
}

func (t *slabInterner[T]) intern(v T) *T {
	if ref, ok := t.index.Load(v); ok {
		if s := ref.slab.Value(); s != nil {
			return &s[ref.idx]
		}
	}
	var p *T
	t.index.Compute(v, func(old slabRef[T], loaded bool) (slabRef[T], xsync.ComputeOp) {
		if loaded {
			if s := old.slab.Value(); s != nil {
				p = &s[old.idx]
				return old, xsync.CancelOp
			}
		}
		var ref slabRef[T]
		ref, p = t.claim(v)
		return ref, xsync.UpdateOp
	})
	return p
}

// claim takes a slot without a lock: the caller that overflows a slab opens the next one, later callers retry.
func (t *slabInterner[T]) claim(v T) (slabRef[T], *T) {
	for {
		s := t.cur.Load()
		i := s.next.Add(1) - 1
		if i < slabLen {
			s.slab[i], s.values.values[i] = v, v
			s.values.n.Add(1)
			return slabRef[T]{s.ref, i}, &s.slab[i]
		}
		if i == slabLen {
			t.cur.Store(newOpenSlab(t.index))
		} else {
			runtime.Gosched()
		}
	}
}

// forgetSlab keeps an entry a newer slab has taken over.
func forgetSlab[T comparable](d deadSlab[T]) {
	for _, v := range d.values.values[:d.values.n.Load()] {
		d.index.Compute(v, func(old slabRef[T], loaded bool) (slabRef[T], xsync.ComputeOp) {
			if loaded && old.slab == d.slab {
				return old, xsync.DeleteOp
			}
			return old, xsync.CancelOp
		})
	}
}

type StorageKey unique.Handle[common.Hash]

var (
	ZeroKey = InternKey(common.Hash{})
	NilKey  = StorageKey{}
)

// keySlot mixes both ends of the key: keccak-derived slots differ everywhere, small scalar slots only at the end.
func keySlot(k common.Hash) *atomic.Value {
	return &keyTable[(binary.BigEndian.Uint32(k[:4])^binary.BigEndian.Uint32(k[28:]))&(keyTableSize-1)]
}

func InternKey(k common.Hash) StorageKey {
	slot := keySlot(k)
	if h, ok := slot.Load().(StorageKey); ok && h.Value() == k {
		return h
	}
	h := StorageKey(unique.Make(k))
	slot.Store(h)
	return h
}

func (k StorageKey) IsNil() bool {
	return k == NilKey
}

func (k StorageKey) Value() common.Hash {
	if k == NilKey {
		return common.Hash{}
	}
	return unique.Handle[common.Hash](k).Value()
}

func (k StorageKey) String() string {
	if k == NilKey {
		return "<nil>"
	}
	return k.Value().String()
}

func (k StorageKey) Format(s fmt.State, c rune) {
	if k == NilKey {
		_, _ = s.Write([]byte("<nil>"))
		return
	}
	k.Value().Format(s, c)
}

func (k StorageKey) Cmp(o StorageKey) int {
	switch {
	case k == NilKey:
		switch {
		case o == NilKey:
			return 0
		default:
			return -1
		}
	case o == NilKey:
		return +1
	}

	return k.Value().Cmp(o.Value())
}

type CodeHash unique.Handle[common.Hash]

var (
	ZeroCodeHash  = InternCodeHash(common.Hash{})
	NilCodeHash   = CodeHash{}
	EmptyCodeHash = InternCodeHash(empty.CodeHash)
)

func InternCodeHash(k common.Hash) CodeHash {
	return CodeHash(unique.Make(k))
}

func (h CodeHash) IsNil() bool {
	return h == NilCodeHash
}

func (h CodeHash) IsEmpty() bool {
	return h == EmptyCodeHash || h == ZeroCodeHash || h == NilCodeHash
}

func (h CodeHash) IsZero() bool {
	return h == NilCodeHash || h == ZeroCodeHash
}

func (h CodeHash) Value() common.Hash {
	if h == NilCodeHash {
		return common.Hash{}
	}
	return unique.Handle[common.Hash](h).Value()
}

func (h CodeHash) String() string {
	if h == NilCodeHash {
		return "<nil>"
	}
	return h.Value().String()
}

func (h CodeHash) Format(s fmt.State, c rune) {
	if h == NilCodeHash {
		_, _ = s.Write([]byte("<nil>"))
		return
	}
	h.Value().Format(s, c)
}

func (h CodeHash) Cmp(o CodeHash) int {
	switch {
	case h == NilCodeHash:
		switch {
		case o == NilCodeHash:
			return 0
		default:
			return -1
		}
	case o == NilCodeHash:
		return +1
	}

	return h.Value().Cmp(o.Value())
}
