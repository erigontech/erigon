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

type Address unique.Handle[common.Address]

var (
	ZeroAddress = InternAddress(common.Address{})
	NilAddress  = Address{}
)

// unique drops a value at the first GC after its last handle dies, so values the next request reads again are
// allocated and registered again, and unique.Make parks its caller while a GC cycle finishes marking. These
// direct-mapped tables keep the latest values alive and answer hits without unique.Make; a miss overwrites the slot.
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
	h := Address(unique.Make(a))
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
	return unique.Handle[common.Address](a).Value()
}

func (a Address) Handle() unique.Handle[common.Address] {
	return unique.Handle[common.Address](a)
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

type StorageKey struct{ p *common.Hash }

var (
	ZeroKey = InternKey(common.Hash{})
	NilKey  = StorageKey{}
)

// keySlot mixes both ends of the key: keccak-derived slots differ everywhere, small scalar slots only at the end.
func keySlot(k common.Hash) *atomic.Value {
	return &keyTable[(binary.BigEndian.Uint32(k[:4])^binary.BigEndian.Uint32(k[28:]))&(keyTableSize-1)]
}

// keyCanon replaces unique.Make for storage keys: unique.Make runs weak.Make and runtime.AddCleanup under its map
// lock, so workers that read the same fresh slots at once queue on it. Here both run outside the map lock.
var keyCanon = xsync.NewMap[common.Hash, weak.Pointer[common.Hash]]()

func InternKey(k common.Hash) StorageKey {
	slot := keySlot(k)
	if h, ok := slot.Load().(StorageKey); ok && h.Value() == k {
		return h
	}
	h := internKeyCanon(k)
	slot.Store(h)
	return h
}

func internKeyCanon(k common.Hash) StorageKey {
	if wp, ok := keyCanon.Load(k); ok {
		if p := wp.Value(); p != nil {
			return StorageKey{p}
		}
	}
	p := new(k)
	wp := weak.Make(p)
	var live *common.Hash
	keyCanon.Compute(k, func(old weak.Pointer[common.Hash], loaded bool) (weak.Pointer[common.Hash], xsync.ComputeOp) {
		if loaded {
			// A nil Value means no handle of old is reachable, so replacing it keeps handles unique.
			if live = old.Value(); live != nil {
				return old, xsync.CancelOp
			}
		}
		return wp, xsync.UpdateOp
	})
	if live != nil {
		return StorageKey{live}
	}
	runtime.AddCleanup(p, func(k common.Hash) { releaseKey(k, wp) }, k)
	return StorageKey{p}
}

// releaseKey deletes k only while it still maps to wp: a concurrent intern may have replaced the dead entry.
func releaseKey(k common.Hash, wp weak.Pointer[common.Hash]) {
	keyCanon.Compute(k, func(old weak.Pointer[common.Hash], loaded bool) (weak.Pointer[common.Hash], xsync.ComputeOp) {
		if loaded && old == wp {
			return old, xsync.DeleteOp
		}
		return old, xsync.CancelOp
	})
}

func (k StorageKey) IsNil() bool {
	return k == NilKey
}

func (k StorageKey) Value() common.Hash {
	if k == NilKey {
		return common.Hash{}
	}
	return *k.p
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
