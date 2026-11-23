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
	"fmt"
	"unique"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

// Address and StorageKey are plain values: the nil and the zero value are the same, so code that
// must tell "unset" from 0x0 carries its own flag.
type Address common.Address

var (
	ZeroAddress = Address{}
	NilAddress  = Address{}
)

func InternAddress(a common.Address) Address {
	return Address(a)
}

func (a Address) IsNil() bool {
	return a == NilAddress
}

func (a Address) IsZero() bool {
	return a == ZeroAddress
}

func (a Address) Value() common.Address {
	return common.Address(a)
}

func (a Address) Handle() common.Address {
	return common.Address(a)
}

func (a Address) String() string {
	return a.Value().String()
}

func (a Address) Format(s fmt.State, c rune) {
	a.Value().Format(s, c)
}

// MarshalText returns the hex representation of a.
func (a Address) MarshalText() ([]byte, error) {
	return a.Value().MarshalText()
}

// UnmarshalText parses a hash in hex syntax.
func (a *Address) UnmarshalText(input []byte) error {
	return (*common.Address)(a).UnmarshalText(input)
}

// UnmarshalJSON parses a hash in hex syntax.
func (a *Address) UnmarshalJSON(input []byte) error {
	return (*common.Address)(a).UnmarshalJSON(input)
}

func (a Address) Cmp(o Address) int {
	return a.Value().Cmp(o.Value())
}

type StorageKey common.Hash

var (
	ZeroKey = StorageKey{}
	NilKey  = StorageKey{}
)

func InternKey(k common.Hash) StorageKey {
	return StorageKey(k)
}

func (k StorageKey) IsNil() bool {
	return k == NilKey
}

func (k StorageKey) Value() common.Hash {
	return common.Hash(k)
}

func (k StorageKey) String() string {
	return k.Value().String()
}

func (k StorageKey) Format(s fmt.State, c rune) {
	k.Value().Format(s, c)
}

func (k StorageKey) Cmp(o StorageKey) int {
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
