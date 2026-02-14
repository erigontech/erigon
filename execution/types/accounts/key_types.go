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

type Address unique.Handle[common.Address]

var ZeroAddress = InternAddress(common.Address{})
var NilAddress = Address{}

func InternAddress(a common.Address) Address {
	return Address(unique.Make(a))
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
		s.Write([]byte("<nil>"))
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

type StorageKey unique.Handle[common.Hash]

var ZeroKey = InternKey(common.Hash{})
var NilKey = StorageKey{}

func InternKey(k common.Hash) StorageKey {
	return StorageKey(unique.Make(k))
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
		s.Write([]byte("<nil>"))
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

var ZeroCodeHash = InternCodeHash(common.Hash{})
var NilCodeHash = CodeHash{}
var EmptyCodeHash = InternCodeHash(empty.CodeHash)

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
		s.Write([]byte("<nil>"))
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
