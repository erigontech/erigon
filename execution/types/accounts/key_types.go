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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

type Address common.Address

var ZeroAddress = InternAddress(common.Address{})
var NilAddress = Address{}

func InternAddress(a common.Address) Address {
	return Address(a)
}

func (a Address) IsNil() bool {
	return false
}

func (a Address) IsZero() bool {
	return common.Address(a) == (common.Address{})
}

func (a Address) Value() common.Address {
	return common.Address(a)
}

func (a Address) Handle() common.Address {
	return a.Value()
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
	return a.Value().Cmp(o.Value())
}

type StorageKey common.Hash

var ZeroKey = InternKey(common.Hash{})
var NilKey = StorageKey{}

func InternKey(k common.Hash) StorageKey {
	return StorageKey(k)
}

func (k StorageKey) IsNil() bool {
	return false
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

type CodeHash common.Hash

var ZeroCodeHash = InternCodeHash(common.Hash{})
var NilCodeHash = CodeHash{}
var EmptyCodeHash = InternCodeHash(empty.CodeHash)

func InternCodeHash(k common.Hash) CodeHash {
	return CodeHash(k)
}

func (h CodeHash) IsNil() bool {
	return false
}

func (h CodeHash) IsEmpty() bool {
	value := common.Hash(h)
	return value == empty.CodeHash || value == (common.Hash{})
}

func (h CodeHash) IsZero() bool {
	return common.Hash(h) == (common.Hash{})
}

func (h CodeHash) Value() common.Hash {
	return common.Hash(h)
}

func (h CodeHash) String() string {
	return h.Value().String()
}

func (h CodeHash) Format(s fmt.State, c rune) {
	h.Value().Format(s, c)
}

func (h CodeHash) Cmp(o CodeHash) int {
	return h.Value().Cmp(o.Value())
}
