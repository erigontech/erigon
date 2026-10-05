// Copyright 2026 The Erigon Authors
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

package artifact

import "github.com/erigontech/erigon/common"

type KVIterator func(func(key, value []byte) error) error

type Header struct {
	AddressHash common.Hash
	Nonce       []byte
	Balance     []byte
	Kind        byte
	CodeHash    common.Hash
	CodeSize    []byte
	Target      common.Address
	Slots       []GroupEntry
}

type GroupEntry struct {
	Index byte
	Value []byte
}

type Group struct {
	StemHash common.Hash
	Entries  []GroupEntry
}
