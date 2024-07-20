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

package solid

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/ssz"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type IterableSSZ[T any] interface {
	Clear()
	CopyTo(IterableSSZ[T])
	Range(fn func(index int, value T, length int) bool)
	Get(index int) T
	Set(index int, v T)
	Length() int
	Cap() int
	Bytes() []byte
	Pop() T
	Append(v T)

	ssz2.Sized
	ssz.EncodableSSZ
	ssz.HashableSSZ
}

type Uint64ListSSZ interface {
	IterableSSZ[uint64]
	json.Marshaler
	json.Unmarshaler
}

type Uint64VectorSSZ interface {
	IterableSSZ[uint64]
	json.Marshaler
	json.Unmarshaler
}

type HashListSSZ interface {
	IterableSSZ[common.Hash]
	json.Marshaler
	json.Unmarshaler
}

type HashVectorSSZ interface {
	IterableSSZ[common.Hash]
	json.Marshaler
	json.Unmarshaler
}
