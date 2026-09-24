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

// Package bad holds one struct per way a declaration can leave the generator no choice.
package bad

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

type MissingForm struct {
	Hash common.Hash `json:"hash"`
}

type WrongForm struct {
	Bytes []byte `json:"bytes" ethjson:"quantity"`
}

type Named struct {
	Hash common.Hash `json:"hash" ethjson:"data"`
}

type DuplicateName struct {
	Named
	Hash common.Hash `json:"hash" ethjson:"data"`
}

type TaggedEmbedded struct {
	Named `json:"named"`
}

type PointerEmbedded struct {
	*Named
}

type PointerToSlice struct {
	Bytes *[]byte `json:"bytes" ethjson:"data"`
}

type NarrowQuantity struct {
	Count uint32 `json:"count" ethjson:"quantity"`
}

type NotHashSlice struct {
	Chunks [][]byte `json:"chunks" ethjson:"datalist"`
}

type UnknownOption struct {
	Count uint64 `json:"count,omitzero" ethjson:"quantity"`
}

type OmitemptyObjects struct {
	Logs jsonstream.Marshaler `json:"logs,omitempty" ethjson:"objects"`
}

// WrongResult writes itself, but not with the error the generated call checks.
type WrongResult struct {
	Logs intMarshaller `json:"logs" ethjson:"objects"`
}

type intMarshaller struct{}

func (intMarshaller) MarshalFastJSONTo(*jsonstream.StackStream) int { return 0 }
