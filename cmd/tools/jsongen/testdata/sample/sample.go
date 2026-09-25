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

// Package sample carries one field per branch the generator has, so the golden file changes
// when any of them does.
package sample

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// Left has no encoder, so this assertion never holds. It is here on purpose: an unsatisfied
// interface is the other wording the compiler uses for the method the generator writes, and a
// first run has to read past it.
var _ jsonstream.Marshaler = (*Left)(nil)

type Inner struct {
	Address common.Address `json:"address" ethjson:"data"`
	Count   uint64         `json:"count" ethjson:"quantity"`
}

// Left and Right both promote Value, under different JSON names, so encoding/json writes two
// fields and each generated access has to name the struct it came from.
type Left struct {
	Value uint64 `json:"leftValue" ethjson:"quantity"`
}

type Right struct {
	Value uint64 `json:"rightValue" ethjson:"quantity"`
}

type Sample struct {
	Inner
	Left
	Right                           // flattened
	Hash       common.Hash          `json:"hash" ethjson:"data"`
	Bytes      []byte               `json:"bytes" ethjson:"data"`
	OptBytes   hexutil.Bytes        `json:"optBytes,omitempty" ethjson:"data"`
	PtrBytes   *hexutil.Bytes       `json:"ptrBytes" ethjson:"data"`
	PtrHash    *common.Hash         `json:"ptrHash" ethjson:"data"`
	Topics     []common.Hash        `json:"topics" ethjson:"datalist"`
	Num        uint64               `json:"num" ethjson:"quantity"`
	OptNum     hexutil.Uint         `json:"optNum,omitempty" ethjson:"quantity"`
	PtrNum     *uint64              `json:"ptrNum" ethjson:"quantity"`
	OptPtrNum  *hexutil.Uint64      `json:"optPtrNum,omitempty" ethjson:"quantity"`
	Big        uint256.Int          `json:"big" ethjson:"quantity"`
	OptBig     hexutil.U256         `json:"optBig,omitempty" ethjson:"quantity"`
	PtrBig     *uint256.Int         `json:"ptrBig" ethjson:"quantity"`
	Flag       bool                 `json:"flag" ethjson:"bool"`
	OptFlag    bool                 `json:"optFlag,omitempty" ethjson:"bool"`
	Logs       jsonstream.Marshaler `json:"logs" ethjson:"objects"`
	OptLogs    jsonstream.Marshaler `json:"optLogs,omitempty" ethjson:"objects"`
	Nested     *Inner               `json:"nested" ethjson:"objects"`
	OptNested  *Inner               `json:"optNested,omitempty" ethjson:"objects"`
	Inners     Inners               `json:"inners" ethjson:"objects"`
	OptInners  Inners               `json:"optInners,omitempty" ethjson:"objects"`
	Renamed    uint64               `json:",omitempty" ethjson:"quantity"`
	Skipped    string               `json:"-"`
	unexported int                  //nolint:unused
}

// Inners is a named slice, whose emptiness len reports, so it may be omitempty.
type Inners []Inner

func (in Inners) MarshalFastJSONTo(s *jsonstream.StackStream) error { return nil }

// Inner writes itself, which is what an objects field needs of its type.
func (x *Inner) MarshalFastJSONTo(s *jsonstream.StackStream) error { return nil }

// writeComputedJSON stands for a value the struct does not hold, such as a header's hash.
func (x *Sample) writeComputedJSON(s *jsonstream.StackStream) {}
