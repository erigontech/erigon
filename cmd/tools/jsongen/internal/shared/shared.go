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

// Package shared carries one field per shape a shared group can take, so the golden file
// changes when any of them does.
package shared

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

type Position struct {
	Number hexutil.Uint64 `json:"number" ethjson:"quantity,shared"`
}

// Item has three groups: one that starts in an embedded struct, one whose first member is always
// written, and one whose members may all be left out.
type Item struct {
	Position
	Hash     common.Hash     `json:"hash" ethjson:"data,shared"`
	Empty    [0]byte         `json:"empty,omitempty" ethjson:"data,shared"`
	Index    uint64          `json:"index" ethjson:"quantity"`
	Flag     bool            `json:"flag" ethjson:"bool,shared"`
	Stamp    *hexutil.Uint64 `json:"stamp,omitempty" ethjson:"quantity,shared"`
	Null     *uint64         `json:"null" ethjson:"quantity,shared"`
	Payload  []byte          `json:"payload" ethjson:"data"`
	OptStart *uint64         `json:"optStart,omitempty" ethjson:"quantity,shared"`
	OptNum   hexutil.Uint    `json:"optNum,omitempty" ethjson:"quantity,shared"`
}

type Items []*Item
