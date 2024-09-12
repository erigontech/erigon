// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rlp

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
)

func decodeEncode(input []byte, val interface{}, i int) {
	if err := rlp.DecodeBytes(input, val); err == nil {
		output, err := rlp.EncodeToBytes(val)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(input, output) {
			panic(fmt.Sprintf("case %d: encode-decode is not equal, \ninput : %x\noutput: %x", i, input, output))
		}
	}
}

func Fuzz(input []byte) int {
	if len(input) == 0 {
		return 0
	}

	var i int
	{
		if _, _, _, err := rlp.Split(input); err != nil {
			panic(err)
		}
	}

	{
		if elems, _, err := rlp.SplitList(input); err == nil {
			if _, err = rlp.CountValues(elems); err != nil {
				panic(err)
			}
		}
	}

	{
		if err := rlp.NewStream(bytes.NewReader(input), 0).Decode(new(interface{})); err != nil {
			panic(err)
		}
	}

	{
		decodeEncode(input, new(interface{}), i)
		i++
	}
	{
		var v struct {
			Int    uint
			String string
			Bytes  []byte
		}
		decodeEncode(input, &v, i)
		i++
	}

	{
		type Types struct {
			Bool  bool
			Raw   rlp.RawValue
			Slice []*Types
			Iface []interface{}
		}
		var v Types
		decodeEncode(input, &v, i)
		i++
	}
	{
		type AllTypes struct {
			Int    uint
			String string
			Bytes  []byte
			Bool   bool
			Raw    rlp.RawValue
			Slice  []*AllTypes
			Array  [3]*AllTypes
			Iface  []interface{}
		}
		var v AllTypes
		decodeEncode(input, &v, i)
		i++
	}
	{
		decodeEncode(input, [10]byte{}, i)
		i++
	}
	{
		var v struct {
			Byte [10]byte
			Rool [10]bool
		}
		decodeEncode(input, &v, i)
		i++
	}
	{
		var h types.Header
		decodeEncode(input, &h, i)
		i++
		var b types.Block
		decodeEncode(input, &b, i)
		i++
		var t types.Transaction
		decodeEncode(input, &t, i)
		i++
		var txs types.Transactions
		decodeEncode(input, &txs, i)
		i++
		var rs types.Receipts
		decodeEncode(input, &rs, i)
	}
	return 1
}
