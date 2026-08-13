// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2025 The Erigon Authors
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

package types

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/rlp"
)

func decodeEncode(input []byte, val any) error {
	if err := rlp.DecodeBytes(input, val); err != nil {
		// not valid rlp, nothing to do
		return nil
	}
	// If it _were_ valid rlp, we can encode it again
	output, err := rlp.EncodeToBytes(val)
	if err != nil {
		return err
	}
	if !bytes.Equal(input, output) {
		return fmt.Errorf("encode-decode is not equal, \ninput : %x\noutput: %x", input, output)
	}
	return nil
}

func FuzzRLP(f *testing.F) {
	// Without seeds the fuzzer has to invent valid RLP before it reaches any
	// decoder logic, and never gets past the length prefix. These cover the
	// encoding's branch structure: every size class either side of the
	// single-byte, 55-byte and long-form boundaries, for both strings and
	// lists, plus the shapes fuzzRlp decodes into.
	seed := func(v any) {
		if b, err := rlp.EncodeToBytes(v); err == nil {
			f.Add(b)
		}
	}
	for _, n := range []int{0, 1, 2, 32, 54, 55, 56, 57, 255, 256, 1024} {
		seed(make([]byte, n))        // string size classes
		seed([]any{make([]byte, n)}) // list holding one
		seed(strings.Repeat("x", n))
	}
	for _, u := range []uint64{0, 1, 127, 128, 255, 256, 65535, 1 << 32, ^uint64(0)} {
		seed(u) // integer minimal-encoding edges
	}
	seed([]any{})                                     // empty list
	seed([]any{[]any{}, []any{[]any{}}})              // nesting
	seed([]any{uint(1), "two", []byte{3}})            // the Int/String/Bytes shape
	seed([]any{true, []byte{0x01}, []any{}, []any{}}) // the Types shape
	// a legacy transaction body: nonce, gasPrice, gas, to, value, data, v, r, s
	seed([]any{uint64(9), uint64(20e9), uint64(21000), make([]byte, 20),
		uint64(1e18), []byte{}, uint64(37), make([]byte, 32), make([]byte, 32)})
	// deep nesting, to reach the recursion limits
	deep := any([]any{})
	for range 24 {
		deep = []any{deep}
	}
	seed(deep)

	f.Fuzz(fuzzRlp)
}

func fuzzRlp(t *testing.T, input []byte) {
	if len(input) == 0 || len(input) > 500*1024 {
		return
	}
	rlp.Split(input)
	if elems, _, err := rlp.SplitList(input); err == nil {
		rlp.CountValues(elems)
	}
	rlp.NewStream(bytes.NewReader(input), 0).Decode(new(any))
	if err := decodeEncode(input, new(any)); err != nil {
		t.Fatal(err)
	}
	{
		var v struct {
			Int    uint
			String string
			Bytes  []byte
		}
		if err := decodeEncode(input, &v); err != nil {
			t.Fatal(err)
		}
	}
	{
		type Types struct {
			Bool  bool
			Raw   rlp.RawValue
			Slice []*Types
			Iface []any
		}
		var v Types
		if err := decodeEncode(input, &v); err != nil {
			t.Fatal(err)
		}
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
			Iface  []any
		}
		var v AllTypes
		if err := decodeEncode(input, &v); err != nil {
			t.Fatal(err)
		}
	}
	{
		if err := decodeEncode(input, [10]byte{}); err != nil {
			t.Fatal(err)
		}
	}
	{
		var v struct {
			Byte [10]byte
			Rool [10]bool
		}
		if err := decodeEncode(input, &v); err != nil {
			t.Fatal(err)
		}
	}
	{
		var h Header
		if err := decodeEncode(input, &h); err != nil {
			t.Fatal(err)
		}
		var b Block
		if err := decodeEncode(input, &b); err != nil {
			t.Fatal(err)
		}
		var tx Transaction
		if err := decodeEncode(input, &tx); err != nil {
			t.Fatal(err)
		}
		var txs Transactions
		if err := decodeEncode(input, &txs); err != nil {
			t.Fatal(err)
		}
		var rs Receipts
		if err := decodeEncode(input, &rs); err != nil {
			t.Fatal(err)
		}
	}
	{
		var v struct {
			AnIntPtr  *big.Int
			AnInt     big.Int
			AnU256Ptr *uint256.Int
			AnU256    uint256.Int
			NotAnU256 [4]uint64
		}
		if err := decodeEncode(input, &v); err != nil {
			t.Fatal(err)
		}
	}
}
