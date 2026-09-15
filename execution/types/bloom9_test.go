// Copyright 2014 The go-ethereum Authors
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

package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func TestBloom(t *testing.T) {
	t.Parallel()
	positive := []string{
		"testtest",
		"test",
		"hallo",
		"other",
	}
	negative := []string{
		"tes",
		"lo",
	}

	var bloom Bloom
	for _, data := range positive {
		bloom.Add([]byte(data))
	}

	for _, data := range positive {
		if !bloom.Test([]byte(data)) {
			t.Error("expected", data, "to test true")
		}
	}
	for _, data := range negative {
		if bloom.Test([]byte(data)) {
			t.Error("did not expect", data, "to test true")
		}
	}
}

// TestBloomExtensively does some more thorough tests
func TestBloomExtensively(t *testing.T) {
	t.Parallel()
	var exp = common.HexToHash("c8d3ca65cdb4874300a9e39475508f23ed6da09fdbc487f89a2dcf50b09eb263")
	var b Bloom
	// Add 100 "random" things
	for i := range 100 {
		data := fmt.Sprintf("xxxxxxxxxx data %d yyyyyyyyyyyyyy", i)
		b.Add([]byte(data))
		//b.Add(new(big.Int).SetBytes([]byte(data)))
	}
	got := crypto.Keccak256Hash(b.Bytes())
	if got != exp {
		t.Errorf("Got %x, exp %x", got, exp)
	}
	var b2 Bloom
	b2.SetBytes(b.Bytes())
	got2 := crypto.Keccak256Hash(b2.Bytes())
	if got != got2 {
		t.Errorf("Got %x, exp %x", got, got2)
	}
}

func TestBloomOr(t *testing.T) {
	t.Parallel()

	var left Bloom
	left.Add([]byte("left"))
	var right Bloom
	right.Add([]byte("right"))

	merged := left
	merged.Or(&right)

	if !merged.Test([]byte("left")) {
		t.Fatal("expected merged bloom to contain left input")
	}
	if !merged.Test([]byte("right")) {
		t.Fatal("expected merged bloom to contain right input")
	}
	if left.Test([]byte("right")) {
		t.Fatal("Or should not mutate the source bloom")
	}

	r1 := &Receipt{Logs: []*Log{{
		Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Topics:  []common.Hash{common.HexToHash("0x01")},
	}}}
	r2 := &Receipt{Logs: []*Log{{
		Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Topics:  []common.Hash{common.HexToHash("0x02"), common.HexToHash("0x03")},
	}}}
	combined := CreateBloom(Receipts{r1, r2})
	acc := CreateBloom(Receipts{r1})
	r2Bloom := CreateBloom(Receipts{r2})
	acc.Or(&r2Bloom)
	if acc != combined {
		t.Fatal("expected OR of receipt blooms to match CreateBloom over all receipts")
	}
}

func TestReceiptsMergedBloom(t *testing.T) {
	t.Parallel()
	receipts := Receipts{
		{Logs: Logs{
			{Address: common.HexToAddress("0x1111111111111111111111111111111111111111"), Topics: []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}},
			{Address: common.HexToAddress("0x2222222222222222222222222222222222222222"), Topics: []common.Hash{common.HexToHash("0x03")}},
		}},
		{Logs: Logs{
			{Address: common.HexToAddress("0x3333333333333333333333333333333333333333"), Topics: []common.Hash{common.HexToHash("0x04")}},
		}},
		{},
	}
	for _, r := range receipts {
		r.Bloom = CreateBloom(Receipts{r})
	}

	merged := receipts.MergedBloom()
	if merged.IsEmpty() {
		t.Fatal("expected non-empty merged bloom")
	}
	if want := CreateBloom(receipts); merged != want {
		t.Fatalf("merged bloom mismatch: got %x, want %x", merged, want)
	}
}

func TestIsEmpty(t *testing.T) {
	t.Parallel()
	var b Bloom
	if !b.IsEmpty() {
		t.Error("expected empty")
	}

	b[0] = 1
	if b.IsEmpty() {
		t.Error("expected not empty")
	}

	b = Bloom{}
	b[len(b)-1] = 1
	if b.IsEmpty() {
		t.Error("expected not empty")
	}
}

// AppendText must be byte-identical to MarshalText (only the destination differs).
func TestBloomAppendTextByteIdentical(t *testing.T) {
	for name, b := range map[string]Bloom{
		"zero": {},
		"set":  BytesToBloom(crypto.Keccak256(nil)),
	} {
		t.Run(name, func(t *testing.T) {
			mt, err := b.MarshalText()
			require.NoError(t, err)
			const pfx = "PFX"
			at, err := b.AppendText([]byte(pfx))
			require.NoError(t, err)
			require.Equal(t, append([]byte(pfx), mt...), at)
		})
	}
}
