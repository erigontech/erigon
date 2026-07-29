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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinFuzzSlots is the slot pool the generator draws from. Entropy is the enemy
// here: 32-byte slots picked at random essentially never share a stem, so a
// fuzzer free to choose them would only ever build shallow trees and would never
// reach sub-index sharing, group boundaries or the account/storage zone split.
var pbinFuzzSlots = []uint64{0, 1, 2, 63, 64, 65, 66, 127, 128, 255, 256, 257, 258, 511, 512, 1000, 1 << 20, 1<<20 + 1}

// pbinFuzzAccountBit is the selector bit choosing an account write over a slot.
const pbinFuzzAccountBit = 0x04

// pbinFuzzCorpus reads the input three bytes at a time — what to write, where,
// and with what value — drawing addresses and slots from small pools so keys
// collide by construction.
func pbinFuzzCorpus(data []byte) *pbinTestCorpus {
	c := new(pbinTestCorpus)
	for i := 0; i+2 < len(data); i += 3 {
		where, slot, value := data[i], data[i+1], data[i+2]
		addr := pbinOracleAddr(uint64(where & 0x03))
		if where&pbinFuzzAccountBit != 0 {
			c.account(addr, uint64(value), uint64(value)*1_000_000_007, common.Hash{value, 0xC0})
			continue
		}
		c.storage(addr, pbinOracleSlot(pbinFuzzSlots[int(slot)%len(pbinFuzzSlots)]), value, value^0xFF)
	}
	return c
}

// pbinFuzzBatches cuts the corpus in two, so a run also covers what one Process
// call leaves for the next to read back.
func pbinFuzzBatches(data []byte, cut byte) []*pbinTestCorpus {
	c := pbinFuzzCorpus(data)
	if len(c.plainKeys) == 0 {
		return nil
	}
	at := int(cut) % (len(c.plainKeys) + 1)
	batches := make([]*pbinTestCorpus, 0, 2)
	for _, b := range []*pbinTestCorpus{
		{plainKeys: c.plainKeys[:at], updates: c.updates[:at]},
		{plainKeys: c.plainKeys[at:], updates: c.updates[at:]},
	} {
		if len(b.plainKeys) > 0 {
			batches = append(batches, b)
		}
	}
	return batches
}

// FuzzPBinProcessMatchesOracle is the differential gate: whatever the generator
// produces, the engine's root must equal the reference tree's over the same
// leaves, and the records it left behind must rebuild that root on their own.
//
// go test ./execution/commitment/ -run=Fuzz -fuzz=FuzzPBinProcessMatchesOracle -fuzztime=60s
func FuzzPBinProcessMatchesOracle(f *testing.F) {
	// Seeds spell the generator's (selector, slot, value) triples: bit 2 of the
	// selector asks for an account, its low bits pick the address, and the slot
	// byte indexes the pool.
	f.Add([]byte{0x04, 0, 1, 0x05, 0, 2}, byte(0))                            // two accounts
	f.Add([]byte{0x00, 10, 1, 0x00, 11, 2, 0x00, 12, 3}, byte(2))             // three slots of one group
	f.Add([]byte{0x00, 3, 1, 0x00, 4, 2, 0x04, 0, 3}, byte(1))                // the 63/64 zone boundary plus a header
	f.Add([]byte{0x04, 0, 1, 0x00, 15, 2, 0x01, 15, 3, 0x02, 16, 4}, byte(3)) // one slot per address
	f.Add([]byte{0x00, 10, 1, 0x00, 10, 2, 0x00, 10, 3}, byte(1))             // the same slot rewritten

	f.Fuzz(func(t *testing.T, data []byte, cut byte) {
		batches := pbinFuzzBatches(data, cut)
		if len(batches) == 0 {
			return
		}

		pph, ms := pbinTestEngine(t)
		var root []byte
		for _, b := range batches {
			require.NoError(t, ms.applyPlainUpdates(b.plainKeys, b.updates))
			root = pbinTestProcess(t, pph, b.plainKeys, b.updates)
		}
		require.Len(t, root, length.Hash)

		union := pbinTestUnion(batches...)
		require.Equal(t, union.oracleRoot(t), root)

		// A tree of one leaf is that leaf and writes no record.
		if leaves := union.leafCount(t); leaves > 1 {
			pbinTestVerifyRecords(t, ms, root, leaves)
		}
	})
}
