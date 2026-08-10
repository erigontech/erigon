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

// pbinFuzzSlots is the slot pool the generator draws from. Slots picked at
// random essentially never share a stem, so a fuzzer free to choose all 32 bytes
// would only build shallow trees, never reaching sub-index sharing, group
// boundaries or the account/storage zone split.
var pbinFuzzSlots = []uint64{0, 1, 2, 63, 64, 65, 66, 127, 128, 255, 256, 257, 258, 511, 512, 1000, 1 << 20, 1<<20 + 1}

// pbinFuzzAccountBit asks for an account write, pbinFuzzDeleteBit for an
// account removal; the low three bits of the selector pick the address.
const (
	pbinFuzzAccountBit = 0x08
	pbinFuzzDeleteBit  = 0x10
)

// pbinFuzzCodeShapes is the code pool: a delegation indicator, codes ending in
// an all-zero chunk, and chunk counts straddling the 255/256 and 511/512 group
// boundaries. Address seeds fold onto shapes modulo four, so seeds four apart
// always share bytecode.
var pbinFuzzCodeShapes = [][]byte{
	nil,
	pbinTestCode(23),
	pbinTestIndicator(0x37),
	pbinTestCode(2 * pbinChunkDataLen),
	append(pbinTestCode(2*pbinChunkDataLen), make([]byte, pbinChunkDataLen)...),
	pbinTestCode(255 * pbinChunkDataLen),
	pbinTestCode(256 * pbinChunkDataLen),
	append(pbinTestCode(256*pbinChunkDataLen), make([]byte, pbinChunkDataLen)...),
	pbinTestCode(257 * pbinChunkDataLen),
	pbinTestCode(511 * pbinChunkDataLen),
	pbinTestCode(512 * pbinChunkDataLen),
	pbinTestCode(513 * pbinChunkDataLen),
}

// pbinFuzzCode keys the code on the address so it stays fixed for a whole run,
// which is what keeps the oracle valid: a redeploy to shorter code leaves its
// high chunks in the tree, and the oracle only knows the final state.
func pbinFuzzCode(addrSeed, salt byte) []byte {
	return pbinFuzzCodeShapes[(int(addrSeed%4)+int(salt))%len(pbinFuzzCodeShapes)]
}

// pbinFuzzCorpus reads the input three bytes at a time: what to write, where,
// and with what value. A zero value byte writes zero storage, which is the
// deletion encoding.
func pbinFuzzCorpus(data []byte, codeSalt byte) *pbinTestCorpus {
	c := new(pbinTestCorpus)
	for i := 0; i+2 < len(data); i += 3 {
		where, slot, value := data[i], data[i+1], data[i+2]
		addrSeed := where & 0x07
		addr := pbinOracleAddr(uint64(addrSeed))
		switch {
		case where&pbinFuzzDeleteBit != 0:
			c.remove(addr)
		case where&pbinFuzzAccountBit != 0:
			if code := pbinFuzzCode(addrSeed, codeSalt); code != nil {
				c.accountWithCodeBytes(addr, uint64(value), uint64(value)*1_000_000_007, code)
			} else {
				c.account(addr, uint64(value), uint64(value)*1_000_000_007, common.Hash{value, 0xC0})
			}
		case value == 0:
			c.storage(addr, pbinOracleSlot(pbinFuzzSlots[int(slot)%len(pbinFuzzSlots)]))
		default:
			c.storage(addr, pbinOracleSlot(pbinFuzzSlots[int(slot)%len(pbinFuzzSlots)]), value, value^0xFF)
		}
	}
	return c
}

// pbinFuzzBatches cuts the corpus in two, so a run also covers what one Process
// call leaves for the next to read back — and on which side of the cut a
// removal lands, which decides whether an account created and destroyed by the
// corpus ever materializes.
func pbinFuzzBatches(data []byte, cut, codeSalt byte) []*pbinTestCorpus {
	c := pbinFuzzCorpus(data, codeSalt)
	if len(c.plainKeys) == 0 {
		return nil
	}
	at := int(cut) % (len(c.plainKeys) + 1)
	batches := make([]*pbinTestCorpus, 0, 2)
	for _, b := range []*pbinTestCorpus{
		{plainKeys: c.plainKeys[:at], updates: c.updates[:at], codes: c.codes},
		{plainKeys: c.plainKeys[at:], updates: c.updates[at:], codes: c.codes},
	} {
		if len(b.plainKeys) > 0 {
			batches = append(batches, b)
		}
	}
	return batches
}

// TestPBinFuzzCorpusCoversNewShapes pins the generator's reach, so the fuzz
// seeds cannot go vacuous: delegation, shared bytecode, all-zero chunks, both
// group-boundary straddles, and account removal.
func TestPBinFuzzCorpusCoversNewShapes(t *testing.T) {
	t.Parallel()

	require.True(t, pbinIsDelegation(pbinFuzzCode(0, 2)))
	require.Equal(t, pbinFuzzCode(1, 2), pbinFuzzCode(5, 2), "address seeds four apart share a shape")
	require.NotEmpty(t, pbinFuzzCode(1, 2))

	counts := make(map[int]bool, len(pbinFuzzCodeShapes))
	zeroTails := 0
	for _, shape := range pbinFuzzCodeShapes {
		chunks := pbinChunkifyCode(shape)
		counts[len(chunks)] = true
		if len(chunks) > 0 && chunks[len(chunks)-1] == ([pbinValueLength]byte{}) {
			zeroTails++
		}
	}
	for _, straddle := range []int{255, 256, 257, 511, 512, 513} {
		require.True(t, counts[straddle], "no shape holds %d chunks", straddle)
	}
	require.NotZero(t, zeroTails, "no shape ends in an all-zero chunk")

	removal := pbinFuzzCorpus([]byte{pbinFuzzDeleteBit, 0, 0}, 0)
	require.Len(t, removal.updates, 1)
	require.True(t, removal.updates[0].Deleted())
}

// FuzzPBinProcessMatchesOracle: whatever the generator produces, the engine's
// root must equal the reference tree's over the same leaves, and the records it
// left behind must rebuild that root on their own.
//
// go test ./execution/commitment/ -run=Fuzz -fuzz=FuzzPBinProcessMatchesOracle -fuzztime=60s
func FuzzPBinProcessMatchesOracle(f *testing.F) {
	// Seeds are (selector, slot, value) triples: bit 3 of the selector asks for an
	// account, bit 4 for its removal, the low bits pick the address, and the slot
	// byte indexes the pool.
	f.Add([]byte{0x08, 0, 1, 0x09, 0, 2}, byte(0), byte(0))                            // two accounts, no code
	f.Add([]byte{0x00, 10, 1, 0x00, 11, 2, 0x00, 12, 3}, byte(2), byte(0))             // three slots of one group
	f.Add([]byte{0x00, 3, 1, 0x00, 4, 2, 0x08, 0, 3}, byte(1), byte(0))                // the 63/64 zone boundary plus a header
	f.Add([]byte{0x08, 0, 1, 0x00, 15, 2, 0x01, 15, 3, 0x02, 16, 4}, byte(3), byte(0)) // one slot per address
	f.Add([]byte{0x00, 10, 1, 0x00, 10, 2, 0x00, 10, 3}, byte(1), byte(0))             // the same slot rewritten
	f.Add([]byte{0x08, 0, 1, 0x00, 5, 2, 0x08, 0, 3}, byte(1), byte(1))                // code interleaved with a header slot
	f.Add([]byte{0x08, 0, 1, 0x09, 0, 2, 0x00, 17, 3}, byte(2), byte(4))               // a zero-tailed code beside a 255-chunk one
	f.Add([]byte{0x08, 0, 1, 0x18, 0, 0}, byte(1), byte(2))                            // a delegation inserted, then its account removed
	f.Add([]byte{0x08, 0, 1, 0x08, 0, 2, 0x0C, 0, 3}, byte(1), byte(2))                // a delegation rewritten, plus a second authority on the target
	f.Add([]byte{0x08, 0, 1}, byte(0), byte(7))                                        // a zero chunk alone in its group
	f.Add([]byte{0x09, 0, 1, 0x0D, 0, 2, 0x15, 0, 0}, byte(2), byte(2))                // shared code outliving one holder
	f.Add([]byte{0x09, 0, 1, 0x0D, 0, 2, 0x15, 0, 0}, byte(0), byte(2))                // shared code whose second holder dies in the writing batch
	f.Add([]byte{0x08, 0, 1, 0x09, 0, 2, 0x0A, 0, 3, 0x0B, 0, 4}, byte(0), byte(5))    // chunk counts straddling 255/256
	f.Add([]byte{0x08, 0, 1, 0x09, 0, 2, 0x0A, 0, 3}, byte(0), byte(9))                // chunk counts straddling 511/512
	f.Add([]byte{0x00, 10, 1, 0x00, 10, 0}, byte(1), byte(0))                          // a live slot zeroed by the next batch
	f.Add([]byte{0x08, 0, 5, 0x08, 0, 0}, byte(1), byte(0))                            // basic data zeroed while the code-hash leaf stays

	f.Fuzz(func(t *testing.T, data []byte, cut, codeSalt byte) {
		batches := pbinFuzzBatches(data, cut, codeSalt)
		if len(batches) == 0 {
			return
		}

		pph, ms := pbinTestEngine(t)
		var root []byte
		for _, b := range batches {
			b.applyTo(t, ms)
			root = pbinTestProcess(t, pph, b.plainKeys, b.updates)
		}
		require.Len(t, root, length.Hash)

		final := pbinTestFinalEntries(t, batches...)
		want := pbinOracleRoot(final)
		require.Equal(t, want[:], root)

		// A tree of one leaf is that leaf and writes no record.
		if len(final) > 1 {
			pbinTestVerifyRecords(t, ms, root, len(final))
		}
	})
}
