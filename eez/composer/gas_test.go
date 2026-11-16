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

package composer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFloorGas: the EIP-7623 floor, 21,000 plus 10 per token, a zero byte
// one token and any other four.
func TestFloorGas(t *testing.T) {
	require.Equal(t, uint64(21_000), FloorGas(nil))
	require.Equal(t, uint64(21_110), FloorGas([]byte{0, 0xab, 0, 0xcd, 0}))
	require.Greater(t, FloorGas(bytes.Repeat([]byte{0xff}, 200*1024)), uint64(4_000_000))
}

// TestPostBatchGasCeiling: a ceiling between the gas of a batch of two
// entries plus the drain margin and the EIP-7825 cap is taken; any other
// falls back to the cap.
func TestPostBatchGasCeiling(t *testing.T) {
	require.Equal(t, uint64(1_521_000), uint64(MinPostBatchGas))
	for requested, want := range map[uint64]uint64{
		12_000_000:          12_000_000,
		MinPostBatchGas:     MinPostBatchGas,
		MaxPostBatchGas:     MaxPostBatchGas,
		MinPostBatchGas - 1: MaxPostBatchGas,
		1:                   MaxPostBatchGas,
		MaxPostBatchGas + 1: MaxPostBatchGas,
	} {
		got, ok := PostBatchGasCeiling(requested)
		require.Equal(t, want, got, "requested %d", requested)
		require.Equal(t, want == requested, ok, "requested %d", requested)
	}
}

// TestPostBatchGas: a recorded batch is projected over a proof no shorter
// and no cheaper than the one it carried, at the pinned execution gas above
// its calldata, and at its floor once that is higher.
func TestPostBatchGas(t *testing.T) {
	for name, want := range map[string]uint64{
		// 21,000 + 160,000 + 370,000 + the calldata at 4 and 16 a byte.
		"captured-devnet-window-84":  561_188,
		"captured-devnet-anchor-402": 577_756,
	} {
		_, mined, input := window(t, name)
		got, err := PostBatchGas(mined)
		require.NoError(t, err)
		require.Equal(t, want, got, name)
		require.Greater(t, got, 21_000+postBatchBaseGas+postBatchEntryGas+calldataGas(input), name)
		require.Greater(t, got, FloorGas(input), name)
		require.Len(t, mined.Proofs, 1, "PostBatchGas leaves the batch's proofs")
	}
	// 30 KB of non-zero DA: the floor, 1,245,840, outgrows the standard
	// price, 1,040,936.
	_, mined, _ := window(t, "captured-devnet-window-84")
	fat := *mined
	fat.CallData = bytes.Repeat([]byte{0xff}, 30_000)
	got, err := PostBatchGas(&fat)
	require.NoError(t, err)
	require.Equal(t, uint64(1_245_840), got)
	// The placeholder outprices a 65-byte ECDSA proof under either rate.
	require.GreaterOrEqual(t, len(proofPlaceholder), 65)
	require.GreaterOrEqual(t, calldataGas(proofPlaceholder), calldataGas(bytes.Repeat([]byte{0x11}, 65)))
	require.GreaterOrEqual(t, FloorGas(proofPlaceholder), FloorGas(bytes.Repeat([]byte{0x11}, 65)))
}
