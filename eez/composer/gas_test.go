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
	"crypto/ecdsa"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
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

// TestSyncIntervalWithinCeiling: a Sync interval whose blocks the sequencer
// filled to its DA budget for a ceiling posts within that ceiling, so a
// backlog always has a chunk to post.
func TestSyncIntervalWithinCeiling(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	b := &Batcher{RollupID: 1, ProofSystem: common.Address{0xab}, Signer: testSigner}
	for _, k := range []uint64{2, 6, sequencer.MaxCatchup} {
		for _, ceiling := range []uint64{MinPostBatchGas, 4_000_000, MaxPostBatchGas} {
			blocks := fullInterval(t, key, k, sequencer.DABudget(k, ceiling))
			batch, err := b.AnchorBatch(blocks[0].ParentHash(), blocks)
			require.NoError(t, err)
			gas, err := PostBatchGas(batch)
			require.NoError(t, err)
			require.LessOrEqual(t, gas, ceiling, "K %d, ceiling %d", k, ceiling)
		}
	}
}

// fullInterval returns the k blocks of a Sync interval: k-1 each carrying a
// transaction of non-zero data that fills budget, then the empty Sync block.
func fullInterval(t *testing.T, key *ecdsa.PrivateKey, k uint64, budget int) []*types.Block {
	t.Helper()
	daSize := func(tx types.Transaction) int {
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Len() + len(binary.AppendUvarint(nil, uint64(buf.Len())))
	}
	sign := func(nonce uint64, size int) types.Transaction {
		to := common.Address{1}
		tx, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 30_000_000, To: &to, Data: bytes.Repeat([]byte{0xff}, max(size, 0))},
			ChainID:  *testSigner.ChainID(),
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(1),
		}, *testSigner, key)
		require.NoError(t, err)
		return tx
	}
	parent := common.Hash{0xc0}
	blocks := make([]*types.Block, 0, k)
	for i := range k {
		h := &types.Header{Number: *uint256.NewInt(100 + i), ParentHash: parent, Coinbase: common.Address{0xfe}, Extra: []byte{}, TxHash: empty.RootHash}
		var txs []types.Transaction
		if i < k-1 {
			size := budget
			tx := sign(i, size)
			for daSize(tx) > budget && size > 0 {
				size -= daSize(tx) - budget
				tx = sign(i, size)
			}
			if daSize(tx) <= budget {
				txs = []types.Transaction{tx}
			}
		}
		block := types.NewBlock(h, txs, nil, nil, nil, nil)
		blocks = append(blocks, block)
		parent = block.Hash()
	}
	return blocks
}

// TestBudget: the drain's projection starts at a batch of the anchor entry,
// 551,000, and takes a transaction while it stays within the ceiling less
// the drain margin; the lowest ceiling holds one more entry of no calldata.
func TestBudget(t *testing.T) {
	b := newBudget(MinPostBatchGas, 0)
	require.Equal(t, uint64(551_000), b.projected)
	require.False(t, b.fits(l1Cost{entries: 1, calldata: 1}))
	require.False(t, b.fits(l1Cost{entries: 1, target: 1}))
	require.Equal(t, uint64(551_000), b.projected, "refused, uncharged")
	require.True(t, b.fits(l1Cost{entries: 1}))
	b.take(l1Cost{entries: 1})
	require.Equal(t, uint64(921_000), b.projected)
	require.False(t, b.fits(l1Cost{calldata: 1}))
	require.False(t, newBudget(drainMargin, 0).fits(l1Cost{}), "a ceiling within the margin takes nothing")
}

// TestBudgetBlock: the bundle fits the L1 block's gas: the posting's
// projection with the drain margin and the gas limits of its triggers.
func TestBudgetBlock(t *testing.T) {
	const block = 551_000 + drainMargin + 3_000_000
	b := newBudget(MaxPostBatchGas, block)
	require.True(t, b.fits(l1Cost{trigger: 3_000_000}))
	require.False(t, b.fits(l1Cost{trigger: 3_000_001}))
	require.False(t, b.fits(l1Cost{entries: 1, trigger: 3_000_000}), "the posting grows with its entries")
	b.take(l1Cost{trigger: 2_000_000})
	require.True(t, b.fits(l1Cost{trigger: 1_000_000}))
	require.False(t, b.fits(l1Cost{trigger: 1_000_001}))
	require.True(t, newBudget(MaxPostBatchGas, 0).fits(l1Cost{trigger: 1 << 40}), "no block, no bound")
}

// TestBudgetFloor: calldata heavy enough for the EIP-7623 floor to exceed
// the standard projection is held to the floor.
func TestBudgetFloor(t *testing.T) {
	b := newBudget(MaxPostBatchGas, 0)
	// 410,000 non-zero bytes: 6,560,000 at the standard rate, 16,421,000
	// on the floor, over the cap of 16,177,216.
	data := l1Cost{entries: 1, calldata: 16 * 410_000, tokens: 4 * 410_000}
	require.LessOrEqual(t, b.projected+postBatchEntryGas+data.calldata, b.cap, "the standard projection fits")
	require.False(t, b.fits(data))
	b.take(l1Cost{entries: 1, calldata: 16 * 300_000, tokens: 4 * 300_000})
	require.Equal(t, uint64(txGas+10*4*300_000), b.floor())
	require.False(t, b.fits(l1Cost{calldata: 16 * 120_000, tokens: 4 * 120_000}))
	require.True(t, b.fits(l1Cost{calldata: 16 * 1_000, tokens: 4 * 1_000}))
}

// TestInboundCost: an inbound delivery adds its deferred entry to the batch
// and its sidecar to the DA, and nothing on L1 the posting runs.
func TestInboundCost(t *testing.T) {
	w := loadSyncWindow(t)
	_, call, output := recordedDelivery(t, w)
	in := &protocol.IncomingCall{
		SourceAddress: call.Source, SourceRollupID: call.SourceRollup, TargetAddress: call.Target, RollupID: call.RollupID,
		Value: call.Value.ToBig(), Data: call.Data, ReturnData: output,
	}
	sidecar, err := protocol.NewInboundSidecar(in)
	require.NoError(t, err)
	deferred := &protocol.ExecutionEntry{ProxyEntryHash: sidecar.ProxyEntryHash, ReturnData: output, Success: true}
	mined := w.batch.Entries[3]
	require.Equal(t, mined.ProxyEntryHash, deferred.ProxyEntryHash)
	require.Equal(t, mined.ReturnData, deferred.ReturnData)
	require.Empty(t, mined.L2ToL1Calls)
	encSidecar, err := protocol.EncodeExecutionEntry(sidecar)
	require.NoError(t, err)
	encDeferred, err := protocol.EncodeExecutionEntry(deferred)
	require.NoError(t, err)
	one := l1Cost{entries: 1, calldata: calldataGas(encSidecar) + calldataGas(encDeferred), tokens: floorTokens(encSidecar) + floorTokens(encDeferred)}
	require.Equal(t, FloorGas(append(encSidecar, encDeferred...)), txGas+10*one.tokens)
	cost, err := inboundCost([]*Delivery{{Call: in, Sidecar: sidecar}})
	require.NoError(t, err)
	require.Equal(t, one, cost)
	cost, err = inboundCost([]*Delivery{{Call: in, Sidecar: sidecar}, {Call: in, Sidecar: sidecar}})
	require.NoError(t, err)
	require.Equal(t, l1Cost{entries: 2, calldata: 2 * one.calldata, tokens: 2 * one.tokens}, cost)
}
