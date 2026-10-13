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

package protocol

import (
	"bytes"
	"crypto/sha256"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/rlp"
)

// goldenInboundSystemTx is the type-0x76 transaction pinned by eez-rollup0
// a270a06 in crates/eez-protocol/src/system_tx.rs
// (inbound_system_transaction_golden_vector) and encoded there with
// alloy-sol-types: executeIncomingCrossChainCall with one entry delivering a
// call from 0x…cc on rollup 0 to 0x…bb with data 0x1234 and return data 0xabcd.
const goldenInboundSystemTx = "" +
	"76f9035f010794420000000000000000000000000000000000000780b90344c3fb5f3d00000000000000000000000000" +
	"000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000" +
	"000320000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000" +
	"0000000000000000000000000000000000002047d8d2c330b8e207c17da548b89a3058b10faad241a6b82f88302af38b" +
	"ff8fb700000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000" +
	"000000000000000000000000000000000002407b6cdaf67ec74ff399bdbcf0e0d2a68fb4aa7b638a87d382baef85c6ba" +
	"8e5d3c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000" +
	"000000000000000000000000000000000002600000000000000000000000000000000000000000000000000000000000" +
	"000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000000000000000000000000000000000000cc0000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000bb00000000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000100000000000000000000000000000000000000000000000000000000000000000212340000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"0000000000000000000000000000000000000000000000000000000000000000000002abcd0000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000000"

// goldenInboundCalldata returns the input field of goldenInboundSystemTx.
func goldenInboundCalldata(t *testing.T) []byte {
	t.Helper()
	tx := common.FromHex(goldenInboundSystemTx)
	require.Equal(t, byte(0x76), tx[0])
	fields, rest, err := rlp.SplitList(tx[1:])
	require.NoError(t, err)
	require.Empty(t, rest)
	var input []byte
	for range 5 { // chainId, nonce, to, value, input
		input, fields, err = rlp.SplitString(fields)
		require.NoError(t, err)
	}
	require.Empty(t, fields)
	return input
}

// TestDecodeWindow84Batch is T-VEC-5 on the devnet capture: the batch decodes
// to one anchor entry moving rollup 1 across the window and re-encodes to the
// recorded bytes.
func TestDecodeWindow84Batch(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	oracleHash := func(key string) common.Hash {
		h, err := w.OracleHash(key)
		require.NoError(t, err)
		return h
	}
	sum := sha256.Sum256(w.PostBatch)
	require.Equal(t, oracleHash("postbatch_calldata_sha256"), common.Hash(sum))

	b, err := DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	require.Len(t, b.Entries, 1)
	e := &b.Entries[0]
	require.Equal(t, common.Hash{}, e.ProxyEntryHash)
	require.Equal(t, uint64(1), e.DestinationRollupID)
	require.True(t, e.Success)
	require.Empty(t, e.L2ToL1Calls)
	require.Len(t, e.RollupUpdates, 1)
	u := &e.RollupUpdates[0]
	require.Equal(t, uint64(1), u.RollupID)
	require.Zero(t, u.EtherDelta.Sign())
	require.Equal(t, oracleHash("window_pre_block_hash"), u.CurrentRoot)
	require.Equal(t, oracleHash("window_post_block_hash"), u.NewRoot)
	require.Equal(t, 0, b.ImmediateEntryCount.Cmp(big.NewInt(1)))
	require.Equal(t, []RollupIDWithProofSystems{{RollupID: 1, ProofSystemIndexes: []uint64{0}}}, b.RollupIDsWithProofSystems)
	require.Len(t, b.ProofSystems, 1)
	require.Equal(t, common.HexToAddress(string(w.Oracle["proof_system"][1:43])), b.ProofSystems[0])
	require.Equal(t, [][]byte{common.FromHex(string(w.Oracle["expected_test_signature"][1:133]))}, b.Proofs)
	require.NotEmpty(t, b.CallData)
	require.Empty(t, b.BlobIndices)

	enc, err := EncodePostAndVerifyBatch(b)
	require.NoError(t, err)
	require.Equal(t, w.PostBatch, enc)
}

// TestDecodeChiadoBatch decodes the live Chiado settlement: an anchor entry
// whose new root is the registry's root for rollup 1 at the settling block,
// with its DA in the transaction's only blob.
func TestDecodeChiadoBatch(t *testing.T) {
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	b, err := DecodePostAndVerifyBatch(s.Transaction.Input)
	require.NoError(t, err)
	require.Len(t, b.Entries, 1)
	require.Len(t, b.Entries[0].RollupUpdates, 1)
	u := &b.Entries[0].RollupUpdates[0]
	require.Equal(t, uint64(1), u.RollupID)
	require.Equal(t, s.StateAtBlock.RollupRoot, u.NewRoot)
	require.Equal(t, []common.Address{s.ProofSystem}, b.ProofSystems)
	require.Len(t, b.BlobIndices, len(s.Transaction.BlobVersionedHashes))
	require.Zero(t, b.BlobIndices[0].Sign())
	require.Empty(t, b.CallData)

	enc, err := EncodePostAndVerifyBatch(b)
	require.NoError(t, err)
	require.Equal(t, []byte(s.Transaction.Input), enc)
}

// TestGoldenInboundCalldata decodes the alloy-encoded executeIncomingCrossChainCall
// of the reference client and re-encodes it to the same bytes.
func TestGoldenInboundCalldata(t *testing.T) {
	input := goldenInboundCalldata(t)
	table, err := DecodeExecuteIncomingCrossChainCall(input)
	require.NoError(t, err)
	require.Len(t, table.Entries, 1)
	require.Empty(t, table.StaticEntries)
	e := &table.Entries[0]
	require.Len(t, e.IncomingCalls, 1)
	require.Empty(t, e.ExpectedOutgoingCalls)
	require.True(t, e.Success)
	require.Equal(t, common.FromHex("0xabcd"), e.ReturnData)
	c := &e.IncomingCalls[0]
	require.Equal(t, common.HexToAddress("0xcc"), c.SourceAddress)
	require.Equal(t, uint64(0), c.SourceRollupID)
	require.Equal(t, common.HexToAddress("0xbb"), c.TargetAddress)
	require.Equal(t, common.FromHex("0x1234"), c.Data)
	require.Zero(t, c.Value.Sign())
	require.Zero(t, c.Gas)
	require.False(t, c.IsStatic)
	require.Zero(t, c.RevertNextNCalls)

	enc, err := EncodeExecuteIncomingCrossChainCall(table)
	require.NoError(t, err)
	require.Equal(t, input, enc)

	_, err = DecodeLoadExecutionTable(input)
	require.ErrorIs(t, err, ErrSelector)
	load, err := EncodeLoadExecutionTable(table)
	require.NoError(t, err)
	require.Equal(t, LoadExecutionTableSelector[:], load[:4])
	require.Equal(t, input[4:], load[4:])
}

// TestDecodeAcceptsWhatSolidityAccepts covers encodings the registry accepts
// although they are not canonical: trailing bytes, a relocated tail and a
// missing final padding decode to the same batch.
func TestDecodeAcceptsWhatSolidityAccepts(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	canonical := w.PostBatch
	require.Equal(t, common.LeftPadBytes([]byte{0x20}, 32), canonical[4:36])

	// The batch tuple's offset 0x20 becomes 0x40 over an inserted junk word.
	relocated := bytes.Clone(canonical[:4])
	relocated = append(relocated, common.LeftPadBytes([]byte{0x40}, 32)...)
	relocated = append(relocated, bytes.Repeat([]byte{0xff}, 32)...)
	relocated = append(relocated, canonical[36:]...)

	for name, input := range map[string][]byte{
		"trailing bytes": append(bytes.Clone(canonical), 0xde, 0xad),
		"relocated tail": relocated,
		// The last tail is the 65-byte proof, padded with 31 zero bytes.
		"missing final padding": canonical[:len(canonical)-31],
	} {
		t.Run(name, func(t *testing.T) {
			b, err := DecodePostAndVerifyBatch(input)
			require.NoError(t, err)
			enc, err := EncodePostAndVerifyBatch(b)
			require.NoError(t, err)
			require.Equal(t, canonical, enc)
		})
	}
}

func TestDecodeRejects(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	canonical := w.PostBatch

	_, err = DecodePostAndVerifyBatch(canonical[:3])
	require.ErrorIs(t, err, ErrSelector)
	_, err = DecodePostAndVerifyBatch(goldenInboundCalldata(t))
	require.ErrorIs(t, err, ErrSelector)
	_, err = DecodePostAndVerifyBatch(canonical[:4])
	require.Error(t, err)
	_, err = DecodePostAndVerifyBatch(canonical[:len(canonical)-32])
	require.Error(t, err)

	beyond := bytes.Clone(canonical)
	copy(beyond[4:36], common.LeftPadBytes([]byte{0x7f, 0xff, 0xff}, 32))
	_, err = DecodePostAndVerifyBatch(beyond)
	require.Error(t, err)

	// An int192 word that is not sign-extended reads above the int192 range;
	// Solidity reverts on it.
	marker := big.NewInt(-0x123456789)
	g := newValueGen(2)
	b := g.batch()
	b.Entries = []ExecutionEntry{g.entry()}
	b.Entries[0].RollupUpdates = []RollupUpdate{{EtherDelta: marker}}
	enc, err := EncodePostAndVerifyBatch(&b)
	require.NoError(t, err)
	word := refInt(marker)
	at := bytes.Index(enc, word[:])
	require.Positive(t, at)
	clear(enc[at : at+8])
	_, err = DecodePostAndVerifyBatch(enc)
	require.ErrorIs(t, err, ErrIntegerRange)
	require.ErrorContains(t, err, "entries[0].rollupUpdates[0].etherDelta")
}

func TestEncodeRejectsNilIntegers(t *testing.T) {
	b := newValueGen(3).batch()
	b.ImmediateEntryCount = nil
	_, err := EncodePostAndVerifyBatch(&b)
	require.ErrorIs(t, err, ErrNilInteger)
	require.ErrorContains(t, err, "immediateEntryCount")

	b = newValueGen(3).batch()
	b.BlobIndices = []*big.Int{nil}
	_, err = EncodePostAndVerifyBatch(&b)
	require.ErrorIs(t, err, ErrNilInteger)

	_, err = EncodeLoadExecutionTable(&L2ExecutionTable{StaticEntries: []L2StaticExecutionEntry{{}}})
	require.ErrorIs(t, err, ErrNilInteger)
	require.ErrorContains(t, err, "staticEntries[0].expectedEntryIndex")

	_, err = EncodeExecuteIncomingCrossChainCall(&L2ExecutionTable{Entries: []L2ExecutionEntry{{
		ExpectedOutgoingCalls: []ExpectedCall{{Calls: []CrossChainCall{{}}}},
	}}})
	require.ErrorIs(t, err, ErrNilInteger)
	require.ErrorContains(t, err, "entries[0].expectedOutgoingCalls[0].calls[0].value")
}

// TestCalldataRoundTrip requires decode to invert encode on generated values,
// compared through their canonical encoding.
func TestCalldataRoundTrip(t *testing.T) {
	g := newValueGen(4)
	for range 200 {
		b := g.batch()
		enc, err := EncodePostAndVerifyBatch(&b)
		require.NoError(t, err)
		require.Equal(t, refEncode(refBatch(&b)), enc[4:])
		dec, err := DecodePostAndVerifyBatch(enc)
		require.NoError(t, err)
		reenc, err := EncodePostAndVerifyBatch(dec)
		require.NoError(t, err)
		require.Equal(t, enc, reenc)

		table := L2ExecutionTable{Entries: g.l2Entries(), StaticEntries: g.l2StaticEntries()}
		enc, err = EncodeLoadExecutionTable(&table)
		require.NoError(t, err)
		decTable, err := DecodeLoadExecutionTable(enc)
		require.NoError(t, err)
		reenc, err = EncodeLoadExecutionTable(decTable)
		require.NoError(t, err)
		require.Equal(t, enc, reenc)
	}
}

func FuzzDecodePostAndVerifyBatch(f *testing.F) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(f, err)
	f.Add(w.PostBatch)
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(f, err)
	f.Add([]byte(s.Transaction.Input))
	f.Fuzz(func(t *testing.T, input []byte) {
		b, err := DecodePostAndVerifyBatch(input)
		if err != nil {
			return
		}
		enc, err := EncodePostAndVerifyBatch(b)
		require.NoError(t, err)
		again, err := DecodePostAndVerifyBatch(enc)
		require.NoError(t, err)
		reenc, err := EncodePostAndVerifyBatch(again)
		require.NoError(t, err)
		require.Equal(t, enc, reenc)
	})
}
