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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/rlp"
)

func mustRLP(t *testing.T, v any) []byte {
	t.Helper()
	b, err := rlp.EncodeToBytes(v)
	require.NoError(t, err)
	return b
}

func nativeVector(t *testing.T, id string) testfixtures.NativeProfileVector {
	t.Helper()
	vectors, err := testfixtures.LoadNativeProfileVectors()
	require.NoError(t, err)
	v, ok := vectors.Vector(id)
	require.True(t, ok, id)
	return v
}

func parseUint(t *testing.T, s string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(s, 0, 64)
	require.NoError(t, err)
	return n
}

func TestSourceHashVectors(t *testing.T) {
	for _, id := range []string{
		"source-base", "source-after-prefix-consumed", "source-next-group", "source-first-group",
		"source-new-parent", "source-different-chain", "source-uint64-maximum",
	} {
		t.Run(id, func(t *testing.T) {
			v := nativeVector(t, id)
			require.Equal(t, []string{sourceEntryDomain}, v.Arguments[:1])
			got := SourceHash(parseUint(t, v.Arguments[1]), common.HexToHash(v.Arguments[2]), parseUint(t, v.Arguments[3]))
			require.Equal(t, crypto.Keccak256Hash(v.ExpectedBytes), v.ExpectedKeccak256)
			require.Equal(t, v.ExpectedKeccak256, got)
		})
	}
}

func TestNativeTxVectors(t *testing.T) {
	for _, id := range []string{"native-source-base", "native-source-next-group"} {
		t.Run(id, func(t *testing.T) {
			v := nativeVector(t, id)
			var f []hexutil.Bytes
			require.NoError(t, json.Unmarshal(v.Fields, &f))
			require.Len(t, f, 6)
			tx := &NativeTx{
				ChainID:    new(uint256.Int).SetBytes(f[0]).Uint64(),
				SourceHash: common.BytesToHash(f[1]),
				GasLimit:   new(uint256.Int).SetBytes(f[2]).Uint64(),
				To:         common.BytesToAddress(f[3]),
				Value:      *new(uint256.Int).SetBytes(f[4]),
				Input:      f[5],
			}
			enc, err := tx.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, []byte(v.ExpectedBytes), enc)
			h, err := tx.Hash()
			require.NoError(t, err)
			require.Equal(t, v.ExpectedKeccak256, h)

			dec, err := DecodeNativeTx(enc)
			require.NoError(t, err)
			require.Equal(t, tx, dec)
		})
	}
}

func TestHistoryTxVectors(t *testing.T) {
	for _, id := range []string{"native-history-advance", "native-history-maximum"} {
		t.Run(id, func(t *testing.T) {
			v := nativeVector(t, id)
			var f []json.RawMessage
			require.NoError(t, json.Unmarshal(v.Fields, &f))
			require.Len(t, f, 3)
			var version, chainID hexutil.Bytes
			var pairs [][]hexutil.Bytes
			require.NoError(t, json.Unmarshal(f[0], &version))
			require.NoError(t, json.Unmarshal(f[1], &chainID))
			require.NoError(t, json.Unmarshal(f[2], &pairs))
			require.Equal(t, hexutil.Bytes{1}, version)
			tx := &HistoryTx{ChainID: new(uint256.Int).SetBytes(chainID).Uint64()}
			for _, p := range pairs {
				tx.Blocks = append(tx.Blocks, HistoryBlock{Number: new(uint256.Int).SetBytes(p[0]).Uint64(), Hash: common.BytesToHash(p[1])})
			}
			if id == "native-history-maximum" {
				require.Len(t, tx.Blocks, MaxHistoryBlocks)
			}
			enc, err := tx.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, []byte(v.ExpectedBytes), enc)
			h, err := tx.Hash()
			require.NoError(t, err)
			require.Equal(t, v.ExpectedKeccak256, h)

			dec, err := DecodeHistoryTx(enc)
			require.NoError(t, err)
			require.Equal(t, tx, dec)
		})
	}
}

func TestHistoryCalldataVector(t *testing.T) {
	v := nativeVector(t, "history-advance-calldata")
	require.Equal(t, "importL1Blocks((uint64,bytes32)[])", v.Signature)
	require.Equal(t, []string{"[(100," + common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999").Hex() + ")]"}, v.Arguments)
	tx := &HistoryTx{Blocks: []HistoryBlock{{Number: 100, Hash: common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999")}}}
	require.Equal(t, []byte(v.ExpectedBytes), tx.Calldata())
}

// TestLiveHistoryTx is the first history update of 699169, tx 0 of its L2
// block 3: it imports its L1 reference block 23,731,489 and the next.
func TestLiveHistoryTx(t *testing.T) {
	enc := common.FromHex("0x46f85501830aab21f84ee684016a1d21a056e9172b298dcba6a05740ebb72bb1a196e28f1031624618b204d9780b97c5ebe684016a1d22a0597ce0fd46ff872140859dcbdaaa8e1b322b6810aba88c5a2c548395ad55f440")
	tx, err := DecodeHistoryTx(enc)
	require.NoError(t, err)
	require.Equal(t, uint64(699169), tx.ChainID)
	require.Equal(t, []HistoryBlock{
		{Number: 23_731_489, Hash: common.HexToHash("0x56e9172b298dcba6a05740ebb72bb1a196e28f1031624618b204d9780b97c5eb")},
		{Number: 23_731_490, Hash: common.HexToHash("0x597ce0fd46ff872140859dcbdaaa8e1b322b6810aba88c5a2c548395ad55f440")},
	}, tx.Blocks)
	h, err := tx.Hash()
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x2e5b4f1e02654c388a5af490d17ac674ac2c802d1baa3bc63f48ca0c1214123c"), h)
}

func TestNativeTxGasLimit(t *testing.T) {
	require.Equal(t, NativeTxGasCap, NativeTxGasLimit(30_000_000, 0))
	require.Equal(t, NativeTxGasCap, NativeTxGasLimit(30_000_000, 30_000_000-NativeTxGasCap))
	require.Equal(t, uint64(1_000), NativeTxGasLimit(30_000_000, 29_999_000))
	require.Zero(t, NativeTxGasLimit(30_000_000, 30_000_000))
}

func TestNativeTxRejects(t *testing.T) {
	valid := NativeTx{ChainID: 699169, GasLimit: NativeTxGasCap, To: RollupXEEZL2Address, Input: []byte{1}}
	enc, err := valid.MarshalBinary()
	require.NoError(t, err)

	for name, tx := range map[string]NativeTx{
		"rollup0 EEZL2":  {ChainID: 1, To: EEZL2Address},
		"gas above cap":  {ChainID: 1, GasLimit: NativeTxGasCap + 1, To: RollupXEEZL2Address},
		"other contract": {ChainID: 1, To: HistoryAddress},
	} {
		_, err := tx.MarshalBinary()
		require.ErrorIs(t, err, ErrNativeTx, name)
	}
	for name, b := range map[string][]byte{
		"empty":          nil,
		"type 0x76":      append([]byte{SystemTxType}, enc[1:]...),
		"trailing bytes": append(append([]byte{}, enc...), 0),
		"five fields":    append([]byte{NativeTxType}, mustRLP(t, []any{uint64(1), common.Hash{}, uint64(0), RollupXEEZL2Address, uint64(0)})...),
	} {
		_, err := DecodeNativeTx(b)
		require.ErrorIs(t, err, ErrNativeTx, name)
	}
}

func TestHistoryTxRejects(t *testing.T) {
	block := func(n uint64) HistoryBlock { return HistoryBlock{Number: n, Hash: common.Hash{byte(n)}} }
	many := make([]HistoryBlock, MaxHistoryBlocks+1)
	for i := range many {
		many[i] = block(uint64(i + 1))
	}
	for name, blocks := range map[string][]HistoryBlock{
		"no blocks":  nil,
		"too many":   many,
		"block zero": {{Number: 0, Hash: common.Hash{1}}},
		"zero hash":  {{Number: 1}},
		"repeated":   {block(2), block(2)},
		"decreasing": {block(3), block(2)},
	} {
		tx := HistoryTx{ChainID: 1, Blocks: blocks}
		_, err := tx.MarshalBinary()
		require.ErrorIs(t, err, ErrHistoryTx, name)
		enc := append([]byte{HistoryTxType}, mustRLP(t, &historyTxRLP{Version: 1, ChainID: 1, Blocks: blocks})...)
		_, err = DecodeHistoryTx(enc)
		require.ErrorIs(t, err, ErrHistoryTx, name)
	}
	enc := append([]byte{HistoryTxType}, mustRLP(t, &historyTxRLP{Version: 2, ChainID: 1, Blocks: []HistoryBlock{block(1)}})...)
	_, err := DecodeHistoryTx(enc)
	require.ErrorIs(t, err, ErrHistoryTx)
}
