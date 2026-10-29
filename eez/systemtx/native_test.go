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

package systemtx_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var v01Rules = &chain.Rules{ChainID: uint256.NewInt(699169), IsLondon: true, L2Version: config.RollupXV01}

func nativeVector(t *testing.T, id string) testfixtures.NativeProfileVector {
	t.Helper()
	vectors, err := testfixtures.LoadNativeProfileVectors()
	require.NoError(t, err)
	v, ok := vectors.Vector(id)
	require.True(t, ok, id)
	return v
}

func nativeTx(t *testing.T, body protocol.NativeTx) *systemtx.NativeTransaction {
	t.Helper()
	tx, err := systemtx.NewNative(body)
	require.NoError(t, err)
	return tx
}

func historyTx(t *testing.T, body protocol.HistoryTx) *systemtx.HistoryTransaction {
	t.Helper()
	tx, err := systemtx.NewHistory(body)
	require.NoError(t, err)
	return tx
}

// decodings decodes enc, an EIP-2718 encoding, along every path a block or
// the wire takes.
func decodings(t *testing.T, enc []byte) []types.Transaction {
	t.Helper()
	wrapped, err := rlp.EncodeToBytes(enc)
	require.NoError(t, err)
	var out []types.Transaction
	for _, decode := range []func() (types.Transaction, error){
		func() (types.Transaction, error) { return types.UnmarshalTransactionFromBinary(enc, false) },
		func() (types.Transaction, error) { return types.DecodeTransaction(enc) },
		func() (types.Transaction, error) { return types.DecodeWrappedTransaction(enc) },
		func() (types.Transaction, error) { return types.DecodeTransaction(wrapped) },
	} {
		tx, err := decode()
		require.NoError(t, err)
		out = append(out, tx)
	}
	return out
}

// TestNativeVector is T-RLP-1 for type 0x45 on the reference client's
// vector native-source-base.
func TestNativeVector(t *testing.T) {
	v := nativeVector(t, "native-source-base")
	tx := nativeTx(t, protocol.NativeTx{
		ChainID:    901,
		SourceHash: common.HexToHash("0x07c8ec0f1fe6865c6719e4962c40af0cfd05cfc6dabe2c6edc0465c9649127d7"),
		GasLimit:   protocol.NativeTxGasCap,
		To:         protocol.RollupXEEZL2Address,
		Input:      []byte{0x12, 0x34, 0x56, 0x78},
	})
	require.Equal(t, []byte(v.ExpectedBytes), marshal(t, tx))
	require.Equal(t, len(v.ExpectedBytes), tx.EncodingSize())
	require.Equal(t, v.ExpectedKeccak256, tx.Hash())

	var body bytes.Buffer
	require.NoError(t, tx.EncodeRLP(&body))
	wrapped, err := rlp.EncodeToBytes([]byte(v.ExpectedBytes))
	require.NoError(t, err)
	require.Equal(t, wrapped, body.Bytes())

	for _, got := range decodings(t, v.ExpectedBytes) {
		require.IsType(t, &systemtx.NativeTransaction{}, got)
		require.Equal(t, tx.Body(), got.(*systemtx.NativeTransaction).Body())
		require.Equal(t, tx.Hash(), got.Hash())
	}
}

// TestHistoryVector is T-RLP-1 for type 0x46 on the reference client's
// vector native-history-advance.
func TestHistoryVector(t *testing.T) {
	v := nativeVector(t, "native-history-advance")
	tx := historyTx(t, protocol.HistoryTx{ChainID: 901, Blocks: []protocol.HistoryBlock{{Number: 1, Hash: common.HexToHash("0x0101010101010101010101010101010101010101010101010101010101010101")}}})
	require.Equal(t, []byte(v.ExpectedBytes), marshal(t, tx))
	require.Equal(t, v.ExpectedKeccak256, tx.Hash())
	for _, got := range decodings(t, v.ExpectedBytes) {
		require.IsType(t, &systemtx.HistoryTransaction{}, got)
		require.Equal(t, tx.Body(), got.(*systemtx.HistoryTransaction).Body())
		require.Equal(t, tx.Hash(), got.Hash())
	}
}

func TestNativeDecodeRejects(t *testing.T) {
	enc := nativeVector(t, "native-source-base").ExpectedBytes
	for n := 2; n < len(enc); n++ {
		_, err := types.UnmarshalTransactionFromBinary(enc[:n], false)
		require.Error(t, err, n)
	}
	other := bytes.Replace(enc, protocol.RollupXEEZL2Address[:], protocol.EEZL2Address[:], 1)
	_, err := types.UnmarshalTransactionFromBinary(other, false)
	require.ErrorIs(t, err, protocol.ErrNativeTx)
}

func TestNativeRegistration(t *testing.T) {
	require.True(t, types.TxTypeMintsValue(protocol.NativeTxType))
	require.True(t, types.TxTypeNonceless(protocol.NativeTxType))
	require.False(t, types.TxTypeMintsValue(protocol.HistoryTxType))
	require.True(t, types.TxTypeNonceless(protocol.HistoryTxType))
	require.False(t, types.TxTypeNonceless(protocol.SystemTxType))
}

func TestNativeAsMessage(t *testing.T) {
	signer := *types.LatestSignerForChainID(uint256.NewInt(699169))
	tx := nativeTx(t, protocol.NativeTx{ChainID: 699169, GasLimit: 1_000_000, To: protocol.RollupXEEZL2Address, Value: *uint256.NewInt(7), Input: []byte{1}})

	msg, err := tx.AsMessage(signer, uint256.NewInt(100), v01Rules)
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(protocol.NativeSystemAddress), msg.From())
	require.Equal(t, accounts.InternAddress(protocol.RollupXEEZL2Address), msg.To())
	require.Equal(t, uint64(1_000_000), msg.Gas())
	require.False(t, msg.GasFromBlock())
	require.Equal(t, uint256.NewInt(7), msg.Value())
	require.Equal(t, []byte{1}, msg.Data())
	require.True(t, msg.IsFree())
	require.False(t, msg.CheckNonce())
	require.True(t, msg.FeeCap().IsZero())
	require.Equal(t, protocol.NativeTxType, msg.TxType())

	for _, rules := range []*chain.Rules{nil, eezRules, {ChainID: uint256.NewInt(699169), L2Version: config.RollupX}} {
		_, err := tx.AsMessage(signer, nil, rules)
		require.ErrorIs(t, err, types.ErrTxTypeNotSupported)
	}
	_, err = tx.AsMessage(*types.LatestSignerForChainID(uint256.NewInt(1)), nil, v01Rules)
	require.ErrorIs(t, err, types.ErrInvalidChainId)
}

func TestHistoryAsMessage(t *testing.T) {
	signer := *types.LatestSignerForChainID(uint256.NewInt(699169))
	body := protocol.HistoryTx{ChainID: 699169, Blocks: []protocol.HistoryBlock{{Number: 5, Hash: common.Hash{5}}}}
	tx := historyTx(t, body)
	require.Empty(t, tx.GetData())
	require.Equal(t, protocol.HistoryTxGasCap, tx.GetGasLimit())
	require.Equal(t, protocol.HistoryAddress, *tx.GetTo())

	msg, err := tx.AsMessage(signer, nil, v01Rules)
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(protocol.NativeSystemAddress), msg.From())
	require.Equal(t, accounts.InternAddress(protocol.HistoryAddress), msg.To())
	require.Equal(t, protocol.HistoryTxGasCap, msg.Gas())
	require.True(t, msg.GasFromBlock())
	require.True(t, msg.Value().IsZero())
	require.Equal(t, body.Calldata(), msg.Data())
	require.True(t, msg.IsFree())
	require.False(t, msg.CheckNonce())
	require.Equal(t, protocol.HistoryTxType, msg.TxType())

	_, err = tx.AsMessage(signer, nil, eezRules)
	require.ErrorIs(t, err, types.ErrTxTypeNotSupported)
}

// TestNativeReceipt checks the standard typed receipt of type 0x45 against
// the reference client's vector native-success-receipt.
func TestNativeReceipt(t *testing.T) {
	v := nativeVector(t, "native-success-receipt")
	r := &types.Receipt{Type: protocol.NativeTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 21_000, Logs: []*types.Log{}}
	enc, err := r.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, []byte(v.ExpectedBytes), enc)

	var dec types.Receipt
	require.NoError(t, dec.UnmarshalBinary(enc))
	require.Equal(t, protocol.NativeTxType, dec.Type)
	require.Equal(t, uint64(21_000), dec.CumulativeGasUsed)
}

func TestNativeJSON(t *testing.T) {
	tx := nativeTx(t, protocol.NativeTx{ChainID: 699169, SourceHash: common.Hash{9}, GasLimit: 1_000, To: protocol.RollupXEEZL2Address, Value: *uint256.NewInt(2), Input: []byte{1}})
	enc, err := json.Marshal(tx)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"type":       "0x45",
		"hash":       tx.Hash().Hex(),
		"chainId":    "0xaab21",
		"sourceHash": common.Hash{9}.Hex(),
		"nonce":      "0x0",
		"to":         "0x4b2760f84b83d6b3829e30a7573b0172b6d6402c",
		"value":      "0x2",
		"input":      "0x01",
		"gas":        "0x3e8",
		"gasPrice":   "0x0",
		"v":          "0x0",
		"r":          "0x0",
		"s":          "0x0",
		"yParity":    "0x0",
	}, jsonFields(t, tx))
	dec, err := types.UnmarshalTransactionFromJSON(enc)
	require.NoError(t, err)
	require.Equal(t, tx.Body(), dec.(*systemtx.NativeTransaction).Body())
	require.Equal(t, tx.Hash(), dec.Hash())

	_, err = types.UnmarshalTransactionFromJSON(bytes.Replace(enc, []byte(`"nonce":"0x0"`), []byte(`"nonce":"0x1"`), 1))
	require.ErrorIs(t, err, protocol.ErrNativeTx)
}

// TestHistoryJSON parses the rendering of 699169's first history update,
// as its node served it.
func TestHistoryJSON(t *testing.T) {
	served := `{"type":"0x46","version":"0x1","chainId":"0xaab21","blocks":[{"number":"0x16a1d21","blockHash":"0x56e9172b298dcba6a05740ebb72bb1a196e28f1031624618b204d9780b97c5eb"},{"number":"0x16a1d22","blockHash":"0x597ce0fd46ff872140859dcbdaaa8e1b322b6810aba88c5a2c548395ad55f440"}],"nonce":"0x0","to":"0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0077","value":"0x0","input":"0x","gas":"0x1e8480","gasPrice":"0x0","v":"0x0","r":"0x0","s":"0x0","yParity":"0x0","hash":"0x2e5b4f1e02654c388a5af490d17ac674ac2c802d1baa3bc63f48ca0c1214123c"}`
	tx, err := types.UnmarshalTransactionFromJSON([]byte(served))
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x2e5b4f1e02654c388a5af490d17ac674ac2c802d1baa3bc63f48ca0c1214123c"), tx.Hash())

	var want, got map[string]any
	require.NoError(t, json.Unmarshal([]byte(served), &want))
	got = jsonFields(t, tx)
	require.Equal(t, want, got)

	for _, bad := range []string{
		`"input":"0x01"`, `"gas":"0x1"`, `"to":"0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0078"`, `"version":"0x2"`,
	} {
		key := bad[:bytes.IndexByte([]byte(bad), ':')]
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(served), &fields))
		fields[key[1:len(key)-1]] = json.RawMessage(bad[len(key)+1:])
		enc, err := json.Marshal(fields)
		require.NoError(t, err)
		_, err = types.UnmarshalTransactionFromJSON(enc)
		require.ErrorIs(t, err, protocol.ErrHistoryTx, bad)
	}
}

// TestLiveBlocks decodes the first blocks of 699169 and 916969, their
// history updates and native transactions included, and rebuilds every
// block hash, transactions root and encoding.
func TestLiveBlocks(t *testing.T) {
	for chainID, genesis := range map[uint64]common.Hash{
		699169: common.HexToHash("0x283b471b65276c23867b5351fe1973fc9438310d865b6f833d1cd6666478ccb0"),
		916969: common.HexToHash("0xbb9e52d97581ec62134cdfd6a4c21330de6002557f66fb8bd49b3e2f800d80ae"),
	} {
		blocks, err := testfixtures.LoadRollupXBlocks(chainID)
		require.NoError(t, err)
		counts := map[byte]int{}
		parent := common.Hash{}
		for n, raw := range blocks.Raw {
			var b types.Block
			require.NoError(t, rlp.DecodeBytes(raw, &b), n)
			require.Equal(t, uint64(n), b.NumberU64())
			require.Equal(t, parent, b.ParentHash(), n)
			require.Equal(t, b.TxHash(), types.DeriveSha(b.Transactions()), n)
			enc, err := rlp.EncodeToBytes(&b)
			require.NoError(t, err)
			require.Equal(t, raw, enc, n)
			for _, tx := range b.Transactions() {
				counts[tx.Type()]++
				require.Equal(t, chainID, tx.GetChainID().Uint64())
			}
			parent = b.Hash()
			if n == 0 {
				require.Equal(t, genesis, parent)
			}
		}
		require.Equal(t, blocks.Head, parent)
		require.NotZero(t, counts[protocol.HistoryTxType], chainID)
		require.NotZero(t, counts[protocol.NativeTxType], chainID)
		require.NotZero(t, counts[types.DynamicFeeTxType], chainID)
	}
}
