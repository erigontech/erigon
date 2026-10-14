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
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMain(m *testing.M) {
	systemtx.Register()
	os.Exit(m.Run())
}

// minimalSystemTx is the transaction eez-rollup0 a270a06 pins in
// crates/eez-primitives/src/tests.rs (native_wire_sender_hash_and_storage_round_trip).
const minimalSystemTx = "0x76dd0180944200000000000000000000000000000000000007808401020304"

var eezRules = &chain.Rules{ChainID: uint256.NewInt(1), IsLondon: true, L2Version: config.ProtocolVersion}

func mustNew(t *testing.T, body protocol.SystemTx) *systemtx.Transaction {
	t.Helper()
	tx, err := systemtx.New(body)
	require.NoError(t, err)
	return tx
}

func marshal(t *testing.T, tx types.Transaction) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	return buf.Bytes()
}

// TestReferenceVector is T-RLP-1 on the reference client's vector.
func TestReferenceVector(t *testing.T) {
	want := common.FromHex(minimalSystemTx)
	tx := mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address, Input: []byte{1, 2, 3, 4}})

	require.Equal(t, want, marshal(t, tx))
	require.Equal(t, len(want), tx.EncodingSize())
	require.Equal(t, crypto.Keccak256Hash(want), tx.Hash())

	wrapped, err := rlp.EncodeToBytes(want)
	require.NoError(t, err)
	enc, err := rlp.EncodeToBytes(tx)
	require.NoError(t, err)
	require.Equal(t, wrapped, enc)

	for _, decode := range []func() (types.Transaction, error){
		func() (types.Transaction, error) { return types.UnmarshalTransactionFromBinary(want, false) },
		func() (types.Transaction, error) { return types.DecodeTransaction(want) },
		func() (types.Transaction, error) { return types.DecodeWrappedTransaction(want) },
		func() (types.Transaction, error) { return types.DecodeTransaction(wrapped) },
	} {
		got, err := decode()
		require.NoError(t, err)
		require.IsType(t, &systemtx.Transaction{}, got)
		require.Equal(t, tx.Body(), got.(*systemtx.Transaction).Body())
		require.Equal(t, tx.Hash(), got.Hash())
	}
}

func TestEncodingMatchesProtocol(t *testing.T) {
	bigValue := new(uint256.Int).Lsh(uint256.NewInt(1), 255)
	for _, body := range []protocol.SystemTx{
		{ChainID: 6290, To: protocol.EEZL2Address},
		{ChainID: 6290, Nonce: 127, To: protocol.EEZL2Address, Value: *uint256.NewInt(128), Input: []byte{0x7f}},
		{ChainID: 1<<64 - 1, Nonce: 1<<64 - 1, To: protocol.EEZL2Address, Value: *bigValue, Input: bytes.Repeat([]byte{0xab}, 56)},
		{ChainID: 699069, Nonce: 1 << 20, To: protocol.EEZL2Address, Input: bytes.Repeat([]byte{1}, 70_000)},
	} {
		tx := mustNew(t, body)
		want, err := body.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, want, marshal(t, tx))
		require.Equal(t, len(want), tx.EncodingSize())
		h, err := body.Hash()
		require.NoError(t, err)
		require.Equal(t, h, tx.Hash())

		got, err := types.UnmarshalTransactionFromBinary(want, false)
		require.NoError(t, err)
		require.Equal(t, body.ChainID, got.GetChainID().Uint64())
		require.Equal(t, body.Nonce, got.GetNonce())
		require.Equal(t, &body.Value, got.GetValue())
		require.True(t, bytes.Equal(body.Input, got.GetData()))
	}
}

func TestDecodeRejects(t *testing.T) {
	enc := common.FromHex(minimalSystemTx)
	for n := 2; n < len(enc); n++ {
		_, err := types.UnmarshalTransactionFromBinary(enc[:n], false)
		require.Error(t, err, "prefix of %d bytes", n)
	}
	_, err := types.UnmarshalTransactionFromBinary(append(bytes.Clone(enc), 0), false)
	require.Error(t, err)

	// Byte 10 lies in the target address.
	wrongTarget := bytes.Clone(enc)
	wrongTarget[10] ^= 1
	_, err = types.UnmarshalTransactionFromBinary(wrongTarget, false)
	require.ErrorIs(t, err, protocol.ErrSystemTx)

	// A nonce of 0 encoded as 0x00 instead of 0x80 is not canonical RLP.
	nonCanonical := bytes.Clone(enc)
	require.Equal(t, byte(0x80), nonCanonical[3])
	nonCanonical[3] = 0x00
	_, err = types.UnmarshalTransactionFromBinary(nonCanonical, false)
	require.Error(t, err)

	type signed struct {
		ChainID, Nonce uint64
		To             common.Address
		Value          uint256.Int
		Input          []byte
		V, R, S        uint256.Int
	}
	body, err := rlp.EncodeToBytes(&signed{ChainID: 1, To: protocol.EEZL2Address, Input: []byte{1}, V: *uint256.NewInt(27)})
	require.NoError(t, err)
	_, err = types.UnmarshalTransactionFromBinary(append([]byte{protocol.SystemTxType}, body...), false)
	require.Error(t, err)

	_, err = systemtx.New(protocol.SystemTx{ChainID: 1, To: common.HexToAddress("0x01")})
	require.ErrorIs(t, err, protocol.ErrSystemTx)
}

func TestSender(t *testing.T) {
	tx, err := types.DecodeTransaction(common.FromHex(minimalSystemTx))
	require.NoError(t, err)
	want := accounts.InternAddress(protocol.SystemAddress)

	from, err := types.LatestSignerForChainID(uint256.NewInt(1)).SenderWithContext(nil, tx)
	require.NoError(t, err)
	require.Equal(t, want, from)
	cached, ok := tx.GetSender()
	require.True(t, ok)
	require.Equal(t, want, cached)

	_, err = types.LatestSignerForChainID(uint256.NewInt(2)).SenderWithContext(nil, tx)
	require.ErrorIs(t, err, types.ErrInvalidChainId)

	_, err = tx.WithSignature(*types.LatestSignerForChainID(uint256.NewInt(1)), make([]byte, 65))
	require.Error(t, err)
	v, r, s := tx.RawSignatureValues()
	require.True(t, v.IsZero() && r.IsZero() && s.IsZero())
}

// TestAsMessage is the AsMessage part of T-RLP-1.
func TestAsMessage(t *testing.T) {
	tx := mustNew(t, protocol.SystemTx{ChainID: 1, Nonce: 9, To: protocol.EEZL2Address, Value: *uint256.NewInt(5), Input: []byte{1, 2}})
	signer := *types.LatestSignerForChainID(uint256.NewInt(1))
	baseFee := uint256.NewInt(7)

	msg, err := tx.AsMessage(signer, baseFee, eezRules)
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(protocol.SystemAddress), msg.From())
	require.Equal(t, accounts.InternAddress(protocol.EEZL2Address), msg.To())
	require.Equal(t, uint64(9), msg.Nonce())
	require.Equal(t, uint256.NewInt(5), msg.Value())
	require.Equal(t, []byte{1, 2}, msg.Data())
	require.Equal(t, protocol.SystemTxGasLimit, msg.Gas())
	require.True(t, msg.GasPrice().IsZero())
	require.True(t, msg.FeeCap().IsZero())
	require.True(t, msg.TipCap().IsZero())
	require.True(t, msg.IsFree())
	require.True(t, msg.CheckNonce())
	require.False(t, msg.CheckTransaction())
	require.False(t, msg.CheckGas())
	require.Empty(t, msg.AccessList())

	_, err = tx.AsMessage(signer, baseFee, &chain.Rules{ChainID: uint256.NewInt(1), IsLondon: true})
	require.ErrorIs(t, err, types.ErrTxTypeNotSupported)
	_, err = tx.AsMessage(*types.LatestSignerForChainID(uint256.NewInt(2)), baseFee, eezRules)
	require.ErrorIs(t, err, types.ErrInvalidChainId)
}

func TestBlockBody(t *testing.T) {
	sys := mustNew(t, protocol.SystemTx{ChainID: 1, Nonce: 3, To: protocol.EEZL2Address, Input: []byte{1, 2, 3}})
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	user := types.MustSignNewTx(key, *types.LatestSignerForChainID(uint256.NewInt(1)),
		types.NewEIP1559Transaction(*uint256.NewInt(1), 0, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), uint256.NewInt(2), nil))
	txs := types.Transactions{sys, user}

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(7)}
	block := types.NewBlock(header, txs, nil, nil, []*types.Withdrawal{}, nil)
	require.Equal(t, types.DeriveSha(txs), block.TxHash())

	enc, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	var decoded types.Block
	require.NoError(t, rlp.DecodeBytes(enc, &decoded))
	require.Len(t, decoded.Transactions(), 2)
	require.IsType(t, &systemtx.Transaction{}, decoded.Transactions()[0])
	require.Equal(t, sys.Hash(), decoded.Transactions()[0].Hash())
	require.Equal(t, user.Hash(), decoded.Transactions()[1].Hash())
	require.Equal(t, block.TxHash(), types.DeriveSha(decoded.Transactions()))
	require.Equal(t, block.Hash(), decoded.Hash())
}

// TestReceipt is T-RLP-3: a type-0x76 receipt carries the standard typed
// payload under its own type byte, in consensus and storage encodings.
func TestReceipt(t *testing.T) {
	newReceipt := func(ty uint8, status uint64, logs types.Logs) *types.Receipt {
		r := &types.Receipt{Type: ty, Status: status, CumulativeGasUsed: 42_000, Logs: logs}
		r.Bloom = types.CreateBloom(types.Receipts{r})
		return r
	}
	logs := types.Logs{{Address: protocol.EEZL2Address, Topics: []common.Hash{common.BytesToHash(bytes.Repeat([]byte{7}, 32))}, Data: bytes.Repeat([]byte{1}, 256)}}
	for _, tc := range []struct {
		status uint64
		logs   types.Logs
	}{{types.ReceiptStatusFailed, nil}, {types.ReceiptStatusSuccessful, logs}} {
		r := newReceipt(protocol.SystemTxType, tc.status, tc.logs)
		enc, err := r.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, protocol.SystemTxType, enc[0])
		std, err := newReceipt(types.DynamicFeeTxType, tc.status, tc.logs).MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, std[1:], enc[1:])

		var dec types.Receipt
		require.NoError(t, dec.UnmarshalBinary(enc))
		require.Equal(t, protocol.SystemTxType, dec.Type)
		require.Equal(t, tc.status, dec.Status)
		require.Equal(t, r.CumulativeGasUsed, dec.CumulativeGasUsed)
		require.Len(t, dec.Logs, len(tc.logs))

		var buf bytes.Buffer
		types.Receipts{r}.EncodeIndex(0, &buf)
		require.Equal(t, enc, buf.Bytes())

		stored, err := rlp.EncodeToBytes((*types.ReceiptForStorage)(r))
		require.NoError(t, err)
		var back types.ReceiptForStorage
		require.NoError(t, rlp.DecodeBytes(stored, &back))
		require.Equal(t, protocol.SystemTxType, back.Type)
		require.Equal(t, tc.status, back.Status)
		require.Equal(t, r.CumulativeGasUsed, back.CumulativeGasUsed)
	}
}
