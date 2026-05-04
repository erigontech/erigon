// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

var hashParseTests = []struct {
	payloadStr  string
	hashStr     string
	expectedErr bool
}{
	{payloadStr: "a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", expectedErr: false},
}

func TestParseHash(t *testing.T) {
	for i, tt := range hashParseTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var hashBuf [32]byte
			payload := hexutil.MustDecodeHex(tt.payloadStr)
			_, parseEnd, err := ParseHash(payload, 0, hashBuf[:0])
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(len(payload), parseEnd)
			require.Equal(hexutil.MustDecodeHex(tt.hashStr), hashBuf[:])
		})
	}
}

var hashEncodeTests = []struct {
	payloadStr  string
	hashesStr   string
	hashCount   int
	expectedErr bool
}{
	{payloadStr: "e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		hashesStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashCount: 1, expectedErr: false},
	{hashesStr: fmt.Sprintf("%x", toHashes(1, 2, 3)),
		payloadStr: "f863a00100000000000000000000000000000000000000000000000000000000000000a00200000000000000000000000000000000000000000000000000000000000000a00300000000000000000000000000000000000000000000000000000000000000", hashCount: 3, expectedErr: false},
}

func TestEncodeHash(t *testing.T) {
	for i, tt := range hashEncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeHashes(hexutil.MustDecodeHex(tt.hashesStr), encodeBuf)
			require.Equal(hexutil.MustDecodeHex(tt.payloadStr), encodeBuf)
		})
	}
}

var gpt66EncodeTests = []struct {
	payloadStr  string
	hashesStr   string
	hashCount   int
	requestID   uint64
	expectedErr bool
}{
	{payloadStr: "e68306f854e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		hashesStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashCount: 1, requestID: 456788, expectedErr: false},
}

// TestEncodeGPT66 tests the encoding of GetPoolTransactions66 packet
func TestEncodeGPT66(t *testing.T) {
	for i, tt := range gpt66EncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			var err error
			encodeBuf, err = EncodeGetPooledTransactions66(hexutil.MustDecodeHex(tt.hashesStr), tt.requestID, encodeBuf)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(hexutil.MustDecodeHex(tt.payloadStr), encodeBuf)
			if err != nil {
				return
			}
			requestID, hashes, _, err := ParseGetPooledTransactions66(encodeBuf, 0, nil)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(tt.requestID, requestID)
			require.Equal(hexutil.MustDecodeHex(tt.hashesStr), hashes)
		})
	}
}

var ptp66EncodeTests = []struct {
	txns        [][]byte
	encoded     string
	requestID   uint64
	chainID     uint64
	expectedErr bool
}{
	{
		txns: [][]byte{
			hexutil.MustDecodeHex("02f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab"),
		},
		encoded: "f88088a4e61e8ad32f4845f875b87302f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab", requestID: 11882218248461043781, expectedErr: false, chainID: 5,
	},
	{
		txns: [][]byte{
			hexutil.MustDecodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"),
			hexutil.MustDecodeHex("f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
		},
		encoded: "f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", requestID: 1111, expectedErr: false, chainID: 1,
	},
}

func TestPooledTransactionsPacket(t *testing.T) {
	b := hexutil.MustDecodeHex("e317e1a084a64018534279c4d3f05ea8cc7c9bfaa6f72d09c1d0a5f3be337e8b9226a680")
	requestID, out, pos, err := ParseGetPooledTransactions66(b, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(23), requestID)
	require.Equal(t, hexutil.MustDecodeHex("84a64018534279c4d3f05ea8cc7c9bfaa6f72d09c1d0a5f3be337e8b9226a680"), out)
	require.Equal(t, 36, pos)
}

func TestPooledTransactionsPacket66(t *testing.T) {
	for i, tt := range ptp66EncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodePooledTransactions66(tt.txns, tt.requestID, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			ctx := NewTxnParseContext(*uint256.NewInt(tt.chainID))
			slots := &TxnSlots{}
			requestID, _, err := ParsePooledTransactions66(encodeBuf, 0, ctx, slots, nil)
			require.NoError(err)
			require.Equal(tt.requestID, requestID)
			require.Len(slots.Txns, len(tt.txns))
			for i, txn := range tt.txns {
				require.Equal(fmt.Sprintf("%x", txn), fmt.Sprintf("%x", slots.Txns[i].Rlp))
			}
		})
	}
	for i, tt := range ptp66EncodeTests {
		t.Run("reject_all_"+strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodePooledTransactions66(tt.txns, tt.requestID, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			chainID := uint256.NewInt(tt.chainID)
			ctx := NewTxnParseContext(*chainID)
			slots := &TxnSlots{}
			requestID, _, err := ParsePooledTransactions66(encodeBuf, 0, ctx, slots, func(bytes []byte) error { return ErrRejected })
			require.NoError(err)
			require.Equal(tt.requestID, requestID)
			require.Empty(slots.Txns)
			require.Equal(0, slots.Senders.Len())
			require.Empty(slots.IsLocal)
		})
	}
}

var tpEncodeTests = []struct {
	txns        [][]byte
	encoded     string
	chainID     uint64
	expectedErr bool
}{
	{
		txns: [][]byte{
			hexutil.MustDecodeHex("02f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab"),
		},
		encoded: "f875b87302f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab", expectedErr: false, chainID: 5,
	},
	{
		txns: [][]byte{
			hexutil.MustDecodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"),
			hexutil.MustDecodeHex("f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
		},
		encoded: "f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", expectedErr: false, chainID: 1,
	},
}

// TestEIP7702BatchPoisoning is a regression test for
// https://github.com/ethereum-bounty/erigon/issues/7. A single EIP-7702
// SetCode transaction carrying one auth tuple with an unrecoverable signature
// must not cause sibling transactions in the same Transactions / PooledTransactions
// devp2p packet to be dropped, nor invalidate the SetCode transaction itself.
// Per the EIP, "if any step fails for a tuple, processing continues to the
// next one" — invalid auth tuples are skipped at execution time and the
// enclosing transaction remains valid (the sender still pays for every tuple).
func TestEIP7702BatchPoisoning(t *testing.T) {
	chainID := uint256.NewInt(1)
	signer := types.LatestSignerForChainID(chainID.ToBig())
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	to := common.Address{0x42}

	mkDynFee := func(nonce uint64) types.Transaction {
		unsigned := &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    nonce,
				GasLimit: 21000,
				To:       &to,
			},
			ChainID: *chainID,
			TipCap:  *uint256.NewInt(1_000_000),
			FeeCap:  *uint256.NewInt(1_000_000),
		}
		signed, signErr := types.SignTx(unsigned, *signer, key)
		require.NoError(t, signErr)
		return signed
	}

	// SetCode tx with one auth tuple whose signature is unrecoverable (R=S=0).
	// The sender's signature on the transaction itself is still valid.
	setCodeUnsigned := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 100_000,
				To:       &to,
			},
			ChainID: *chainID,
			TipCap:  *uint256.NewInt(1_000_000),
			FeeCap:  *uint256.NewInt(1_000_000),
		},
		Authorizations: []types.Authorization{{
			ChainID: *chainID,
			Address: common.Address{0x99},
			Nonce:   0,
			YParity: 0,
			// R and S left zero — RecoverSigner rejects the tuple.
		}},
	}
	setCode, err := types.SignTx(setCodeUnsigned, *signer, key)
	require.NoError(t, err)

	// Three-tx batch: sibling, poisoned SetCode, sibling.
	txns := []types.Transaction{mkDynFee(0), setCode, mkDynFee(2)}
	txnsRlp := make([][]byte, len(txns))
	for i, txn := range txns {
		buf := bytes.NewBuffer(nil)
		require.NoError(t, txn.MarshalBinary(buf))
		txnsRlp[i] = buf.Bytes()
	}

	cases := []struct {
		name   string
		encode func() []byte
		parse  func(payload []byte, slots *TxnSlots) error
	}{
		{
			name:   "Transactions",
			encode: func() []byte { return EncodeTransactions(txnsRlp, nil) },
			parse: func(payload []byte, slots *TxnSlots) error {
				ctx := NewTxnParseContext(*chainID)
				_, parseErr := ParseTransactions(payload, 0, ctx, slots, nil)
				return parseErr
			},
		},
		{
			name:   "PooledTransactions66",
			encode: func() []byte { return EncodePooledTransactions66(txnsRlp, 1, nil) },
			parse: func(payload []byte, slots *TxnSlots) error {
				ctx := NewTxnParseContext(*chainID)
				_, _, parseErr := ParsePooledTransactions66(payload, 0, ctx, slots, nil)
				return parseErr
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			slots := &TxnSlots{}
			r.NoError(tt.parse(tt.encode(), slots))
			// All three txns survive — no batch poisoning.
			r.Len(slots.Txns, 3)

			// Siblings parsed normally.
			r.Equal(byte(DynamicFeeTxnType), slots.Txns[0].TxType())
			r.Equal(byte(DynamicFeeTxnType), slots.Txns[2].TxType())

			// SetCode tx is accepted, but its single auth tuple was skipped.
			// Gas billing still sees the original auth list (length 1) because
			// per EIP-7702 the sender pays for every tuple regardless of validity.
			setCodeSlot := slots.Txns[1]
			r.Equal(byte(SetCodeTxnType), setCodeSlot.TxType())
			r.Len(setCodeSlot.Txn.GetAuthorizations(), 1)
			r.Empty(setCodeSlot.AuthAndNonces)
		})
	}
}

func TestTransactionsPacket(t *testing.T) {
	for i, tt := range tpEncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeTransactions(tt.txns, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			ctx := NewTxnParseContext(*uint256.NewInt(tt.chainID))
			slots := &TxnSlots{}
			_, err := ParseTransactions(encodeBuf, 0, ctx, slots, nil)
			require.NoError(err)
			require.Len(slots.Txns, len(tt.txns))
			for i, txn := range tt.txns {
				require.Equal(fmt.Sprintf("%x", txn), fmt.Sprintf("%x", slots.Txns[i].Rlp))
			}
		})
	}
	for i, tt := range tpEncodeTests {
		t.Run("reject_all_"+strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeTransactions(tt.txns, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			chainID := uint256.NewInt(tt.chainID)
			ctx := NewTxnParseContext(*chainID)
			slots := &TxnSlots{}
			_, err := ParseTransactions(encodeBuf, 0, ctx, slots, func(bytes []byte) error { return ErrRejected })
			require.NoError(err)
			require.Empty(slots.Txns)
			require.Equal(0, slots.Senders.Len())
			require.Empty(slots.IsLocal)
		})
	}
}
