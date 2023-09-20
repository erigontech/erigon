/*
   Copyright 2021 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package types

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
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
			payload := hexutility.MustDecodeHex(tt.payloadStr)
			_, parseEnd, err := ParseHash(payload, 0, hashBuf[:0])
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(len(payload), parseEnd)
			require.Equal(hexutility.MustDecodeHex(tt.hashStr), hashBuf[:])
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
			encodeBuf = EncodeHashes(hexutility.MustDecodeHex(tt.hashesStr), encodeBuf)
			require.Equal(hexutility.MustDecodeHex(tt.payloadStr), encodeBuf)
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
			encodeBuf, err = EncodeGetPooledTransactions66(hexutility.MustDecodeHex(tt.hashesStr), tt.requestID, encodeBuf)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(hexutility.MustDecodeHex(tt.payloadStr), encodeBuf)
			if err != nil {
				return
			}
			requestID, hashes, _, err := ParseGetPooledTransactions66(encodeBuf, 0, nil)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(tt.requestID, requestID)
			require.Equal(hexutility.MustDecodeHex(tt.hashesStr), hashes)
		})
	}
}

var ptp66EncodeTests = []struct {
	txs         [][]byte
	encoded     string
	requestID   uint64
	chainID     uint64
	expectedErr bool
}{
	{
		txs: [][]byte{
			hexutility.MustDecodeHex("02f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab"),
		},
		encoded: "f88088a4e61e8ad32f4845f875b87302f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab", requestID: 11882218248461043781, expectedErr: false, chainID: 5,
	},
	{
		txs: [][]byte{
			hexutility.MustDecodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"),
			hexutility.MustDecodeHex("f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
		},
		encoded: "f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", requestID: 1111, expectedErr: false, chainID: 1,
	},
}

func TestPooledTransactionsPacket(t *testing.T) {
	b := hexutility.MustDecodeHex("e317e1a084a64018534279c4d3f05ea8cc7c9bfaa6f72d09c1d0a5f3be337e8b9226a680")
	requestID, out, pos, err := ParseGetPooledTransactions66(b, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(23), requestID)
	require.Equal(t, hexutility.MustDecodeHex("84a64018534279c4d3f05ea8cc7c9bfaa6f72d09c1d0a5f3be337e8b9226a680"), out)
	require.Equal(t, 36, pos)
}

func TestPooledTransactionsPacket66(t *testing.T) {
	for i, tt := range ptp66EncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodePooledTransactions66(tt.txs, tt.requestID, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			ctx := NewTxParseContext(*uint256.NewInt(tt.chainID))
			slots := &TxSlots{}
			requestID, _, err := ParsePooledTransactions66(encodeBuf, 0, ctx, slots, nil)
			require.NoError(err)
			require.Equal(tt.requestID, requestID)
			require.Equal(len(tt.txs), len(slots.Txs))
			for i, txn := range tt.txs {
				require.Equal(fmt.Sprintf("%x", txn), fmt.Sprintf("%x", slots.Txs[i].Rlp))
			}
		})
	}
	for i, tt := range ptp66EncodeTests {
		t.Run("reject_all_"+strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodePooledTransactions66(tt.txs, tt.requestID, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			chainID := uint256.NewInt(tt.chainID)
			ctx := NewTxParseContext(*chainID)
			slots := &TxSlots{}
			requestID, _, err := ParsePooledTransactions66(encodeBuf, 0, ctx, slots, func(bytes []byte) error { return ErrRejected })
			require.NoError(err)
			require.Equal(tt.requestID, requestID)
			require.Equal(0, len(slots.Txs))
			require.Equal(0, slots.Senders.Len())
			require.Equal(0, len(slots.IsLocal))
		})
	}
}

var tpEncodeTests = []struct {
	txs         [][]byte
	encoded     string
	chainID     uint64
	expectedErr bool
}{
	{
		txs: [][]byte{
			hexutility.MustDecodeHex("02f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab"),
		},
		encoded: "f875b87302f870051b8477359400847735940a82520894c388750a661cc0b99784bab2c55e1f38ff91643b861319718a500080c080a028bf802cf4be66f51ab0570fa9fc06365c1b816b8a7ffe40bc05f9a0d2d12867a012c2ce1fc908e7a903b750388c8c2ae82383a476bc345b7c2826738fc321fcab", expectedErr: false, chainID: 5,
	},
	{
		txs: [][]byte{
			hexutility.MustDecodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"),
			hexutility.MustDecodeHex("f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
		},
		encoded: "f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", expectedErr: false, chainID: 1,
	},
}

func TestTransactionsPacket(t *testing.T) {
	for i, tt := range tpEncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeTransactions(tt.txs, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			ctx := NewTxParseContext(*uint256.NewInt(tt.chainID))
			slots := &TxSlots{}
			_, err := ParseTransactions(encodeBuf, 0, ctx, slots, nil)
			require.NoError(err)
			require.Equal(len(tt.txs), len(slots.Txs))
			for i, txn := range tt.txs {
				require.Equal(fmt.Sprintf("%x", txn), fmt.Sprintf("%x", slots.Txs[i].Rlp))
			}
		})
	}
	for i, tt := range tpEncodeTests {
		t.Run("reject_all_"+strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeTransactions(tt.txs, encodeBuf)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))

			chainID := uint256.NewInt(tt.chainID)
			ctx := NewTxParseContext(*chainID)
			slots := &TxSlots{}
			_, err := ParseTransactions(encodeBuf, 0, ctx, slots, func(bytes []byte) error { return ErrRejected })
			require.NoError(err)
			require.Equal(0, len(slots.Txs))
			require.Equal(0, slots.Senders.Len())
			require.Equal(0, len(slots.IsLocal))
		})
	}
}
