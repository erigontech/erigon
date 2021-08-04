/*
   Copyright 2021 Erigon contributors

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

package txpool

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

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
			payload := decodeHex(tt.payloadStr)
			_, parseEnd, err := ParseHash(payload, 0, hashBuf[:0])
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(len(payload), parseEnd)
			require.Equal(decodeHex(tt.hashStr), hashBuf[:])
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
	{hashesStr: fmt.Sprintf("%x", toHashes([32]byte{1}, [32]byte{2}, [32]byte{3})),
		payloadStr: "f863a00100000000000000000000000000000000000000000000000000000000000000a00200000000000000000000000000000000000000000000000000000000000000a00300000000000000000000000000000000000000000000000000000000000000", hashCount: 3, expectedErr: false},
}

func TestEncodeHash(t *testing.T) {
	for i, tt := range hashEncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			encodeBuf = EncodeHashes(decodeHex(tt.hashesStr), encodeBuf)
			require.Equal(decodeHex(tt.payloadStr), encodeBuf)
		})
	}
}

var gpt66EncodeTests = []struct {
	payloadStr  string
	hashesStr   string
	hashCount   int
	requestId   uint64
	expectedErr bool
}{
	{payloadStr: "e68306f854e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		hashesStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashCount: 1, requestId: 456788, expectedErr: false},
}

// TestEncodeGPT66 tests the encoding of GetPoolTransactions66 packet
func TestEncodeGPT66(t *testing.T) {
	for i, tt := range gpt66EncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			var err error
			encodeBuf, err = EncodeGetPooledTransactions66(decodeHex(tt.hashesStr), tt.requestId, encodeBuf)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(decodeHex(tt.payloadStr), encodeBuf)
			if err != nil {
				return
			}
			requestID, hashes, _, err := ParseGetPooledTransactions66(encodeBuf, 0, nil)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(tt.requestId, requestID)
			require.Equal(decodeHex(tt.hashesStr), hashes)
		})
	}
}

var ptp66EncodeTests = []struct {
	txs         [][]byte
	encoded     string
	requestId   uint64
	expectedErr bool
}{
	{
		txs: [][]byte{
			decodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"),
			decodeHex("f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
		},
		encoded: "f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", requestId: 1111, expectedErr: false},
}

func TestPooledTransactionsPacket66(t *testing.T) {
	for i, tt := range ptp66EncodeTests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require := require.New(t)
			var encodeBuf []byte
			var err error
			encodeBuf = EncodePooledTransactions66(tt.txs, tt.requestId, encodeBuf)
			require.Equal(tt.expectedErr, err != nil)
			require.Equal(tt.encoded, fmt.Sprintf("%x", encodeBuf))
		})
	}
}
