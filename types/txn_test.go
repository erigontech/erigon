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

package types

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransactionRLP(t *testing.T) {
	for _, testSet := range allNetsTestCases {
		testSet := testSet
		t.Run(strconv.Itoa(int(testSet.chainID.Uint64())), func(t *testing.T) {
			require := require.New(t)
			ctx := NewTxParseContext(testSet.chainID)
			tx, txSender := &TxSlot{}, [20]byte{}
			for i, tt := range testSet.tests {
				tt := tt
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					payload := decodeHex(tt.PayloadStr)
					parseEnd, err := ctx.ParseTransaction(payload, 0, tx, txSender[:], false /* hasEnvelope */, nil)
					require.NoError(err)
					require.Equal(len(payload), parseEnd)
					if tt.SignHashStr != "" {
						signHash := decodeHex(tt.SignHashStr)
						if !bytes.Equal(signHash, ctx.Sighash[:]) {
							t.Errorf("signHash expected %x, got %x", signHash, ctx.Sighash)
						}
					}
					if tt.IdHashStr != "" {
						idHash := decodeHex(tt.IdHashStr)
						if !bytes.Equal(idHash, tx.IDHash[:]) {
							t.Errorf("IdHash expected %x, got %x", idHash, tx.IDHash)
						}
					}
					if tt.SenderStr != "" {
						expectSender := decodeHex(tt.SenderStr)
						if !bytes.Equal(expectSender, txSender[:]) {
							t.Errorf("expectSender expected %x, got %x", expectSender, txSender)
						}
					}
					require.Equal(tt.Nonce, tx.Nonce)
				})
			}
		})
	}
}

func TestTxSlotsGrowth(t *testing.T) {
	assert := assert.New(t)
	s := &TxSlots{}
	s.Resize(11)
	assert.Equal(11, len(s.Txs))
	assert.Equal(11, s.Senders.Len())
	s.Resize(23)
	assert.Equal(23, len(s.Txs))
	assert.Equal(23, s.Senders.Len())

	s = &TxSlots{Txs: make([]*TxSlot, 20), Senders: make(Addresses, 20*20)}
	s.Resize(20)
	assert.Equal(20, len(s.Txs))
	assert.Equal(20, s.Senders.Len())
	s.Resize(23)
	assert.Equal(23, len(s.Txs))
	assert.Equal(23, s.Senders.Len())

	s.Resize(2)
	assert.Equal(2, len(s.Txs))
	assert.Equal(2, s.Senders.Len())
}

func TestDedupHashes(t *testing.T) {
	assert := assert.New(t)
	h := toHashes(2, 6, 2, 5, 2, 4)
	c := h.DedupCopy()
	assert.Equal(4, c.Len())
	assert.Equal(toHashes(2, 4, 5, 6), c)
}

func toHashes(h ...byte) (out Hashes) {
	for i := range h {
		hash := [32]byte{h[i]}
		out = append(out, hash[:]...)
	}
	return out
}
