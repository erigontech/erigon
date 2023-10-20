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
	"bytes"
	"crypto/rand"
	"strconv"
	"testing"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
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
					payload := hexutility.MustDecodeHex(tt.PayloadStr)
					parseEnd, err := ctx.ParseTransaction(payload, 0, tx, txSender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
					require.NoError(err)
					require.Equal(len(payload), parseEnd)
					if tt.SignHashStr != "" {
						signHash := hexutility.MustDecodeHex(tt.SignHashStr)
						if !bytes.Equal(signHash, ctx.Sighash[:]) {
							t.Errorf("signHash expected %x, got %x", signHash, ctx.Sighash)
						}
					}
					if tt.IdHashStr != "" {
						idHash := hexutility.MustDecodeHex(tt.IdHashStr)
						if !bytes.Equal(idHash, tx.IDHash[:]) {
							t.Errorf("IdHash expected %x, got %x", idHash, tx.IDHash)
						}
					}
					if tt.SenderStr != "" {
						expectSender := hexutility.MustDecodeHex(tt.SenderStr)
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

func TestTransactionSignatureValidity1(t *testing.T) {
	chainId := new(uint256.Int).SetUint64(1)
	ctx := NewTxParseContext(*chainId)
	ctx.WithAllowPreEip2s(true)

	tx, txSender := &TxSlot{}, [20]byte{}
	validTxn := hexutility.MustDecodeHex("f83f800182520894095e7baea6a6c7c4c2dfeb977efac326af552d870b801ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a3664935301")
	_, err := ctx.ParseTransaction(validTxn, 0, tx, txSender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.NoError(t, err)

	preEip2Txn := hexutility.MustDecodeHex("f85f800182520894095e7baea6a6c7c4c2dfeb977efac326af552d870b801ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a07fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a1")
	_, err = ctx.ParseTransaction(preEip2Txn, 0, tx, txSender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.NoError(t, err)

	// Now enforce EIP-2
	ctx.WithAllowPreEip2s(false)
	_, err = ctx.ParseTransaction(validTxn, 0, tx, txSender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.NoError(t, err)

	_, err = ctx.ParseTransaction(preEip2Txn, 0, tx, txSender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.Error(t, err)
}

// Problematic txn included in a bad block on Görli
func TestTransactionSignatureValidity2(t *testing.T) {
	chainId := new(uint256.Int).SetUint64(5)
	ctx := NewTxParseContext(*chainId)
	slot, sender := &TxSlot{}, [20]byte{}
	rlp := hexutility.MustDecodeHex("02f8720513844190ab00848321560082520894cab441d2f45a3fee83d15c6b6b6c36a139f55b6288054607fc96a6000080c001a0dffe4cb5651e663d0eac8c4d002de734dd24db0f1109b062d17da290a133cc02a0913fb9f53f7a792bcd9e4d7cced1b8545d1ab82c77432b0bc2e9384ba6c250c5")
	_, err := ctx.ParseTransaction(rlp, 0, slot, sender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.Error(t, err)

	// Only legacy transactions can happen before EIP-2
	ctx.WithAllowPreEip2s(true)
	_, err = ctx.ParseTransaction(rlp, 0, slot, sender[:], false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	assert.Error(t, err)
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
	assert.Equal(6, h.Len())
	assert.Equal(4, c.Len())
	assert.Equal(toHashes(2, 2, 2, 4, 5, 6), h)
	assert.Equal(toHashes(2, 4, 5, 6), c)

	h = toHashes(2, 2)
	c = h.DedupCopy()
	assert.Equal(toHashes(2, 2), h)
	assert.Equal(toHashes(2), c)

	h = toHashes(1)
	c = h.DedupCopy()
	assert.Equal(1, h.Len())
	assert.Equal(1, c.Len())
	assert.Equal(toHashes(1), h)
	assert.Equal(toHashes(1), c)

	h = toHashes()
	c = h.DedupCopy()
	assert.Equal(0, h.Len())
	assert.Equal(0, c.Len())
	assert.Equal(0, len(h))
	assert.Equal(0, len(c))

	h = toHashes(1, 2, 3, 4)
	c = h.DedupCopy()
	assert.Equal(toHashes(1, 2, 3, 4), h)
	assert.Equal(toHashes(1, 2, 3, 4), c)

	h = toHashes(4, 2, 1, 3)
	c = h.DedupCopy()
	assert.Equal(toHashes(1, 2, 3, 4), h)
	assert.Equal(toHashes(1, 2, 3, 4), c)

}

func toHashes(h ...byte) (out Hashes) {
	for i := range h {
		hash := [32]byte{h[i]}
		out = append(out, hash[:]...)
	}
	return out
}

func TestBlobTxParsing(t *testing.T) {
	// First parse a blob transaction body (no blobs/commitments/proofs)
	wrappedWithBlobs := false
	// Some arbitrary hardcoded example
	bodyRlpHex := "f9012705078502540be4008506fc23ac008357b58494811a752c8cd697e3cb27" +
		"279c330ed1ada745a8d7808204f7f872f85994de0b295669a9fd93d5f28d9ec85e40f4cb697b" +
		"aef842a00000000000000000000000000000000000000000000000000000000000000003a000" +
		"00000000000000000000000000000000000000000000000000000000000007d694bb9bc244d7" +
		"98123fde783fcc1c72d3bb8c189413c07bf842a0c6bdd1de713471bd6cfa62dd8b5a5b42969e" +
		"d09e26212d3377f3f8426d8ec210a08aaeccaf3873d07cef005aca28c39f8a9f8bdb1ec8d79f" +
		"fc25afc0a4fa2ab73601a036b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc09035" +
		"90c16b02b0a05edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094"
	bodyRlp := hexutility.MustDecodeHex(bodyRlpHex)

	hasEnvelope := true
	bodyEnvelopePrefix := hexutility.MustDecodeHex("b9012b")
	var bodyEnvelope []byte
	bodyEnvelope = append(bodyEnvelope, bodyEnvelopePrefix...)
	bodyEnvelope = append(bodyEnvelope, BlobTxType)
	bodyEnvelope = append(bodyEnvelope, bodyRlp...)

	ctx := NewTxParseContext(*uint256.NewInt(5))
	ctx.withSender = false

	var thinTx TxSlot // only tx body, no blobs
	txType, err := PeekTransactionType(bodyEnvelope)
	require.NoError(t, err)
	assert.Equal(t, BlobTxType, txType)

	p, err := ctx.ParseTransaction(bodyEnvelope, 0, &thinTx, nil, hasEnvelope, wrappedWithBlobs, nil)
	require.NoError(t, err)
	assert.Equal(t, len(bodyEnvelope), p)
	assert.Equal(t, len(bodyEnvelope)-len(bodyEnvelopePrefix), int(thinTx.Size))
	assert.Equal(t, bodyEnvelope[3:], thinTx.Rlp)
	assert.Equal(t, BlobTxType, thinTx.Type)
	assert.Equal(t, 2, len(thinTx.BlobHashes))
	assert.Equal(t, 0, len(thinTx.Blobs))
	assert.Equal(t, 0, len(thinTx.Commitments))
	assert.Equal(t, 0, len(thinTx.Proofs))

	// Now parse the same tx body, but wrapped with blobs/commitments/proofs
	wrappedWithBlobs = true
	hasEnvelope = false

	blobsRlpPrefix := hexutility.MustDecodeHex("fa040008")
	blobRlpPrefix := hexutility.MustDecodeHex("ba020000")
	blob0 := make([]byte, fixedgas.BlobSize)
	rand.Read(blob0)
	blob1 := make([]byte, fixedgas.BlobSize)
	rand.Read(blob1)

	proofsRlpPrefix := hexutility.MustDecodeHex("f862")
	var commitment0, commitment1 gokzg4844.KZGCommitment
	rand.Read(commitment0[:])
	rand.Read(commitment1[:])
	var proof0, proof1 gokzg4844.KZGProof
	rand.Read(proof0[:])
	rand.Read(proof1[:])

	wrapperRlp := hexutility.MustDecodeHex("03fa0401fe")
	wrapperRlp = append(wrapperRlp, bodyRlp...)
	wrapperRlp = append(wrapperRlp, blobsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob0...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob1...)
	wrapperRlp = append(wrapperRlp, proofsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment0[:]...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment1[:]...)
	wrapperRlp = append(wrapperRlp, proofsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, proof0[:]...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, proof1[:]...)

	var fatTx TxSlot // with blobs/commitments/proofs
	txType, err = PeekTransactionType(wrapperRlp)
	require.NoError(t, err)
	assert.Equal(t, BlobTxType, txType)

	p, err = ctx.ParseTransaction(wrapperRlp, 0, &fatTx, nil, hasEnvelope, wrappedWithBlobs, nil)
	require.NoError(t, err)
	assert.Equal(t, len(wrapperRlp), p)
	assert.Equal(t, len(wrapperRlp), int(fatTx.Size))
	assert.Equal(t, wrapperRlp, fatTx.Rlp)
	assert.Equal(t, BlobTxType, fatTx.Type)

	assert.Equal(t, thinTx.Value, fatTx.Value)
	assert.Equal(t, thinTx.Tip, fatTx.Tip)
	assert.Equal(t, thinTx.FeeCap, fatTx.FeeCap)
	assert.Equal(t, thinTx.Nonce, fatTx.Nonce)
	assert.Equal(t, thinTx.DataLen, fatTx.DataLen)
	assert.Equal(t, thinTx.DataNonZeroLen, fatTx.DataNonZeroLen)
	assert.Equal(t, thinTx.AlAddrCount, fatTx.AlAddrCount)
	assert.Equal(t, thinTx.AlStorCount, fatTx.AlStorCount)
	assert.Equal(t, thinTx.Gas, fatTx.Gas)
	assert.Equal(t, thinTx.IDHash, fatTx.IDHash)
	assert.Equal(t, thinTx.Creation, fatTx.Creation)
	assert.Equal(t, thinTx.BlobFeeCap, fatTx.BlobFeeCap)
	assert.Equal(t, thinTx.BlobHashes, fatTx.BlobHashes)

	require.Equal(t, 2, len(fatTx.Blobs))
	require.Equal(t, 2, len(fatTx.Commitments))
	require.Equal(t, 2, len(fatTx.Proofs))
	assert.Equal(t, blob0, fatTx.Blobs[0])
	assert.Equal(t, blob1, fatTx.Blobs[1])
	assert.Equal(t, commitment0, fatTx.Commitments[0])
	assert.Equal(t, commitment1, fatTx.Commitments[1])
	assert.Equal(t, proof0, fatTx.Proofs[0])
	assert.Equal(t, proof1, fatTx.Proofs[1])
}
