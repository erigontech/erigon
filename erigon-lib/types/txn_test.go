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

	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon-lib/common/hexutility"
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
	assert.Zero(h.Len())
	assert.Zero(c.Len())
	assert.Zero(len(h))
	assert.Zero(len(c))

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

	var thinTx TxSlot // only txn body, no blobs
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

	// Now parse the same txn body, but wrapped with blobs/commitments/proofs
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

func TestSetCodeTxParsing(t *testing.T) {
	bodyRlxHex := "0x04f902b10188a804b97e55f1619888a08ae04b7dc9b296889262bfb381a9852c88c3ed5816717427719426b97a6e638930cd51d0482d4c3908e171c848ca88e7fcd96deec354e0b8b45c84e7ba97d4d60285eebbf3f7be79aed25dfe2a9f086c69c8ae152aa777b806f45ab602f4b354a6537154e24b4f6b85b58535726876fa885dba96b202417326bb4e4ba5e0bcccd9b4e4df6096401c19e7d38df3599157249a72ac3cf095c39cfde8d4233303823f5341ccaa9ebaf78cd8dd06ec61af9924df9a2f97d13c88ae737a017c914d21d3390984a6102c51293b3b2cec8214e6be2ee033ed4795f1158d9103c9ab5f50786729dd9baf395bb20c71456cf8d5f89b94f8032107975d3fbd90ffa74185a0cb5ab43afe85f884a037e36aea1770355c584876f969c7014017aa51a5e287c9679f4402d1a878b6e3a0be3cdb54e9c5fc3032535d262e6e9ce6a092e068ad0c95f19b4022c1111652b0a0562f7754a0c1d29acfbdaad8ae779125ccc6afec0ec1177056391479b25cee72a069a8be541415e284e16be88ecdb73d5e14ae0e0ade0db635a8d717b70d98293ef794cad5e3980e2d53db6d79d5e6821cff73bef01803e1a0415787a09d11b750dfd34dfe0868ab2c7e6bd8d7ef1a66720f2ea6c7f6e9bb01f8ccf201943d8b4362475b3d0504ccd9e63cddd351b88fa052c088832c50d9581133828864e3d39680cff14988237c283c57f04c54f201940e7ceefb1855e91bd085d85852b2a9df4f9da4f0c088a87ef261eb89b837882d717143b8cb5e718854d879dc9f18304ef2019477be91ff1fb94eb618aebb4d829e9f8eeec4301bc088343c738cf5c7f5b1880521e62bff507ec288ce2f51e36cb6d54ef20194c32daf3ad4597567184d790ab83d7bf34cf0e446c088c04ac61fbe29181988f8c0d967f0799fb988772265a4be2b26ab0188c91023f53ea594d7881c46b9feb3b7cbc6"
	bodyRlx := hexutility.MustDecodeHex(bodyRlxHex)

	hasEnvelope := false
	ctx := NewTxParseContext(*uint256.NewInt(1))
	ctx.withSender = false

	var tx TxSlot
	txType, err := PeekTransactionType(bodyRlx)
	require.NoError(t, err)
	assert.Equal(t, SetCodeTxType, txType)

	_, err = ctx.ParseTransaction(bodyRlx, 0, &tx, nil, hasEnvelope, false, nil)
	assert.ErrorContains(t, err, "authorization nonce: rlp parse: uint64 must be a string, not isList")

	// test empty authorizations
	bodyRlxHex = "0x04f903420188db1b29114eba96ab887145908699cc1f3488987b96c0c55fced7886b85e4937b481442949a60a150fda306891ad7aff6d47584c8a0e1571788c5f7682286d452e0b9021164b57ad1f652639f5d44536f1b868437787082df48b3e2a684742d0eafcfda5336e7a958afb22ae57aad8e9a271528f9aa1f4a34e29491a8929732e22c04a438578b2b8510862572dd36b5304a9c3b6668b7c8f818be8411c07866ccb1fbe34586f80a1ace62753b918139acefc71f92d0c4679c0a56bb6c8ae38bc37a7ee8f348255c8ada95e842b52d4bd2b2447789a8543beda9f3bc8e27f28d51373ef9b1494c3d21adc6b0416444088ed08834eb5736d48566da000356bbcd7d78b118c39d15a56874fd254dcfcc172cd7a82e36621b964ebc54fdaa64de9e381b1545cfc7c4ea1cfccff829f0dfa395ef5f750b79689e5c8e3f6c7de9afe34d05f599dac8e3ae999f7acb32f788991425a7d8b36bf92a7dc14d913c3cc5854e580f48d507bf06018f3d012155791e1930791afccefe46f268b59e023ddacaf1e8278026a4c962f9b968f065e7c33d98d2aea49e8885ac77bfcc952e322c5e414cb5b4e7477829c0a4b8b0964fc28d202bca1b3bedca34f3fe12d62629b30a4764121440d0ea0f50f26579c486070d00309a44c14f6c3347c5d14b520eca8a399a1cd3c421f28ae5485e96b4c500a411754a78f558701d1a9788d22e6d2f02fefd1c45c2d427b518adda66a34432c3f94b4b3811e2d063dca2917f403033b0400e4e9dc3fd327b10a43a15229332596671d0392e501c39f43b23f814e95b093e981418091f9e2a32013ab8fa7a409d5636b52fded6f8d5f794de688ae4be9a54b20eb5366903223863de2bc895e1a0f5ecb3956919b8e9e9956c20c89b523e71c5803592c99871b7d5ee025e402941f89b94ae16863cc3bf6e6946f186d0f63d77343f81363ef884a0b09afe54c0376e3e3091473edb4e2bf43f08530356a2c9236bf373869b79c8d0a0ec2c57ca577173865f340a7cd13cf0051e52229722e3a529f851d4b74e315c8ca00bbe5f1a1ef2e5830d0c5cb8e93a05d4d29b4d7bf244ceea432888c4fbd5d5d5a0823b7ceaeba3a4cd70572c2ccc560d588ffeed638aec7c0cc364afa7dbf1c51cc0018818848492f65ca7bd88ea2017dc526fff7f"
	bodyRlx = hexutility.MustDecodeHex(bodyRlxHex)
	ctx = NewTxParseContext(*uint256.NewInt(1))
	ctx.withSender = false
	var tx2 TxSlot

	txType, err = PeekTransactionType(bodyRlx)
	require.NoError(t, err)
	assert.Equal(t, SetCodeTxType, txType)

	_, err = ctx.ParseTransaction(bodyRlx, 0, &tx2, nil, hasEnvelope, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(tx2.Authorizations))
	assert.Equal(t, SetCodeTxType, tx2.Type)

	// generated using this in encdec_test.go
	/*
		func TestGenerateSetCodeTxRlp(t *testing.T) {
			tr := NewTRand()
			var tx Transaction
			requiredAuthLen := 0
			for tx = tr.RandTransaction(); tx.Type() != types2.SetCodeTxType || len(tx.(*SetCodeTransaction).GetAuthorizations()) != requiredAuthLen; tx = tr.RandTransaction() {
			}
			v, _, _ := tx.RawSignatureValues()
			v.SetUint64(uint64(randIntInRange(0, 2)))

			tx.GetChainID().SetUint64(1)

			for _, auth := range tx.(*SetCodeTransaction).GetAuthorizations() {
				auth.ChainID.SetUint64(1)
				auth.V.SetUint64(uint64(randIntInRange(0, 2)))
			}
			w := bytes.NewBuffer(nil)
			if err := tx.MarshalBinary(w); err != nil {
				t.Error(err)
			}

			hex := hexutility.Bytes(w.Bytes()).String()
			//hex := libcommon.BytesToHash().Hex()
			authj, err := json.Marshal(tx.(*SetCodeTransaction).GetAuthorizations())
			if err != nil {
				t.Error(err)
			}
			fmt.Println("tx", hex, len(tx.(*SetCodeTransaction).GetAuthorizations()), string(authj))
		}
	*/
}
