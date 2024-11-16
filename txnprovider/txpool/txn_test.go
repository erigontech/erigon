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
	bodyRlxHex := "0x04f9041701880124ec419e9796da8868b499f209983df888bb35ca86e3d9ea47882486c24309101b0e94e4ec12c49d6bcf7cc1325aa50afff92a561229fe880c716dca0e3e3d28b902b6779e563691f1ca8a86a02efdd93db261215047dad430a475d0e191f66b580d6e759a7c7a739532455e65160acf92dc1e1cc11970e7851277278e9d5d2549e451de8c8dd98ebdd3c55e73cd0b465875b72ea6d54917474f7ddfbd1f66d1a929694becc69bc3064c79c32b2db2a094844b400133724e046d9a96f2b6c7888fe008e6a667a970068487ce9a8e6c1260973956b26c1b78235f3452e21c5ed6d47507023ec4072b9ebea8ea9bde77ea64352ef7a6a8efb2ca61fbd0cf7c31491a4c38e3081dfc7b5e8066fca60d8f57b641032f23119a67a37ad0514529df22ba73b4028dc4a6aef0b26161371d731a81d8ac20ea90515b924f2534e32c240d0b75b5d1683e1bc7ecf8b82b73fb4c40d7cfc38e8c32f2c4d3424a86ba8c6e867f13328be201dd8d5e8ee47e03c1d9096968b71228b068cc21514f6bab7867a0d0a2651f40e927079b008c3ef11d571eb5f71d729ee9cfb3d2a99d258c10371fa1df271f4588e031498b155244295490fd842b3055e240ea89843a188b7f15be53252367761b9a8d21818d2c756822c0383246e167dd645722aefe4ecc5e78608bcc851dc5a51255a3f91e908bb5fa53063596458f45c6e25a712de4b2a5b36eea57f5b772c84f1d0f2f2ae103445fb7f2d38493041ca452f1e846c34331bea7b5b350d02306fa3a15b50e978b4efebccce8a3479479d51c95a08e0cab0732fc4f8095337d7502c6a962199342ed127701a6f5b0e54cbdd88f23556aab406a3a7ef49f848c3efbf4cf62052999bde1940abf4944158aefc5472f4ec9e23308cfb63deedc79e9a4f39d8b353c7e6f15d36f4c63987ae6f32701c6579e68f05f9ae86b6fbbc8d57bc17e5c2f3e5389ea75d102017767205c10d6bf5cf6e33a94ad9e6cfac5accf56d61dcee39f2e954ea89b7241e480e6021fa099a81bc9d28d6ca58a11d36f406b212be70c721bd8a4d1d643fa2bf30ebd59a4f838f794fbba2afaae8cabd778b6e151b0431e3fef0a033ce1a07081820b2a08cc2ed4355811644547f23597f7ebe516538baac51d97cbccee97f8ccf201941d994a07f0b3e925d332d4eae10c9ba474da3d8a8806320d2ae09c60e880887dbf8422d2f6549088321947f20ebcbfeff20194327d773bdc6c27cd28a533e81074372dc33a8afd884ef63dce09c5e56c8088cb702ac89cff765f88d26fe11c3d471949f20194f61ffc773a97207c8124c29526a59e6fa0b34a52880e563a787da952ab808884f2a19b171abfb2882d473907f3ada086f20194c1d608bb39e078a99086e7564e89a7625ed86dca88e8a0ab45821912e88088df6c3d43080350518895a828c35680a0278088e2487fd89ca40b3488689accdbeb8d4d2e"
	bodyRlx := hexutility.MustDecodeHex(bodyRlxHex)

	hasEnvelope := false
	ctx := NewTxParseContext(*uint256.NewInt(1))
	ctx.withSender = false

	var tx TxSlot
	txType, err := PeekTransactionType(bodyRlx)
	require.NoError(t, err)
	assert.Equal(t, SetCodeTxType, txType)

	_, err = ctx.ParseTransaction(bodyRlx, 0, &tx, nil, hasEnvelope, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(tx.Authorizations))
	assert.Equal(t, SetCodeTxType, tx.Type)

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

	// generated using this in core/types/encdec_test.go
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

			auths := tx.(*SetCodeTransaction).GetAuthorizations()
			for i := range auths {
				auths[i].ChainID.SetUint64(1)
				auths[i].V.SetUint64(uint64(randIntInRange(0, 2)))
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

// See https://github.com/ethereum/EIPs/pull/8845
func TestSetCodeTxParsingWithLargeAuthorizationValues(t *testing.T) {
	// generated using this in core/types/encdec_test.go
	/*
		func TestGenerateSetCodeTxRlpWithLargeAuthorizationValues(t *testing.T) {
			tr := NewTRand()
			var tx Transaction
			requiredAuthLen := 1
			for tx = tr.RandTransaction(); tx.Type() != types2.SetCodeTxType || len(tx.(*SetCodeTransaction).GetAuthorizations()) != requiredAuthLen; tx = tr.RandTransaction() {
			}
			v, _, _ := tx.RawSignatureValues()
			v.SetUint64(uint64(randIntInRange(0, 2)))

			tx.GetChainID().SetUint64(1)

			auths := tx.(*SetCodeTransaction).GetAuthorizations()
			for i := range auths {
				auths[i].ChainID.SetAllOne()
				auths[i].Nonce = math.MaxUint64
				auths[i].V.SetAllOne()
				auths[i].R.SetAllOne()
				auths[i].S.SetAllOne()
			}
			w := bytes.NewBuffer(nil)
			if err := tx.MarshalBinary(w); err != nil {
				t.Error(err)
			}

			hex := hexutility.Bytes(w.Bytes()).String()
			fmt.Println("tx", hex)
		}
	*/
	bodyRlxHex := "0x04f90546018820d75f30c9812472883efdaaf328683c8f88791ecd238942f142882f6a9ebaec6c1af49484d85e90da36f0392202193df1101863eb895d498883bf224e11f95355b902540e61088a043ef305b21d1bab6e4c14edeae66b3c0d6c183e423324c562b33b98ca2900d6a0adc2c13fdce4835debb623bf9f02860959bf589e88392a98a57db341a182e7d063945aaa4153010dd1d58d508d27ddd07e82b2f95af341b76b3acfbe00fde8d890867b6794ac95a4096dccbfaf9f92f446d6e4f3a718185f875ce49308ad24cf8b7316f1a73da7fb626e92e7d45d3b6ffeb2e3d153e92bc2499ccd8a43a6295db0be96dffcb857ed409b3e4f29a126e38982bf927efa9e637f670f631b36795dc158cb32069e846f28e4721b9d3004c6951075bf14f27e9f81dfa409e344e713143da8b8b35bd5ebfbc2b5c1e6909fadc7a5681ca63ecd387b745bbd3fff1169308a02e313671f2e5e5e914e8085236112fe12ae52403f54a95230c5807e5697b34bea0bf932a8cd45b6bf7ac1b318c594de85c523b1b82358ee8fce4f94d04bc30369c5a5482668a354bb36bb0f6e8943662be124cc3650f8e563a9c62808fb65444f425ff274974cd6cce771e67c2d45a95431a731ae0db60ef83793d9c257d48a24dae64e43870ecaf56a7f3e95415cdaacee3da94ad569e430ba4cdf5f0ac7cd8bd2e5507a1d0f16849e1d1823fcce1670c861b591051170c60883a32c23bc0a552415203baad7d1a6d44e3229956387ae014e944dbd7b918fa1c52fc7a4ee1ea813f1ba4a1bdedd7a23b4a93faca01eb1127d51953603eae70499d485dc3c8633b778dbae199158e03977b003a9da717ae8f31780949c53f82c5d09dd7428747426f77e39349dcd4fb8e3793d6a46cd4e9be6d6614b285d79e772bae41fb8a1625d23b38cef14911092e78c86f901f0f85994e805c10b96ec670ff8d6b684ed413eb71babf735f842a0e1b6f8621d3f4513ac7d491849fdcf3a8264e6cf4a1503357414a38682662555a027d3e5a0f5e7478629ef06e831162a8e99c4d9cd973919e21c162d29ac1410e6f87a94a5bfb70f5ffb19b2b2173e2d80a1bbfb6ff10dc4f863a0e129afdd8d7ac6e5c52708e658ded4859b06f1b483497e0474f6599b72dd2f75a0a7169872faa1fdfc3c2d80c207bc64ae817301b4087979563ef690b02b0d7964a0fe009fc656d1f0fcfc1c7c93b7ab67bdc9dd06f06a37fdaecbd43ed5d1423a11f87a94feae80b40ef29bdd926061a9000c1b0875cb0be3f863a0a5a381867886a9780e6f955d62cf73849e338918751dba1442d632fccb8d52a8a0ee8501f7bbc25d0dde1f60771167abc421be099cf0ec07636f4e5092f8f6b66ba0660a025a421dd73f4747e3a262a726ab8ff76b893c23e4d595ac50c86b9ae8c0f89b94d94f45974bbdb5d2e27419b5202fd2eec115fe26f884a04ee78854a71d2c0965a62080f2cff53b788a3dc0f8c6a55bf0535e0197a8ba77a08eb0010ea0fbe6e9251421dcbc26d83abe55bc24c6466b1833365dd87b253623a0040fa96b15c24f6a6e6dfd1e7be34b7fb58e8cfc26e4ed16b45d8eb567b60089a0cbfa955cc7c5435e0b6694e1a3d64d773a8baa3c4a85d6eddad407dbff5e645df8a4f8a2a0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff942d9986714cb0a7f215d435013a08af34c42406be88ffffffffffffffffa0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffa0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffa0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff01886417de6405dde7ee88813b5026c7af887a"
	bodyRlx := hexutility.MustDecodeHex(bodyRlxHex)

	ctx := NewTxParseContext(*uint256.NewInt(1))
	ctx.withSender = false

	var tx TxSlot
	txType, err := PeekTransactionType(bodyRlx)
	require.NoError(t, err)
	assert.Equal(t, SetCodeTxType, txType)

	_, err = ctx.ParseTransaction(bodyRlx, 0, &tx, nil, false /* hasEnvelope */, false, nil)
	assert.ErrorContains(t, err, "chainId is too big")
}

var txParseCalaverasTests = []parseTxTest{
	// Legacy protected (EIP-155) from calveras, with chainId 123
	{PayloadStr: "f86d808459682f0082520894e80d2a018c813577f33f9e69387dc621206fb3a48856bc75e2d63100008082011aa04ae3cae463329a32573f4fbf1bd9b011f93aecf80e4185add4682a03ba4a4919a02b8f05f3f4858b0da24c93c2a65e51b2fbbecf5ffdf97c1f8cc1801f307dc107",
		IdHashStr:   "f4a91979624effdb45d2ba012a7995c2652b62ebbeb08cdcab00f4923807aa8a",
		SignHashStr: "ff44cf01ee9b831f09910309a689e8da83d19aa60bad325ee9154b7c25cf4de8", Nonce: 0},
}

var txParseDevNetTests = []parseTxTest{
	{PayloadStr: "f8620101830186a09400000000000000000000000000000000000000006401820a96a04f353451b272c6b183cedf20787dab556db5afadf16733a3c6bffb0d2fcd2563a0773cd45f7cc62250f7ee715b9e19f0489176f8966f75c8eed2fbf1ac861cb50c",
		IdHashStr:   "35767571787cd95dd71e2081ac4f667076f012b854fc314ed3b131e12623cbbd",
		SignHashStr: "a2719fbc84efd65eee79201e46f5110edc4731c90ffe1cebf8644c8ddd62528d",
		SenderStr:   "67b1d87101671b127f5f8714789c7192f7ad340e", Nonce: 1},
}

var txRopstenTests = []parseTxTest{
	{PayloadStr: "f868188902000000000000000082520894000000000000000000000000000000000000004380801ba01d852f75e0bdbdf3c2b770be97eb3b74d9a3b2450fb58aa6cfbc9e9faa1c4b24a079aef959a5f032ed90b2e44b74a2e850043a3e0ab83f994ab0619638173fe035",
		Nonce: 24, SenderStr: "874b54a8bd152966d63f706bae1ffeb0411921e5", IdHashStr: "5928c90044b9d2add2fc7f580e0c6ea1b2dca2ea8c254dfa4092c251f895ed52"},
}

type parseTxTest struct {
	PayloadStr  string
	SenderStr   string
	IdHashStr   string
	SignHashStr string
	Nonce       uint64
}

var allNetsTestCases = []struct {
	tests   []parseTxTest
	chainID uint256.Int
}{
	{
		chainID: *uint256.NewInt(1),
		tests:   TxParseMainnetTests,
	},
	{
		chainID: *uint256.NewInt(123),
		tests:   txParseCalaverasTests,
	},
	{
		chainID: *uint256.NewInt(1337),
		tests:   txParseDevNetTests,
	},
	{
		chainID: *uint256.NewInt(3),
		tests:   txRopstenTests,
	},
}

var TxParseMainnetTests = []parseTxTest{
	// Legacy unprotected
	{PayloadStr: "f86a808459682f0082520894fe3b557e8fb62b89f4916b721be55ceb828dbd73872386f26fc10000801ca0d22fc3eed9b9b9dbef9eec230aa3fb849eff60356c6b34e86155dca5c03554c7a05e3903d7375337f103cb9583d97a59dcca7472908c31614ae240c6a8311b02d6",
		SenderStr: "fe3b557e8fb62b89f4916b721be55ceb828dbd73", IdHashStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		SignHashStr: "e2b043ecdbcfed773fe7b5ffc2e23ec238081c77137134a06d71eedf9cdd81d3", Nonce: 0},

	{PayloadStr: "02f86a0180843b9aca00843b9aca0082520894e80d2a018c813577f33f9e69387dc621206fb3a48080c001a02c73a04cd144e5a84ceb6da942f83763c2682896b51f7922e2e2f9a524dd90b7a0235adda5f87a1d098e2739e40e83129ff82837c9042e6ad61d0481334dcb6f1a",
		SenderStr: "81f5daee2c61807d0fc5e4c8b4e1d3c3e028d9ab", IdHashStr: "b4a8cd0b91310b0f84216c834c4cfa25a4ffc116b54692ac1f4e682be7fa73c9",
		SignHashStr: "c63673a5d989925d01a6c1339252f546e99b6957ce566a488154e169ae9bd49c", Nonce: 0},
	{PayloadStr: "01f86b01018203e882520894236ff1e97419ae93ad80cafbaa21220c5d78fb7d880de0b6b3a764000080c080a0987e3d8d0dcd86107b041e1dca2e0583118ff466ad71ad36a8465dd2a166ca2da02361c5018e63beea520321b290097cd749febc2f437c7cb41fdd085816742060",
		SenderStr: "91406aebf7370d6db8d1796bc8fe97ca4b6bed78", IdHashStr: "7edf4b1a1594b252eb80edcf51605eb1a3b17ccdf891b303a8be146269821b65",
		SignHashStr: "ae2a02407f345601507c52d1af6a89bafd4622fbcdac0001272ebca42cf7f7c2", Nonce: 1},
	{PayloadStr: "f86780862d79883d2000825208945df9b87991262f6ba471f09758cde1c0fc1de734827a69801ca088ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0a045e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
		SenderStr: "a1e4380a3b1f749673e270229993ee55f35663b4", IdHashStr: "5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
		SignHashStr: "19b1e28c14f33e74b96b88eba97d4a4fc8a97638d72e972310025b7e1189b049", Nonce: 0},
	{PayloadStr: "b903a301f9039f018218bf85105e34df0083048a949410a0847c2d170008ddca7c3a688124f49363003280b902e4c11695480000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d9439960000000000000000000000008510211a852f0c5994051dd85eaef73112a82eb5000000000000000000000000000000000000000000000000000000000000012000000000000000000000000000bad4de000000000000000000000000607816a600000000000000000000000000000000000000000000000000000000000002200000000000000000000000000000000000000000000000000000001146aa2600000000000000000000000000000000000000000000000000000000000001bc9b000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee000000000000000000000000482579f93dc13e6b434e38b5a0447ca543d88a4600000000000000000000000000000000000000000000000000000000000000c42df546f40000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d943996000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0000000000000000000000007d93f93d41604572119e4be7757a7a4a43705f080000000000000000000000000000000000000000000000003782dace9d90000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000082b5a61569b5898ac347c82a594c86699f1981aa88ca46a6a00b8e4f27b3d17bdf3714e7c0ca6a8023b37cca556602fce7dc7daac3fcee1ab04bbb3b94c10dec301cc57266db6567aa073efaa1fa6669bdc6f0877b0aeab4e33d18cb08b8877f08931abf427f11bade042177db48ca956feb114d6f5d56d1f5889047189562ec545e1c000000000000000000000000000000000000000000000000000000000000f84ff7946856ccf24beb7ed77f1f24eee5742f7ece5557e2e1a00000000000000000000000000000000000000000000000000000000000000001d694b1dd690cc9af7bb1a906a9b5a94f94191cc553cec080a0d52f3dbcad3530e73fcef6f4a75329b569a8903bf6d8109a960901f251a37af3a00ecf570e0c0ffa6efdc6e6e49be764b6a1a77e47de7bb99e167544ffbbcd65bc",
		SenderStr: "1ced2cef30d40bb3617f8d455071b69f3b12d06f", IdHashStr: "851bad0415758075a1eb86776749c829b866d43179c57c3e4a4b9359a0358231",
		SignHashStr: "894d999ea27537def37534b3d55df3fed4e1492b31e9f640774432d21cf4512c", Nonce: 6335},
	{PayloadStr: "02f8cf01038502540be40085174876e8008301869f94e77162b7d2ceb3625a4993bab557403a7b706f18865af3107a400080f85bf85994de0b295669a9fd93d5f28d9ec85e40f4cb697baef842a00000000000000000000000000000000000000000000000000000000000000003a0000000000000000000000000000000000000000000000000000000000000000780a0f73da48f3f5c9f324dfd28d106dcf911b53f33c92ae068cf6135352300e7291aa06ee83d0f59275d90000ac8cf912c6eb47261d244c9db19ffefc49e52869ff197",
		SenderStr: "e252dd9e8b19f4bcdc0f542e04e732fed2047f00", IdHashStr: "f08b9885d48a1307c5d5841fb2f92adaa35815846ac071cba65376ccfbc99c5d",
		SignHashStr: "aeda585cd1f1dc0812d7c9ef0396fe3fb9102c9b927a14724180d4616f4568f2", Nonce: 3},
	// Access list
	{PayloadStr: "01f889018201f30a8301e241808080f838f7940000000000000000000000000000000000000001e1a0000000000000000000000000000000000000000000000000000000000000000080a0a2196512ef8325b781e32d96d283a9d4cf3946947da77f3cd310eee050c537d5a00144af5513a24363bf49abed9a25476cb7c33df6e0c0053b63ee8dac64b027aa",
		SenderStr: "4d8286232b1f058d8bdb1702d0f6a1e887ced385", IdHashStr: "bde66bd7925917db9e49e38a12ed0dcd6f9422f8db90de26d34a4523f8861d1e",
		SignHashStr: "7f69febd06ddc1e72d9cd34524c82b3a8a116a02a10757be34cf536d6992d51c", Nonce: 499},
	{PayloadStr: "01f84b01018080808080c080a0382d06e968cc18373209a2532b2c9df494c36475e479020730c918b1b6f73f6ba0084b433c82339de844e2531363f59fa64218e965016cc55069828d88959b58fe", Nonce: 1},
}
