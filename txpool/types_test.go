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
	"bytes"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parseTxTest struct {
	payloadStr  string
	senderStr   string
	idHashStr   string
	signHashStr string
	nonce       uint64
}

var txParseMainnetTests = []parseTxTest{
	// Legacy unprotected
	{payloadStr: "f86a808459682f0082520894fe3b557e8fb62b89f4916b721be55ceb828dbd73872386f26fc10000801ca0d22fc3eed9b9b9dbef9eec230aa3fb849eff60356c6b34e86155dca5c03554c7a05e3903d7375337f103cb9583d97a59dcca7472908c31614ae240c6a8311b02d6",
		senderStr: "fe3b557e8fb62b89f4916b721be55ceb828dbd73", idHashStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		signHashStr: "e2b043ecdbcfed773fe7b5ffc2e23ec238081c77137134a06d71eedf9cdd81d3", nonce: 0},

	{payloadStr: "b86d02f86a7b80843b9aca00843b9aca0082520894e80d2a018c813577f33f9e69387dc621206fb3a48080c001a02c73a04cd144e5a84ceb6da942f83763c2682896b51f7922e2e2f9a524dd90b7a0235adda5f87a1d098e2739e40e83129ff82837c9042e6ad61d0481334dcb6f1a",
		senderStr: "e80d2a018c813577f33f9e69387dc621206fb3a4", idHashStr: "1247438da30b5919f1401eff4422fd11added646eff41278cd5276a5d3df802e",
		signHashStr: "34ef1790ebd860a84c73ba27576ae96621ec21e96f70935c94e8e24dc1b62f2b", nonce: 0},
	{payloadStr: "b86e01f86b7b018203e882520894236ff1e97419ae93ad80cafbaa21220c5d78fb7d880de0b6b3a764000080c080a0987e3d8d0dcd86107b041e1dca2e0583118ff466ad71ad36a8465dd2a166ca2da02361c5018e63beea520321b290097cd749febc2f437c7cb41fdd085816742060",
		senderStr: "4774e55994fce67b26c94716612c7048dcbf2dcd", idHashStr: "dec28fbfd19eb82ba91437922ea91d550d2861efb8cc7a4040b0f5efd3658284",
		signHashStr: "1ee032826e5aa14bc7353bf9f3af8683fd9f657a779879ff562ddcab0ecda30a", nonce: 1},
	{payloadStr: "f86780862d79883d2000825208945df9b87991262f6ba471f09758cde1c0fc1de734827a69801ca088ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0a045e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
		senderStr: "a1e4380a3b1f749673e270229993ee55f35663b4", idHashStr: "5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
		signHashStr: "19b1e28c14f33e74b96b88eba97d4a4fc8a97638d72e972310025b7e1189b049", nonce: 0},
	{payloadStr: "b903a301f9039f018218bf85105e34df0083048a949410a0847c2d170008ddca7c3a688124f49363003280b902e4c11695480000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d9439960000000000000000000000008510211a852f0c5994051dd85eaef73112a82eb5000000000000000000000000000000000000000000000000000000000000012000000000000000000000000000bad4de000000000000000000000000607816a600000000000000000000000000000000000000000000000000000000000002200000000000000000000000000000000000000000000000000000001146aa2600000000000000000000000000000000000000000000000000000000000001bc9b000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee000000000000000000000000482579f93dc13e6b434e38b5a0447ca543d88a4600000000000000000000000000000000000000000000000000000000000000c42df546f40000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d943996000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0000000000000000000000007d93f93d41604572119e4be7757a7a4a43705f080000000000000000000000000000000000000000000000003782dace9d90000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000082b5a61569b5898ac347c82a594c86699f1981aa88ca46a6a00b8e4f27b3d17bdf3714e7c0ca6a8023b37cca556602fce7dc7daac3fcee1ab04bbb3b94c10dec301cc57266db6567aa073efaa1fa6669bdc6f0877b0aeab4e33d18cb08b8877f08931abf427f11bade042177db48ca956feb114d6f5d56d1f5889047189562ec545e1c000000000000000000000000000000000000000000000000000000000000f84ff7946856ccf24beb7ed77f1f24eee5742f7ece5557e2e1a00000000000000000000000000000000000000000000000000000000000000001d694b1dd690cc9af7bb1a906a9b5a94f94191cc553cec080a0d52f3dbcad3530e73fcef6f4a75329b569a8903bf6d8109a960901f251a37af3a00ecf570e0c0ffa6efdc6e6e49be764b6a1a77e47de7bb99e167544ffbbcd65bc",
		senderStr: "1ced2cef30d40bb3617f8d455071b69f3b12d06f", idHashStr: "851bad0415758075a1eb86776749c829b866d43179c57c3e4a4b9359a0358231",
		signHashStr: "894d999ea27537def37534b3d55df3fed4e1492b31e9f640774432d21cf4512c", nonce: 6335},
	{payloadStr: "b8d202f8cf7b038502540be40085174876e8008301869f94e77162b7d2ceb3625a4993bab557403a7b706f18865af3107a400080f85bf85994de0b295669a9fd93d5f28d9ec85e40f4cb697baef842a00000000000000000000000000000000000000000000000000000000000000003a0000000000000000000000000000000000000000000000000000000000000000780a0f73da48f3f5c9f324dfd28d106dcf911b53f33c92ae068cf6135352300e7291aa06ee83d0f59275d90000ac8cf912c6eb47261d244c9db19ffefc49e52869ff197",
		senderStr: "0961ca10d49b9b8e371aa0bcf77fe5730b18f2e4", idHashStr: "27db095399b22dc311aaab4d9ed45195873fdff1288fdc7c4e6dc1bfb17c061a",
		signHashStr: "35fbc0cd33a181e62b7432338f172106886a1396e1e3647ddf1e756740d81ae1", nonce: 3},
	// Access list
	{payloadStr: "01f889018201f30a8301e241808080f838f7940000000000000000000000000000000000000001e1a0000000000000000000000000000000000000000000000000000000000000000080a0a2196512ef8325b781e32d96d283a9d4cf3946947da77f3cd310eee050c537d5a00144af5513a24363bf49abed9a25476cb7c33df6e0c0053b63ee8dac64b027aa",
		senderStr: "4d8286232b1f058d8bdb1702d0f6a1e887ced385", idHashStr: "bde66bd7925917db9e49e38a12ed0dcd6f9422f8db90de26d34a4523f8861d1e",
		signHashStr: "7f69febd06ddc1e72d9cd34524c82b3a8a116a02a10757be34cf536d6992d51c", nonce: 499},
}

var txParseCalaverasTests = []parseTxTest{
	// Legacy protected (EIP-155) from calveras, with chainId 123
	{payloadStr: "f86d808459682f0082520894e80d2a018c813577f33f9e69387dc621206fb3a48856bc75e2d63100008082011aa04ae3cae463329a32573f4fbf1bd9b011f93aecf80e4185add4682a03ba4a4919a02b8f05f3f4858b0da24c93c2a65e51b2fbbecf5ffdf97c1f8cc1801f307dc107",
		idHashStr:   "f4a91979624effdb45d2ba012a7995c2652b62ebbeb08cdcab00f4923807aa8a",
		signHashStr: "ff44cf01ee9b831f09910309a689e8da83d19aa60bad325ee9154b7c25cf4de8", nonce: 0},
}

var txParseDevNetTests = []parseTxTest{
	{payloadStr: "f8620101830186a09400000000000000000000000000000000000000006401820a96a04f353451b272c6b183cedf20787dab556db5afadf16733a3c6bffb0d2fcd2563a0773cd45f7cc62250f7ee715b9e19f0489176f8966f75c8eed2fbf1ac861cb50c",
		idHashStr:   "35767571787cd95dd71e2081ac4f667076f012b854fc314ed3b131e12623cbbd",
		signHashStr: "a2719fbc84efd65eee79201e46f5110edc4731c90ffe1cebf8644c8ddd62528d",
		senderStr:   "67b1d87101671b127f5f8714789c7192f7ad340e", nonce: 1},
}

var allNetsTestCases = []struct {
	chainID uint256.Int
	tests   []parseTxTest
}{
	{
		chainID: *uint256.NewInt(1),
		tests:   txParseMainnetTests,
	},
	{
		chainID: *uint256.NewInt(123),
		tests:   txParseCalaverasTests,
	},
	{
		chainID: *uint256.NewInt(1337),
		tests:   txParseDevNetTests,
	},
}

func TestParseTransactionRLP(t *testing.T) {
	for _, testSet := range allNetsTestCases {
		t.Run(strconv.Itoa(int(testSet.chainID.Uint64())), func(t *testing.T) {
			ctx := NewTxParseContext(testSet.chainID)
			require := require.New(t)

			tx, txSender := &TxSlot{}, [20]byte{}
			for i, tt := range testSet.tests {
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					payload := decodeHex(tt.payloadStr)
					parseEnd, err := ctx.ParseTransaction(payload, 0, tx, txSender[:])
					require.NoError(err)
					require.Equal(len(payload), parseEnd)
					if tt.signHashStr != "" {
						signHash := decodeHex(tt.signHashStr)
						if !bytes.Equal(signHash, ctx.sighash[:]) {
							t.Errorf("signHash expected %x, got %x", signHash, ctx.sighash)
						}
					}
					if tt.idHashStr != "" {
						idHash := decodeHex(tt.idHashStr)
						if !bytes.Equal(idHash, tx.idHash[:]) {
							t.Errorf("idHash expected %x, got %x", idHash, tx.idHash)
						}
					}
					if tt.senderStr != "" {
						expectSender := decodeHex(tt.senderStr)
						if !bytes.Equal(expectSender, txSender[:]) {
							t.Errorf("expectSender expected %x, got %x", expectSender, txSender)
						}
					}
					require.Equal(tt.nonce, tx.nonce)
				})
			}
		})
	}
}
func TestTxSlotsGrowth(t *testing.T) {
	assert := assert.New(t)
	s := &TxSlots{}
	s.Resize(11)
	assert.Equal(11, len(s.txs))
	assert.Equal(11, s.senders.Len())
	s.Resize(23)
	assert.Equal(23, len(s.txs))
	assert.Equal(23, s.senders.Len())

	s = &TxSlots{txs: make([]*TxSlot, 20), senders: make(Addresses, 20*20)}
	s.Resize(20)
	assert.Equal(20, len(s.txs))
	assert.Equal(20, s.senders.Len())
	s.Resize(23)
	assert.Equal(23, len(s.txs))
	assert.Equal(23, s.senders.Len())

	s.Resize(2)
	assert.Equal(2, len(s.txs))
	assert.Equal(2, s.senders.Len())
}
