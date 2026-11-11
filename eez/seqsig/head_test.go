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

package seqsig_test

import (
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// Known answers for development key #0 signing block 7 (parent 0x11…11, every
// other header field at its zero or empty-trie value, empty body) as head
// counter 1 of epoch 1,700,000,000,000, rollup 1, on L2 chain 1337 with
// registry 0x…aa. Derived independently with Foundry cast 1.8.5:
//   - type, name and version hashes: cast keccak '<string>';
//   - separator: cast keccak $(cast abi-encode 'f(bytes32,bytes32,bytes32,uint256,address)' …);
//   - struct hash: cast keccak $(cast abi-encode 'f(bytes32,uint64,uint64,bytes32,bytes32,uint64,uint64)' …);
//   - digest: cast keccak 0x1901‖separator‖structHash;
//   - signature: cast wallet sign --no-hash, equal to cast wallet sign --data
//     over the EIP-712 typed data;
//   - header, block and envelope RLP: cast to-rlp, hashed with cast keccak.
const (
	keyHex         = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	signerHex      = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	domainTypeHex  = "0x8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39400f"
	headTypeHex    = "0x92ace2e10d606aabd1dbb446b77fb1b2c97c195ad066f0600490e54048d98051"
	separatorHex   = "0x547edc5757c989326f4f3117f20ad6423a35b3e39db146fff9153d6bba6f862f"
	structHashHex  = "0x2ef6bfaf71f84b7a26247252d803c1663ce1f37d3b5628442982602e5c4ac17c"
	digestHex      = "0x737ba12bb36dd646b65142c155cb826cf4c52d48d572f33803b53f93f71e693a"
	signatureHex   = "0x7b5b31c607467a2dd273bfd2cc34506477fa1da5f5bb8b0ec77c94855196d08e0a6bb082f1d409a8a8cb32b02f0eb4e2fdb53c12abf7a2bbfa50fa72f59493ea1c"
	blockHashHex   = "0x0c467060700fd4695ec429a3f1eff9670ac6098f428f1552ec22ded432f3767b"
	block8HashHex  = "0x2540d025c318ead873f72f7b9d8c46ca6d3e40007404e19ee97cd9d22e8ff4eb"
	blockRLPKeccak = "0x89bc42802b81ccd8acbc71846a94598736531a9b4678103de6449afa67522b07"
	epochMs        = 1_700_000_000_000
)

var (
	domain = seqsig.Domain{ChainID: 1337, Registry: common.HexToAddress("0xaa"), RollupID: 1}
	parent = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
)

func key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.HexToECDSA(keyHex)
	require.NoError(t, err)
	return k
}

// testBlock returns block number on parent with an empty body.
func testBlock(number uint64) *types.Block {
	return types.NewBlock(&types.Header{
		ParentHash: parent,
		Root:       empty.RootHash,
		Number:     *uint256.NewInt(number),
	}, nil, nil, nil, nil, nil)
}

func encodeBlock(t *testing.T, b *types.Block) []byte {
	t.Helper()
	enc, err := rlp.EncodeToBytes(b)
	require.NoError(t, err)
	return enc
}

// headOf is the head the sequencer signs for b as counter 1.
func headOf(b *types.Block) seqsig.Head {
	return seqsig.Head{
		RollupID:   domain.RollupID,
		Number:     b.NumberU64(),
		BlockHash:  b.Hash(),
		ParentHash: b.ParentHash(),
		EpochMs:    epochMs,
		Counter:    1,
	}
}

func signed(t *testing.T, h seqsig.Head, block []byte) seqsig.Envelope {
	t.Helper()
	sig, err := seqsig.Sign(h, domain, key(t))
	require.NoError(t, err)
	return seqsig.Envelope{Head: h, Signature: sig, Block: block}
}

func TestTypeHashes(t *testing.T) {
	require.Equal(t, common.HexToHash(domainTypeHex), seqsig.DomainTypeHash)
	require.Equal(t, common.HexToHash(headTypeHex), seqsig.HeadTypeHash)
}

func TestSigningHashKnownAnswers(t *testing.T) {
	b := testBlock(7)
	require.Equal(t, common.HexToHash(blockHashHex), b.Hash())
	require.Equal(t, common.HexToHash(block8HashHex), testBlock(8).Hash())
	require.Equal(t, common.HexToHash(blockRLPKeccak), crypto.Keccak256Hash(encodeBlock(t, b)))

	h := headOf(b)
	require.Equal(t, common.HexToHash(separatorHex), domain.Separator())
	require.Equal(t, common.HexToHash(structHashHex), h.StructHash())
	require.Equal(t, common.HexToHash(digestHex), h.SigningHash(domain))

	sig, err := seqsig.Sign(h, domain, key(t))
	require.NoError(t, err)
	require.Equal(t, common.FromHex(signatureHex), sig)
	require.Equal(t, common.HexToAddress(signerHex), crypto.PubkeyToAddress(key(t).PublicKey))
}

func TestSigningHashBindsEveryField(t *testing.T) {
	base := headOf(testBlock(7))
	other := common.HexToHash("0x22")
	for name, tamper := range map[string]func(*seqsig.Head){
		"rollupId":   func(h *seqsig.Head) { h.RollupID++ },
		"number":     func(h *seqsig.Head) { h.Number++ },
		"blockHash":  func(h *seqsig.Head) { h.BlockHash = other },
		"parentHash": func(h *seqsig.Head) { h.ParentHash = other },
		"epochMs":    func(h *seqsig.Head) { h.EpochMs++ },
		"counter":    func(h *seqsig.Head) { h.Counter++ },
	} {
		h := base
		tamper(&h)
		require.NotEqual(t, base.SigningHash(domain), h.SigningHash(domain), name)
	}
	for name, d := range map[string]seqsig.Domain{
		"chainId":  {ChainID: domain.ChainID + 1, Registry: domain.Registry, RollupID: domain.RollupID},
		"registry": {ChainID: domain.ChainID, Registry: common.HexToAddress("0xab"), RollupID: domain.RollupID},
	} {
		require.NotEqual(t, base.SigningHash(domain), base.SigningHash(d), name)
	}
}

func TestVersionOrder(t *testing.T) {
	for _, c := range []struct {
		a, b seqsig.Version
		want int
	}{
		{seqsig.Version{EpochMs: 100, Counter: 1}, seqsig.Version{EpochMs: 100, Counter: 2}, -1},
		{seqsig.Version{EpochMs: 100, Counter: 2}, seqsig.Version{EpochMs: 100, Counter: 2}, 0},
		{seqsig.Version{EpochMs: 200, Counter: 1}, seqsig.Version{EpochMs: 100, Counter: 10_000}, 1},
		{seqsig.Version{EpochMs: 100, Counter: 10_000}, seqsig.Version{EpochMs: 200, Counter: 1}, -1},
	} {
		require.Equal(t, c.want, c.a.Compare(c.b), "%v vs %v", c.a, c.b)
	}
}
