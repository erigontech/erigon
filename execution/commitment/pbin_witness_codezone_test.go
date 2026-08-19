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

package commitment

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// Deploying a contract writes leaves into the content-addressed code zone.
// When another contract's chunks are already there, the new leaves split an
// existing subtree, and the witness has to carry the node they split.

// pbinSpillingCode returns code of chunkCount chunks, distinct per seed so two
// accounts land on different code-zone stems.
func pbinSpillingCode(seed byte, chunkCount int) []byte {
	code := bytes.Repeat([]byte{0x01}, 31*chunkCount)
	code[0] = seed
	return code
}

// pbinDeployCorpus is one account deploying code, the shape a create block has:
// the account's leaves and its chunks all arrive at once.
func pbinDeployCorpus(addrSeed uint64, code []byte) *pbinTestCorpus {
	c := new(pbinTestCorpus)
	return c.accountWithCodeBytes(pbinOracleAddr(addrSeed), 1, 1, code)
}

// pbinStreamKeys runs the update stream over state and returns the tree keys it
// expands to, in emission order.
func pbinStreamKeys(t *testing.T, state PatriciaContext, c *pbinTestCorpus, block PBinWitnessBlock, witness bool) []string {
	t.Helper()
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), c.plainKeys, c.updates)
	s := &pbinUpdateStream{witness: block, witnessPass: witness}
	var keys []string
	_, err := s.process(context.Background(), upd, state, func(treeKey, _ []byte, _ *Update) error {
		keys = append(keys, hex.EncodeToString(treeKey))
		return nil
	})
	require.NoError(t, err)
	return keys
}

// TestPBinWitnessCodeOverrideMatchesFoldKeys is the property the override exists
// for: a witness pass reading the parent state has to expand an account to the
// same tree keys the fold did against the state the block leaves behind. Without
// the override the parent has no code for a contract the block creates, and the
// chunk keys go missing.
func TestPBinWitnessCodeOverrideMatchesFoldKeys(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(72)
	code := pbinSpillingCode(0xC0, 136)
	deploy := pbinDeployCorpus(72, code)

	post := NewMockState(t)
	require.NoError(t, post.applyPlainUpdates(deploy.plainKeys, deploy.updates))
	post.setCode(addr, code)
	fold := pbinStreamKeys(t, post, deploy, PBinWitnessBlock{}, false)

	parent := NewMockState(t)
	witness := pbinStreamKeys(t, parent, deploy, PBinWitnessBlock{Code: map[string][]byte{string(addr): code}}, true)

	require.Equal(t, fold, witness)
	require.Len(t, fold, 3+136, "three header keys and one key per chunk")

	// Without it the parent state yields the account's header keys only.
	require.Len(t, pbinStreamKeys(t, parent, deploy, PBinWitnessBlock{}, true), 3)
}

// pbinWitnessStateFor commits the corpus, proves a touch of addr and decodes the
// pruned witness back into a readable state.
func pbinWitnessStateFor(t *testing.T, corpus *pbinTestCorpus, addr []byte) (*PBinWitnessState, [][]byte, []byte) {
	t.Helper()
	ms, parentRoot := pbinWitnessCommitted(t, corpus)

	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), [][]byte{addr}, []Update{{}})
	nodes, provedKeys, root := pbinWitnessesOf(t, ms, upd, false)
	require.Equal(t, parentRoot, root)

	lean, err := PBinWitnessNodesForKeys(nodes, root, provedKeys)
	require.NoError(t, err)
	state, err := PBinNewWitnessState(lean, root)
	require.NoError(t, err)
	return state, lean, root
}

// TestPBinWitnessDelegatedAccountIsPresent: a delegated account holds no
// CODE_HASH leaf, so the delegation leaf has to mark it present, with the code
// hash EXTCODEHASH defines — the keccak of the indicator bytes.
func TestPBinWitnessDelegatedAccountIsPresent(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(75)
	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0x22}, 20)...)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 3, 700, indicator)

	state, _, _ := pbinWitnessStateFor(t, corpus, addr)

	acc, ok, err := state.Account(addr)
	require.NoError(t, err)
	require.True(t, ok, "the delegation leaf marks the account present")
	require.Equal(t, uint64(3), acc.Nonce)
	require.Equal(t, uint64(700), acc.Balance.Uint64())
	require.Equal(t, uint64(pbinDelegationCodeLength), acc.CodeSize)
	require.Equal(t, common.Hash(keccak.Sum256(indicator)), acc.CodeHash)
}

// TestPBinWitnessDelegatedAccountCarriesNoChunks: the indicator is the code, read
// straight from the header leaf — the witness holds no code-zone leaf for it.
func TestPBinWitnessDelegatedAccountCarriesNoChunks(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(76)
	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0x33}, 20)...)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 5, indicator)

	state, lean, root := pbinWitnessStateFor(t, corpus, addr)

	code, ok, err := state.Code(addr)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, indicator, code, "the code is the leading code_size bytes of the delegation leaf")

	tree, err := pbinDecodeWitness(lean, root)
	require.NoError(t, err)
	for _, node := range tree.nodes {
		if node.isLeaf() {
			require.NotEqual(t, byte(pbinCodeZone), node.key[0], "a delegated account owns no code-zone leaf")
		}
	}
}

// TestPBinWitnessReassemblesCodeAcrossGroups: chunk 256 lives under tree_index 1,
// a different code-zone stem than chunks 0-255. The read has to cross that group
// boundary and come back byte-for-byte.
func TestPBinWitnessReassemblesCodeAcrossGroups(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(77)
	code := pbinSpillingCode(0xD0, pbinStemSubtreeWidth+1)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 1, code)

	state, _, _ := pbinWitnessStateFor(t, corpus, addr)

	got, ok, err := state.Code(addr)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, code, got)
}

// TestPBinWitnessDeployIntoPopulatedCodeZone: a witness for a deploy has to let
// a verifier reach the post-state root, whether or not the code zone already
// holds another contract's chunks. The empty-zone case passes on its own, so the
// populated one is what the shared subtree adds.
func TestPBinWitnessDeployIntoPopulatedCodeZone(t *testing.T) {
	t.Parallel()

	const chunks = 136

	for _, tc := range []struct {
		name  string
		prior int // chunks the code zone already holds, 0 for an empty zone
	}{
		{name: "empty code zone", prior: 0},
		{name: "prior contract of 136 chunks", prior: 136},
		{name: "prior contract of 129 chunks", prior: 129},
		{name: "prior contract of 256 chunks", prior: 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			prior := new(pbinTestCorpus)
			if tc.prior > 0 {
				prior = pbinDeployCorpus(70, pbinSpillingCode(0xA0, tc.prior))
			}
			ms, parentRoot := pbinWitnessCommitted(t, prior)

			code := pbinSpillingCode(0xB0, chunks)
			deploy := pbinDeployCorpus(71, code)
			// The block is executed, so its code is readable, but its leaves are
			// not in the tree the witness proves: that is the parent's.
			ms.setCode(pbinOracleAddr(71), code)

			upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), deploy.plainKeys, deploy.updates)
			nodes, provedKeys, root := pbinWitnessesOf(t, ms, upd, false)
			require.Equal(t, parentRoot, root, "the witness pass must prove the pre-state")

			lean, err := PBinWitnessNodesForKeys(nodes, root, provedKeys)
			require.NoError(t, err)

			state, err := PBinNewWitnessState(lean, root)
			require.NoError(t, err)
			state.SetCode(pbinOracleAddr(71), code)

			got, err := state.Root(context.Background(), deploy.plainKeys, deploy.updates)
			require.NoError(t, err, "the witness must carry every node the deploy descends through")

			deploy.applyTo(t, ms)
			applied := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), deploy.plainKeys, deploy.updates)
			want, err := NewPBinPatriciaHashed(ms).Process(context.Background(), applied, "", nil, WarmupConfig{})
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestPBinWitnessAccountMissingCodeHashIsMalformed: a present account holds
// exactly one of the CODE_HASH and DELEGATION leaves, so a witness that proves
// both absent under a live BASIC_DATA leaf describes a state the tree cannot
// hold. Reading it as an absent account would recompute a wrong root instead.
func TestPBinWitnessAccountMissingCodeHashIsMalformed(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(77)
	keys := pbinDigestCache{sum: pbinSelectedSum}
	var value [pbinValueLength]byte
	value[pbinBasicDataNonceOffset+7] = 1

	preimage := append([]byte{pbinLeafTag}, keys.accountKey(addr, pbinBasicDataLeafKey)...)
	preimage = append(preimage, value[:]...)
	hasher := pbinHasher{sum: pbinSelectedSum}
	root := hasher.hash(preimage)

	state, err := PBinNewWitnessState([][]byte{preimage}, root[:])
	require.NoError(t, err)

	_, _, err = state.Account(addr)
	require.ErrorIs(t, err, errPBinWitnessNode)
}

// TestPBinWitnessZeroCodeSizeNeedsTheEmptyCodeHash: the writer refuses to emit a
// CODE_HASH leaf for real code with no code_size behind it, so a witness carrying
// that pair is malformed. Reading it as empty code would hand the EVM no bytecode
// for a contract and blame the divergence on the root.
func TestPBinWitnessZeroCodeSizeNeedsTheEmptyCodeHash(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(79)
	keys := pbinDigestCache{sum: pbinSelectedSum}
	hasher := pbinHasher{sum: pbinSelectedSum}

	var codeHash common.Hash
	codeHash[0] = 0xCC
	value := pbinCodeHashValue(codeHash)
	preimage := append([]byte{pbinLeafTag}, keys.accountKey(addr, pbinCodeHashLeafKey)...)
	preimage = append(preimage, value[:]...)
	root := hasher.hash(preimage)

	state, err := PBinNewWitnessState([][]byte{preimage}, root[:])
	require.NoError(t, err)

	_, _, err = state.Code(addr)
	require.ErrorIs(t, err, errPBinWitnessNode)
}

// TestPBinWitnessOversizedCodeSizeIsRefused: code_size is four bytes the witness
// itself carries, so a reader that trusts it allocates whatever the sender asks
// for. The refusal must land before the buffer and the chunk walk.
func TestPBinWitnessOversizedCodeSizeIsRefused(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(80)
	keys := pbinDigestCache{sum: pbinSelectedSum}
	hasher := pbinHasher{sum: pbinSelectedSum}

	leaf := func(key []byte, value [pbinValueLength]byte) ([]byte, common.Hash) {
		preimage := append(append([]byte{pbinLeafTag}, key...), value[:]...)
		return preimage, hasher.hash(preimage)
	}

	basicKey := keys.accountKey(addr, pbinBasicDataLeafKey)
	basic, err := pbinEncodeBasicData(1, new(uint256.Int), math.MaxUint32)
	require.NoError(t, err)
	basicNode, basicHash := leaf(basicKey, basic)

	var codeHash common.Hash
	codeHash[0] = 0xDD
	hashNode, hashLeaf := leaf(keys.accountKey(addr, pbinCodeHashLeafKey), pbinCodeHashValue(codeHash))

	// Sub-indices 0 and 1 diverge on the last bit of the key.
	prefix := pbinPathFromBits(basicKey, int16(8*len(basicKey)-1))
	branch := pbinAppendBitPrefix([]byte{pbinBranchTag}, &prefix)
	branch = append(branch, basicHash[:]...)
	branch = append(branch, hashLeaf[:]...)
	root := hasher.hash(branch)

	state, err := PBinNewWitnessState([][]byte{branch, basicNode, hashNode}, root[:])
	require.NoError(t, err)

	_, _, err = state.Code(addr)
	require.ErrorIs(t, err, errPBinWitnessNode)
	require.ErrorContains(t, err, "a witness may carry")
}

// TestPBinWitnessDelegationLeafPinsCodeSize: a DELEGATION leaf is a fixed shape,
// so its account's code_size is always the indicator length. A BASIC_DATA leaf
// claiming anything else describes a state the tree cannot hold, and reading it
// would report an account running code no one wrote.
func TestPBinWitnessDelegationLeafPinsCodeSize(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(78)
	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0x44}, 20)...)
	keys := pbinDigestCache{sum: pbinSelectedSum}
	hasher := pbinHasher{sum: pbinSelectedSum}

	leaf := func(key []byte, value [pbinValueLength]byte) ([]byte, common.Hash) {
		preimage := append(append([]byte{pbinLeafTag}, key...), value[:]...)
		return preimage, hasher.hash(preimage)
	}

	basicKey := keys.accountKey(addr, pbinBasicDataLeafKey)
	var balance uint256.Int
	balance.SetUint64(2)
	basic, err := pbinEncodeBasicData(1, &balance, pbinDelegationCodeLength-1)
	require.NoError(t, err)
	basicNode, basicHash := leaf(basicKey, basic)
	delegNode, delegHash := leaf(keys.accountKey(addr, pbinDelegationLeafKey), pbinEncodeDelegation(indicator))

	// Sub-indices 0 and 2 diverge two bits before the end of the key.
	prefix := pbinPathFromBits(basicKey, int16(8*len(basicKey)-2))
	branch := pbinAppendBitPrefix([]byte{pbinBranchTag}, &prefix)
	branch = append(branch, basicHash[:]...)
	branch = append(branch, delegHash[:]...)
	root := hasher.hash(branch)

	state, err := PBinNewWitnessState([][]byte{branch, basicNode, delegNode}, root[:])
	require.NoError(t, err)

	_, _, err = state.Account(addr)
	require.ErrorIs(t, err, errPBinWitnessNode)
}

// TestPBinFoldIgnoresWitnessBlock: the block override is a witness-pass input.
// A fold that finds one left behind has to derive its chunk keys from state
// anyway — honouring it would commit a wrong root on the execution path.
func TestPBinFoldIgnoresWitnessBlock(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(73)
	code := pbinSpillingCode(0xC1, 4)
	deploy := pbinDeployCorpus(73, code)

	post := NewMockState(t)
	require.NoError(t, post.applyPlainUpdates(deploy.plainKeys, deploy.updates))
	post.setCode(addr, code)

	stale := PBinWitnessBlock{
		Code:    map[string][]byte{string(addr): pbinSpillingCode(0xC2, 9)},
		Removed: map[string]struct{}{string(addr): {}},
	}
	require.Equal(t,
		pbinStreamKeys(t, post, deploy, PBinWitnessBlock{}, false),
		pbinStreamKeys(t, post, deploy, stale, false))
}
