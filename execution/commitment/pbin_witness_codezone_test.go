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
	"testing"

	"github.com/stretchr/testify/require"
)

// Deploying a spilling contract writes leaves into the content-addressed code
// zone. When another contract's chunks are already there, the new leaves split
// an existing subtree, and the witness has to carry the node they split.

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
	require.Len(t, fold, 2+136, "two header leaves and one key per chunk")

	// Without it the parent state yields the account's two header leaves only.
	require.Len(t, pbinStreamKeys(t, parent, deploy, PBinWitnessBlock{}, true), 2)
}

// TestPBinWitnessDeployIntoPopulatedCodeZone: a witness for a deploy has to let
// a verifier reach the post-state root, whether or not the code zone already
// holds another contract's chunks. The empty-zone case passes on its own, so the
// populated one is what the shared subtree adds.
func TestPBinWitnessDeployIntoPopulatedCodeZone(t *testing.T) {
	t.Parallel()

	const chunks = 136 // 128 header chunks, 8 in the code zone

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
