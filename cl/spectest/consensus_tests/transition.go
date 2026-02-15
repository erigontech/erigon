// Copyright 2024 The Erigon Authors
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

package consensus_tests

import (
	"fmt"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/common"
)

type TransitionCore struct {
}

func (b *TransitionCore) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		PostFork   string `yaml:"post_fork"`
		ForkEpoch  uint64 `yaml:"fork_epoch"`
		BlockCount uint64 `yaml:"blocks_count"`

		ForkBlock *uint64 `yaml:"fork_block,omitempty"`
	}
	if err := spectest.ReadMeta(root, "meta.yaml", &meta); err != nil {
		return err
	}
	startState, err := spectest.ReadBeaconState(root, c.Version()-1, spectest.PreSsz)
	require.NoError(t, err)
	stopState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	require.NoError(t, err)
	switch c.Version() {
	case clparams.AltairVersion:
		startState.BeaconConfig().AltairForkEpoch = meta.ForkEpoch
	case clparams.BellatrixVersion:
		startState.BeaconConfig().BellatrixForkEpoch = meta.ForkEpoch
	case clparams.CapellaVersion:
		startState.BeaconConfig().CapellaForkEpoch = meta.ForkEpoch
	case clparams.DenebVersion:
		startState.BeaconConfig().DenebForkEpoch = meta.ForkEpoch
	case clparams.ElectraVersion:
		startState.BeaconConfig().ElectraForkEpoch = meta.ForkEpoch
	case clparams.FuluVersion:
		startState.BeaconConfig().FuluForkEpoch = meta.ForkEpoch
	case clparams.GloasVersion:
		startState.BeaconConfig().GloasForkEpoch = meta.ForkEpoch
	}

	// Pre-read all blocks so we can look ahead for Gloas payload simulation decisions.
	var blocks []*cltypes.SignedBeaconBlock
	for idx := 0; ; idx++ {
		testSlot, err := spectest.ReadBlockSlot(root, idx)
		require.NoError(t, err)
		var block *cltypes.SignedBeaconBlock
		if testSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch >= meta.ForkEpoch {
			block, err = spectest.ReadBlock(root, c.Version(), idx)
			require.NoError(t, err)
		} else {
			block, err = spectest.ReadBlock(root, c.Version()-1, idx)
			require.NoError(t, err)
		}
		if block == nil {
			break
		}
		blocks = append(blocks, block)
	}

	startSlot := startState.Slot()
	for i, block := range blocks {
		if err := machine.TransitionState(c.Machine, startState, block); err != nil {
			return fmt.Errorf("cannot transition state: %s. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot)
		}

		// [Gloas:EIP7732] Decide whether to simulate execution payload delivery.
		// The pyspec's next_slots_with_attestations helper calls payload_state_transition_no_store
		// after each block, but state_transition_and_sign_block and state_transition_across_slots do not.
		// We detect this by checking: (1) for non-zero block_hash, use parent_block_hash chain matching;
		// (2) for zero block_hash, use consecutive-slot-with-attestations as signal.
		if c.Version() >= clparams.GloasVersion && block.Block.Body.SignedExecutionPayloadBid != nil {
			shouldSimulate := false
			bidBlockHash := block.Block.Body.SignedExecutionPayloadBid.Message.BlockHash
			zeroHash := common.Hash{}
			if bidBlockHash != zeroHash {
				// Non-zero block_hash: use parent_block_hash chain
				if i+1 < len(blocks) {
					nextBid := blocks[i+1].Block.Body.SignedExecutionPayloadBid
					if nextBid != nil && nextBid.Message.ParentBlockHash == bidBlockHash {
						shouldSimulate = true
					}
				} else {
					if stopState.LatestBlockHash() == bidBlockHash {
						shouldSimulate = true
					}
				}
			} else {
				// Zero block_hash: simulate if block is consecutive with previous AND has attestations.
				// This matches the next_slots_with_attestations pattern.
				isConsecutive := i == 0 || block.Block.Slot == blocks[i-1].Block.Slot+1
				hasAttestations := block.Block.Body.Attestations.Len() > 0
				shouldSimulate = isConsecutive && hasAttestations
			}
			if shouldSimulate {
				machine.PayloadStateTransitionNoStore(startState, block.Block)
			}
		}
	}
	expectedRoot, err := stopState.HashSSZ()
	require.NoError(t, err)
	haveRoot, err := startState.HashSSZ()
	require.NoError(t, err)
	assert.EqualValues(t, expectedRoot, haveRoot, "state root")
	return nil
}
