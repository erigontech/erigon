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

	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	}
	startSlot := startState.Slot()
	blockIndex := 0
	for {
		testSlot, err := spectest.ReadBlockSlot(root, blockIndex)
		require.NoError(t, err)
		var block *cltypes.SignedBeaconBlock
		if testSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch >= meta.ForkEpoch {
			block, err = spectest.ReadBlock(root, c.Version(), blockIndex)
			require.NoError(t, err)
		} else {
			block, err = spectest.ReadBlock(root, c.Version()-1, blockIndex)
			require.NoError(t, err)
		}

		if block == nil {
			break
		}
		blockIndex++
		if err := machine.TransitionState(c.Machine, startState, block); err != nil {
			return fmt.Errorf("cannot transition state: %s. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot)
		}
	}
	expectedRoot, err := stopState.HashSSZ()
	require.NoError(t, err)
	haveRoot, err := startState.HashSSZ()
	require.NoError(t, err)
	assert.EqualValues(t, expectedRoot, haveRoot, "state root")
	return nil
}
