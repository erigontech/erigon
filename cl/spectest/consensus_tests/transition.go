package consensus_tests

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/spectest"
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
	assert.NoError(t, err)
	haveRoot, err := startState.HashSSZ()
	assert.NoError(t, err)
	assert.EqualValues(t, haveRoot, expectedRoot, "state root")
	return nil
}
