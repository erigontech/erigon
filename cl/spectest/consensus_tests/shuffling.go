package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/spectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ShufflingCore struct {
}

func (b *ShufflingCore) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		Seed    common.Hash `yaml:"seed"`
		Count   int         `yaml:"count"`
		Mapping []int       `yaml:"mapping"`
	}
	if err := spectest.ReadMeta(root, "mapping.yaml", &meta); err != nil {
		return err
	}

	s := state.New(&clparams.MainnetBeaconConfig)
	keccakOptimized := utils.OptimizedKeccak256NotThreadSafe()
	preInputs := shuffling.ComputeShuffledIndexPreInputs(s.BeaconConfig(), meta.Seed)
	for idx, v := range meta.Mapping {
		shuffledIdx, err := shuffling.ComputeShuffledIndex(s.BeaconConfig(), uint64(idx), uint64(meta.Count), meta.Seed, preInputs, keccakOptimized)
		require.NoError(t, err)
		assert.EqualValues(t, v, shuffledIdx)
	}
	return nil
}
