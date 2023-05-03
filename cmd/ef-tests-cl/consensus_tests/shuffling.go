package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
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

	s := state.GetEmptyBeaconState()
	keccakOptimized := utils.OptimizedKeccak256()
	preInputs := s.ComputeShuffledIndexPreInputs(meta.Seed)
	for idx, v := range meta.Mapping {
		shuffledIdx, err := s.ComputeShuffledIndex(uint64(idx), uint64(meta.Count), meta.Seed, preInputs, keccakOptimized)
		require.NoError(t, err)
		assert.EqualValues(t, v, shuffledIdx)
	}
	return nil
}
