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
	"io/fs"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/utils"
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
	keccakOptimized := utils.OptimizedSha256NotThreadSafe()
	preInputs := shuffling.ComputeShuffledIndexPreInputs(s.BeaconConfig(), meta.Seed)
	for idx, v := range meta.Mapping {
		shuffledIdx, err := shuffling.ComputeShuffledIndex(s.BeaconConfig(), uint64(idx), uint64(meta.Count), meta.Seed, preInputs, keccakOptimized)
		require.NoError(t, err)
		assert.EqualValues(t, v, shuffledIdx)
	}
	return nil
}
