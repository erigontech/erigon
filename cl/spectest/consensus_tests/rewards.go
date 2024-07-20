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
)

type RewardsCore struct {
}

func (b *RewardsCore) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	t.Skipf("Skippinf attestation reward calculation tests for now")
	//preState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	//require.NoError(t, err)

	//source_deltas, err := readDelta(root, "source_deltas.ssz_snappy")
	//require.NoError(t, err)
	//target_deltas, err := readDelta(root, "target_deltas.ssz_snappy")
	//require.NoError(t, err)
	//head_deltas, err := readDelta(root, "head_deltas.ssz_snappy")
	//require.NoError(t, err)
	//inclusion_delay_deltas, err := readDelta(root, "inclusion_delay_deltas.ssz_snappy")
	//require.NoError(t, err)
	//inactivity_penalty_deltas, err := readDelta(root, "inactivity_penalty_deltas.ssz_snappy")
	//require.NoError(t, err)

	return nil
}
