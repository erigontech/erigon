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

package eth2_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
)

func TestVerifyBlockSignatureRejectsOutOfRangeProposerIndex(t *testing.T) {
	s := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, s.AddValidator(solid.NewValidator(), 32_000_000_000))
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	for _, proposerIndex := range []uint64{1, 1 << 63, math.MaxUint64} {
		block.Block.ProposerIndex = proposerIndex
		ok, err := eth2.VerifyBlockSignature(s, block)
		require.ErrorIs(t, err, raw.ErrInvalidValidatorIndex, "proposer index %d", proposerIndex)
		require.False(t, ok)
	}
}
