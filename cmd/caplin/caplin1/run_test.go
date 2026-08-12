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

package caplin1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/consensus"
	"github.com/erigontech/erigon/common/log/v3"
)

// TestSelectConsensusEngine locks the CaplinConfig.ConsensusEngineType ->
// consensus.Engine mapping that lets a per-L2 Caplin instance run the based
// rollup engine instead of the hardcoded beacon engine.
func TestSelectConsensusEngine(t *testing.T) {
	logger := log.New()

	cases := []struct {
		name         string
		engineType   string
		wantType     consensus.EngineType
		wantFinality consensus.FinalityMode
		wantDA       bool
	}{
		{"empty defaults to beacon", "", consensus.BeaconChainEngineType, consensus.FinalityCasperFFG, true},
		{"explicit beacon", "beacon", consensus.BeaconChainEngineType, consensus.FinalityCasperFFG, true},
		{"rollup", "rollup", consensus.RollupEngineType, consensus.FinalityL1Anchor, false},
		{"rollup-dev", "rollup-dev", consensus.DevEngineType, consensus.FinalityInstant, false},
		{"unrecognised falls back to beacon", "bogus", consensus.BeaconChainEngineType, consensus.FinalityCasperFFG, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := selectConsensusEngine(tc.engineType, logger)
			require.NotNil(t, eng)
			require.Equal(t, tc.wantType, eng.Type())
			require.Equal(t, tc.wantFinality, eng.FinalityMode())
			require.Equal(t, tc.wantDA, eng.ShouldVerifyDataAvailability())
		})
	}
}
