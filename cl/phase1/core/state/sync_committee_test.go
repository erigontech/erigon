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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestSyncCommitteeForMessageSlot(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	period := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	p := 10 * period // first slot of a period

	s := state.New(cfg)
	current := solid.NewSyncCommitteeFromParameters(make([]common.Bytes48, cfg.SyncCommitteeSize), common.Bytes48{1})
	next := solid.NewSyncCommitteeFromParameters(make([]common.Bytes48, cfg.SyncCommitteeSize), common.Bytes48{2})
	require.NoError(t, s.SetCurrentSyncCommittee(current))
	require.NoError(t, s.SetNextSyncCommittee(next))

	tests := []struct {
		name      string
		stateSlot uint64
		slot      uint64
		want      *solid.SyncCommittee
	}{
		{"head at the message slot", p - 3, p - 3, current},
		{"last slot of the period", p - 1, p - 1, next},
		{"previous slot before the rotation", p - 1, p - 2, current},
		{"last slot before its block is imported", p - 2, p - 1, next},
		{"next period beyond the next committee", p - 1, p + period - 1, nil},
		{"older period", p, p - 2, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, s.SetSlot(tt.stateSlot))
			committee, err := state.SyncCommitteeForMessageSlot(s, tt.slot)
			if tt.want == nil {
				require.ErrorIs(t, err, state.ErrSyncCommitteeNotInState)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want.AggregatePublicKey(), committee.AggregatePublicKey())
		})
	}
}
