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

package synced_data

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestHeadUpdateKeepsPreviousHead(t *testing.T) {
	updates := []struct {
		name   string
		update func(*SyncedDataManager, *state.CachingBeaconState) error
	}{
		{"OnHeadState", (*SyncedDataManager).OnHeadState},
		{"OnHeadStateWithBlockRoot", func(m *SyncedDataManager, s *state.CachingBeaconState) error {
			return m.OnHeadStateWithBlockRoot(s, common.Hash{})
		}},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
			var headRoot [32]byte
			for i := range 3 {
				published := state.New(&clparams.MainnetBeaconConfig)
				require.NoError(t, published.SetSlot(uint64(100+i)))
				prevRoot := headRoot
				var err error
				headRoot, err = published.BlockRoot()
				require.NoError(t, err)
				require.NoError(t, tc.update(manager, published))

				var head, prev *state.CachingBeaconState
				require.NoError(t, manager.ViewHeadState(func(s *state.CachingBeaconState) error {
					head = s
					return nil
				}))
				prevErr := manager.ViewPreviousHeadState(func(s *state.CachingBeaconState) error {
					prev = s
					return nil
				})
				require.NotSame(t, published, head)
				require.Equal(t, published.Slot(), head.Slot())
				if i == 0 {
					require.ErrorIs(t, prevErr, ErrPreviousStateNotAvailable)
					continue
				}
				require.NoError(t, prevErr)
				require.NotSame(t, head, prev)
				root, err := prev.BlockRoot()
				require.NoError(t, err)
				require.Equal(t, prevRoot, root)
			}
		})
	}
}
