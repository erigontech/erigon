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

package notifications

import (
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type stateChangeRecorder struct {
	batch *remoteproto.StateChangeBatch
}

func (r *stateChangeRecorder) SendStateChanges(_ context.Context, batch *remoteproto.StateChangeBatch) {
	r.batch = batch
}

func TestAccumulatorIncompleteChangesSurviveCheckpointAndAppend(t *testing.T) {
	acc := NewAccumulator()
	header := &types.Header{Number: uint256.Int{70}}
	restoreComplete := acc.Checkpoint()
	acc.StartHeadOnlyRange(1, header)
	restoreComplete()
	acc.StartChange(header, nil, false)
	recorder := &stateChangeRecorder{}
	acc.SendAndReset(t.Context(), recorder, 0, 0, 0, 0)
	require.False(t, recorder.batch.StateChangesIncomplete)

	acc.StartHeadOnlyRange(1, header)
	restoreIncomplete := acc.Checkpoint()
	acc.Reset(0)
	restoreIncomplete()
	target := NewAccumulator()
	acc.AppendAndReset(target)
	target.StartChange(header, nil, false)
	target.ChangeAccount(common.Address{1}, 0, []byte{1})
	target.SendAndReset(t.Context(), recorder, 0, 0, 0, 0)
	require.True(t, recorder.batch.StateChangesIncomplete)

	target.StartChange(header, nil, false)
	target.SendAndReset(t.Context(), recorder, 0, 0, 0, 0)
	require.False(t, recorder.batch.StateChangesIncomplete, "a new batch starts complete")
}

func TestAccumulatorDeleteAccount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		actions []Action
	}{
		{name: "absent"},
		{name: "storage", actions: []Action{ActionStorage}},
		{name: "account", actions: []Action{ActionUpsert}},
		{name: "code", actions: []Action{ActionCode}},
		{name: "account_code_storage", actions: []Action{ActionUpsert, ActionCode, ActionStorage}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAccumulator()
			address := common.Address{1}
			a.StartChange(&types.Header{}, nil, false)
			for _, action := range tc.actions {
				switch action {
				case ActionStorage:
					a.ChangeStorage(address, 1, common.Hash{2}, []byte{3})
				case ActionUpsert:
					a.ChangeAccount(address, 1, []byte{4})
				case ActionCode:
					a.ChangeCode(address, 1, []byte{5})
				}
			}
			for range 2 {
				require.NotPanics(t, func() { a.DeleteAccount(address) })
				require.Len(t, a.Changes()[0].Changes, 1)
				change := a.Changes()[0].Changes[0]
				require.Equal(t, ActionRemove, change.Action)
				require.Empty(t, change.Data)
				require.Empty(t, change.Code)
				require.Empty(t, change.StorageChanges)
			}
		})
	}
}

func TestAccumulatorRecreateAccount(t *testing.T) {
	for _, incarnation := range []uint64{0, 1} {
		for _, first := range []Action{ActionUpsert, ActionCode, ActionStorage} {
			t.Run(fmt.Sprintf("incarnation_%d/first_%d", incarnation, first), func(t *testing.T) {
				a := NewAccumulator()
				address, slot := common.Address{1}, common.Hash{2}
				a.StartChange(&types.Header{}, nil, false)
				a.ChangeStorage(address, 1, slot, []byte{3})
				a.DeleteAccount(address)
				switch first {
				case ActionCode:
					a.ChangeCode(address, incarnation, []byte{4})
				case ActionStorage:
					a.ChangeStorage(address, incarnation, slot, []byte{6})
				}
				a.ChangeAccount(address, incarnation, []byte{5})
				a.ChangeCode(address, incarnation, []byte{4})
				a.ChangeStorage(address, incarnation, slot, []byte{6})
				require.Equal(t, []AccountChange{{Address: address, Incarnation: 1, Action: ActionRemove}, {
					Address: address, Incarnation: incarnation, Action: ActionUpsertCode,
					Data: []byte{5}, Code: []byte{4},
					StorageChanges: []StorageChange{{Location: slot, Data: []byte{6}}},
				}}, a.Changes()[0].Changes)
			})
		}
	}
}

func TestAccumulatorAppendPreservesChanges(t *testing.T) {
	for _, name := range []string{"empty source", "nonempty source"} {
		t.Run(name, func(t *testing.T) {
			source, target := NewAccumulator(), NewAccumulator()
			address, slot := common.Address{1}, common.Hash{2}
			target.StartChange(&types.Header{Time: 1}, nil, true)
			target.ChangeStorage(address, 0, slot, []byte{1})
			if name == "nonempty source" {
				source.StartChange(&types.Header{Time: 2}, nil, false)
				source.ChangeStorage(address, 0, slot, []byte{2})
			}
			want := append([]StateChange(nil), target.Changes()...)
			want = append(want, source.Changes()...)
			source.AppendAndReset(target)
			require.Equal(t, want, target.Changes())
			require.Empty(t, source.Changes())

			target.ChangeStorage(address, 0, slot, []byte{3})
			target.ChangeAccount(address, 0, []byte{4})
			target.ChangeStorage(common.Address{5}, 0, slot, []byte{6})
			last := target.Changes()[len(target.Changes())-1]
			require.Len(t, last.Changes, 2)
			require.Equal(t, []byte{4}, last.Changes[0].Data)
			require.Len(t, last.Changes[0].StorageChanges, 1)
			require.Equal(t, []byte{3}, last.Changes[0].StorageChanges[0].Data)
		})
	}
}

func TestAccumulatorCheckpoint(t *testing.T) {
	a := NewAccumulator()
	address, slot := common.Address{1}, common.Hash{2}
	a.StartChange(&types.Header{Time: 1}, [][]byte{{1}}, true)
	a.ChangeAccount(address, 0, []byte{2})
	a.ChangeStorage(address, 0, slot, []byte{3})
	want := a.Changes()[0].ToProto()
	restore := a.Checkpoint()

	a.StartChange(&types.Header{Time: 2}, [][]byte{{4}}, false)
	a.ChangeStorage(address, 0, slot, []byte{5})
	restore()

	require.Len(t, a.Changes(), 1)
	require.Equal(t, want, a.Changes()[0].ToProto())
	require.Panics(t, func() { a.ChangeStorage(address, 0, slot, []byte{6}) }, "restoring a checkpoint must not reopen completed changes")
	a.StartChange(&types.Header{Time: 3}, nil, false)
	a.ChangeStorage(address, 0, slot, []byte{7})
	require.Equal(t, want, a.Changes()[0].ToProto())
}
