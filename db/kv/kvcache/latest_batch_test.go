// Copyright 2021 The Erigon Authors
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

package kvcache

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestLatestBatchViewRequiresMatchingSnapshot(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	hash := common.Hash{1}
	rawdb.WriteHeadBlockHash(tx, hash)
	require.NoError(t, tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 7)))
	previous := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(hash), StateVersionId: 7}
	target := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(common.Hash{2}), StateVersionId: 8}
	cache := NewLatestBatchCache()
	for _, reset := range []bool{false, true} {
		cache.OnNewBlock(&remoteproto.StateChangeBatch{PreviousView: previous, TargetView: target, StateChangesIncomplete: reset})
		_, err := cache.View(t.Context(), tx)
		if reset {
			require.Error(t, err, "reset cannot read the previous snapshot")
		} else {
			require.NoError(t, err, "a complete advance may overlay the previous snapshot")
		}
	}
	rawdb.WriteHeadBlockHash(tx, common.Hash{3})
	cache.OnNewBlock(&remoteproto.StateChangeBatch{PreviousView: previous, TargetView: target})
	_, err := cache.View(t.Context(), tx)
	require.Error(t, err, "the state version alone does not identify a fork")
}

func TestLatestBatchViewIsPinned(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	cache := NewLatestBatchCache()
	address := common.Address{1}
	batch := func(value byte) *remoteproto.StateChangeBatch {
		return &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{Changes: []*remoteproto.AccountChange{{
			Address: gointerfaces.ConvertAddressToH160(address), Action: remoteproto.Action_UPSERT, Data: []byte{value},
		}}}}}
	}
	cache.OnNewBlock(batch(1))
	view, err := cache.View(t.Context(), tx)
	require.NoError(t, err)
	cache.OnNewBlock(batch(2))
	value, err := view.Get(address[:])
	require.NoError(t, err)
	require.Equal(t, []byte{1}, value)
}

type priorAccountTx struct{ kv.TemporalTx }

func (priorAccountTx) GetLatest(kv.Domain, []byte, kv.GetLatestOptions) ([]byte, kv.Step, error) {
	return []byte{1}, 0, nil
}

func TestLatestBatchDeletionMasksPriorSnapshot(t *testing.T) {
	cache := NewLatestBatchCache()
	address := common.Address{1}
	cache.OnNewBlock(&remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{Changes: []*remoteproto.AccountChange{{
		Address: gointerfaces.ConvertAddressToH160(address), Action: remoteproto.Action_REMOVE,
	}}}}})
	view, err := cache.View(t.Context(), priorAccountTx{})
	require.NoError(t, err)
	value, err := view.Get(address[:])
	require.NoError(t, err)
	require.Empty(t, value)
}
