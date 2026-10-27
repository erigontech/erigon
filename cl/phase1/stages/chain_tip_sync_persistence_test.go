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

package stages

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
)

type blockWriteErrorDB struct {
	kv.RwDB
	err          error
	failures     int
	writes       int
	onWriteError func()
}

func (db *blockWriteErrorDB) Update(ctx context.Context, fn func(kv.RwTx) error) error {
	db.writes++
	if db.writes <= db.failures {
		if db.onWriteError != nil {
			db.onWriteError()
		}
		return db.err
	}
	return db.RwDB.Update(ctx, fn)
}

func TestChainTipBatchRetriesAcceptedBlockAfterWriteFailure(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.indiciesDB = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	seen := make(map[common.Hash]struct{})
	rejected := make(map[common.Hash]error)
	args := Args{targetSlot: block.Block.Slot, hasDownloaded: true, peers: 1}
	batch := []*cltypes.SignedBeaconBlock{block}

	require.False(t, processChainTipBatch(t.Context(), cfg, args, batch, seen, rejected))
	require.ErrorIs(t, rejected[root], db.err)
	_, accepted := cfg.forkChoice.GetHeader(root)
	require.True(t, accepted)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.Nil(t, slot)
		return nil
	}))

	require.True(t, processChainTipBatch(t.Context(), cfg, args, batch, seen, rejected))
	require.Equal(t, 2, db.writes)
	require.Equal(t, 1, graph.added[root], "retry storage without importing the accepted block again")
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		storedRoot, err := beacon_indicies.ReadBlockRootByStateRoot(tx, block.Block.StateRoot)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), storedRoot)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot)
		require.Equal(t, block.Block.Slot, *slot)
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestChainTipListenerRetriesWriteWithoutAnotherBatch(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 2}
	cfg.indiciesDB = db
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().BackfillCustodyColumns(block.Block.Slot, block.Block.Slot).Times(1)
	cfg.peerDas = peerDas
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), cfg, Args{targetSlot: block.Block.Slot}, respCh, make(chan error))

	require.NoError(t, err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "reaching the target in fork choice must not skip the failed write")
		return nil
	}))
	require.Equal(t, 3, db.writes)
	require.Equal(t, 1, graph.added[root])
}

func TestChainTipSyncRetriesWriteAfterStageCancellation(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 100}
	cfg.indiciesDB = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db.onWriteError = cancel
	args := Args{targetSlot: block.Block.Slot, hasDownloaded: true, peers: 1}

	err = listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), cfg, args, respCh, make(chan error))

	require.ErrorIs(t, err, context.Canceled)
	args.seenSlot = cfg.forkChoice.HighestSeen()
	require.Equal(t, args.targetSlot, args.seenSlot)
	stage := ConsensusClStages().Stages[ChainTipSync]
	require.Equal(t, ChainTipSync, stage.TransitionFunc(cfg, args, err))

	db.failures = db.writes
	require.NoError(t, stage.ActionFunc(t.Context(), log.Root(), cfg, args))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "a new stage must finish the write even when fork choice reached the target")
		return nil
	}))
	require.Equal(t, ForkChoice, stage.TransitionFunc(cfg, args, nil))
	require.Equal(t, 1, graph.added[root])
}

func TestChainTipBatchPersistsKnownBlockFromForkChoice(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), block, true, true, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB}
	cfg.indiciesDB = db
	duplicate := *block
	duplicate.Signature[0] ^= 1
	args := Args{targetSlot: block.Block.Slot}
	batch := []*cltypes.SignedBeaconBlock{&duplicate}

	require.True(t, processChainTipBatch(t.Context(), cfg, args, batch, make(map[common.Hash]struct{}), make(map[common.Hash]error)))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, header, "a block accepted by another importer must also be persisted")
		require.Equal(t, block.Signature, header.Signature, "the block root does not authenticate the duplicate's signature")
		return nil
	}))
	require.Equal(t, 1, graph.added[root])
	require.Equal(t, 1, db.writes)

	require.True(t, processChainTipBatch(t.Context(), cfg, args, batch, make(map[common.Hash]struct{}), make(map[common.Hash]error)))
	require.Equal(t, 1, db.writes, "an already persisted block needs no further write")
}

func TestChainTipSyncRetriesWriteFromForwardSync(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.indiciesDB = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	progress, err := processDownloadedBlockBatches(t.Context(), log.Root(), cfg, block.Block.Slot-1, false, []*cltypes.SignedBeaconBlock{block}, nil)
	require.ErrorIs(t, err, db.err)
	require.Equal(t, block.Block.Slot-1, progress)
	args := Args{seenSlot: cfg.forkChoice.HighestSeen(), targetSlot: block.Block.Slot}
	require.Equal(t, args.targetSlot, args.seenSlot)

	require.NoError(t, chainTipSync(t.Context(), log.Root(), cfg, args))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "the stage transition must preserve a failed forward-sync write")
		return nil
	}))
	require.Equal(t, 1, graph.added[root])
}
