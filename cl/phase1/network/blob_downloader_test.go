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

package network

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/types"
)

type staticPeerDasGetter struct{ pd das.PeerDas }

func (s staticPeerDasGetter) GetPeerDas() das.PeerDas { return s.pd }

// A historical fulu block whose PeerDAS data columns are served by no peer (older
// than the network custody window) makes DownloadColumnsAndRecoverBlobs block until
// its context is cancelled. Column recovery must be bounded per block so the archive
// blob backfill cannot hang forever holding the index read tx.
func TestBlobHistoryDownloaderFuluColumnRecoveryIsBounded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().
		DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ []cltypes.ColumnSyncableSignedBlock) error {
			<-ctx.Done() // never recovers — unblocks only when the per-attempt ctx expires
			return ctx.Err()
		}).
		AnyTimes()

	b := &BlobHistoryDownloader{
		ctx:                   context.Background(),
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: 50 * time.Millisecond,
		logger:                log.New(),
	}

	fulu := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	fulu.Block.Slot = 100

	done := make(chan struct{})
	go func() {
		b.recoverFuluColumns([]*cltypes.SignedBeaconBlock{fulu})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recoverFuluColumns hung — unbounded PeerDAS column recovery")
	}
}

// stubBlockReader serves one block for one slot, standing in for a beaconblocks segment read.
type stubBlockReader struct {
	slot  uint64
	block *cltypes.SignedBeaconBlock
}

func (s stubBlockReader) ReadBlockBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return s.blockFor(slot), nil
}

func (s stubBlockReader) ReadBlockByRoot(_ context.Context, _ kv.Tx, _ common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return nil, nil
}

func (s stubBlockReader) ReadHeaderByRoot(_ context.Context, _ kv.Tx, _ common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	return nil, nil
}

func (s stubBlockReader) CacheBlockBody(_ uint64, _ [][]byte, _ []*types.Withdrawal) {}

func (s stubBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return s.blockFor(slot), nil
}

func (s stubBlockReader) FrozenSlots() uint64 { return 0 }

func (s stubBlockReader) blockFor(slot uint64) *cltypes.SignedBeaconBlock {
	if slot == s.slot {
		return s.block
	}
	return nil
}

// A block read out of a beaconblocks segment carries no execution payload, so hashing it gives
// a root that never existed on chain and the store lookup misses. The count must therefore be
// read under the canonical root from the index: otherwise every frozen blob-bearing block looks
// incomplete forever, the backfill re-requests columns no peer still has, and the blob dump
// stays gated behind a pass that never completes.
func TestCollectIncompleteBlocksReadsTheCountUnderTheCanonicalRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	const slot = 1_000
	canonical := common.HexToHash("0xc0ffee")

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, canonical)
	}))

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	commitment := cltypes.KZGCommitment{}
	commitment[0] = 0x01
	block.Block.Body.BlobKzgCommitments.Append(&commitment)

	// The block's own hash is not the canonical root, exactly as for a payload-stripped read.
	selfRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, canonical, common.Hash(selfRoot), "fixture must reproduce the mismatch")

	store := blob_mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().KzgCommitmentsCount(gomock.Any(), canonical).Return(uint32(1), nil).AnyTimes()
	store.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Not(gomock.Eq(canonical))).Return(uint32(0), nil).AnyTimes()

	b := &BlobHistoryDownloader{
		ctx:         context.Background(),
		beaconCfg:   &clparams.MainnetBeaconConfig,
		indiciesDB:  db,
		blobStorage: store,
		blockReader: stubBlockReader{slot: slot, block: block},
		logger:      log.New(),
	}

	batch, _, err := b.collectIncompleteBlocks(slot, slot)
	require.NoError(t, err)
	require.Empty(t, batch, "a slot whose sidecars are stored under its canonical root must not be re-queued")
}
