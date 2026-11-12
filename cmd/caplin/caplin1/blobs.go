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
	"context"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// frozenBlobs reads the blob sidecars retired into snapshots, those of the
// slots below FrozenBlobs.
type frozenBlobs interface {
	FrozenBlobs() uint64
	ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error)
}

type blobStores struct {
	clock   eth_clock.EthereumClock
	indexDB kv.RoDB
	blobs   blob_storage.BlobStorage
	frozen  frozenBlobs
}

// BlobReader reads, in the same process, the blobs that the Caplin instance
// run with WithBlobReader stores: none before it has opened its stores.
type BlobReader struct {
	stores atomic.Pointer[blobStores]
}

// NewBlobReader returns a reader for RunCaplinService to open.
func NewBlobReader() *BlobReader {
	return &BlobReader{}
}

func (r *BlobReader) open(clock eth_clock.EthereumClock, indexDB kv.RoDB, blobs blob_storage.BlobStorage, frozen frozenBlobs) {
	r.stores.Store(&blobStores{clock: clock, indexDB: indexDB, blobs: blobs, frozen: frozen})
}

// ExecutionBlobs returns the blobs of the canonical beacon block whose
// execution payload is the block with hash and timestamp t, in sidecar
// order; found is false while the node stores no such block or not its
// blobs.
func (r *BlobReader) ExecutionBlobs(ctx context.Context, hash common.Hash, t uint64) (blobs [][]byte, found bool, err error) {
	s := r.stores.Load()
	if s == nil {
		return nil, false, nil
	}
	slot := s.clock.GetSlotByTime(time.Unix(int64(t), 0))
	tx, err := s.indexDB.BeginRo(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, false, err
	}
	payload, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
	if err != nil || payload != hash {
		return nil, false, err
	}
	var sidecars []*cltypes.BlobSidecar
	if slot < s.frozen.FrozenBlobs() {
		sidecars, err = s.frozen.ReadBlobSidecars(slot)
	} else {
		sidecars, _, err = s.blobs.ReadBlobSidecars(ctx, slot, root)
	}
	if err != nil || len(sidecars) == 0 {
		return nil, false, err
	}
	blobs = make([][]byte, len(sidecars))
	for i, sc := range sidecars {
		blobs[i] = sc.Blob[:]
	}
	return blobs, true, nil
}
