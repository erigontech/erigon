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

package txpool

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestBlobStoreAliasing(t *testing.T) {
	require := require.New(t)
	pool, _ := newGetBlobsTestPool(t, 1)

	h := common.Hash{0x42}
	txnA := common.Hash{0xa}
	txnB := common.Hash{0xb}
	bundleA := PoolBlobBundle{Blob: []byte("A")}
	bundleB := PoolBlobBundle{Blob: []byte("B")}

	pool.blobs.put(h, txnA, bundleA)
	require.Equal([]byte("A"), pool.GetBlobs([]common.Hash{h})[0].Blob)

	pool.blobs.put(h, txnB, bundleB)
	require.Equal([]byte("B"), pool.GetBlobs([]common.Hash{h})[0].Blob)

	pool.blobs.remove(txnA, []common.Hash{h})
	require.Equal([]byte("B"), pool.GetBlobs([]common.Hash{h})[0].Blob)

	pool.blobs.remove(txnB, []common.Hash{h})
	require.Nil(pool.GetBlobs([]common.Hash{h})[0].Blob)
}

func TestGetBlobsConcurrentReadWrite(t *testing.T) {
	pool, _ := newGetBlobsTestPool(t, 1)

	const numBlobs = 64
	hashes := make([]common.Hash, numBlobs)
	owners := make([]common.Hash, numBlobs)
	bundles := make([]PoolBlobBundle, numBlobs)
	for i := range hashes {
		hashes[i] = common.Hash{byte(i), byte(i >> 8), 0x01}
		owners[i] = common.Hash{byte(i), 0x02}
		bundles[i] = PoolBlobBundle{Blob: []byte{byte(i)}}
	}

	var wg sync.WaitGroup
	var stop atomic.Bool

	for w := range 4 {
		wg.Go(func() {
			for !stop.Load() {
				for i := w; i < numBlobs; i += 4 {
					pool.blobs.put(hashes[i], owners[i], bundles[i])
				}
				for i := w; i < numBlobs; i += 4 {
					pool.blobs.remove(owners[i], []common.Hash{hashes[i]})
				}
			}
		})
	}

	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				got := pool.GetBlobs(hashes)
				if len(got) != numBlobs {
					panic("GetBlobs returned wrong length")
				}
			}
		})
	}

	for range 5000 {
		_ = pool.GetBlobs(hashes)
	}
	stop.Store(true)
	wg.Wait()
}
