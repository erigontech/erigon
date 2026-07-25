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

	"github.com/erigontech/erigon/common"
)

type blobStore struct {
	mu      sync.RWMutex
	bundles map[common.Hash]blobStoreEntry
}

type blobStoreEntry struct {
	bundle  PoolBlobBundle
	txnHash common.Hash
}

func newBlobStore() *blobStore {
	return &blobStore{bundles: make(map[common.Hash]blobStoreEntry)}
}

func (s *blobStore) put(blobHash, txnHash common.Hash, bundle PoolBlobBundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundles[blobHash] = blobStoreEntry{bundle: bundle, txnHash: txnHash}
}

func (s *blobStore) remove(txnHash common.Hash, blobHashes []common.Hash) {
	if len(blobHashes) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range blobHashes {
		if e, ok := s.bundles[h]; ok && e.txnHash == txnHash {
			delete(s.bundles, h)
		}
	}
}

func (s *blobStore) get(blobHashes []common.Hash) []PoolBlobBundle {
	bundles := make([]PoolBlobBundle, len(blobHashes))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, h := range blobHashes {
		if e, ok := s.bundles[h]; ok {
			bundles[i] = e.bundle
		}
	}
	return bundles
}
