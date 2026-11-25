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

package state

import (
	"encoding/binary"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

type storageRootRecords struct {
	n, i  int
	value []byte
}

func (s *storageRootRecords) HasNext() bool { return s.i < s.n }
func (s *storageRootRecords) Close()        {}
func (s *storageRootRecords) Next() ([]byte, []byte, error) {
	var addrHash [32]byte
	binary.BigEndian.PutUint64(addrHash[:], uint64(s.i))
	s.i++
	return v3.StorageNodeKey(addrHash, nil, nil), s.value, nil
}

// A slow batch must not let the other workers run arbitrarily far ahead of the in-order consumer.
func TestFoldCommitmentV3RecordsBoundsReorderBacklog(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(2))
	const batches = 64
	var held atomic.Bool
	var ahead atomic.Uint64
	held.Store(true)
	foldCommitmentV3BatchHook = func(seq uint64) {
		if seq == 0 {
			for deadline := time.Now().Add(500 * time.Millisecond); ahead.Load() < batches-1 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
			held.Store(false)
			return
		}
		if held.Load() {
			ahead.Add(1)
		}
	}
	t.Cleanup(func() { foldCommitmentV3BatchHook = nil })

	hash := make([]byte, 32)
	value := commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1<<6 | 1<<7, Hashes: [16][]byte{6: hash, 7: hash}})
	c, _, err := foldCommitmentV3Records(t.Context(), t.TempDir(), &storageRootRecords{n: batches * commitmentV3Batch, value: value})
	require.NoError(t, err)
	require.Equal(t, uint64(batches*commitmentV3Batch), c.Records)
	require.Less(t, ahead.Load(), uint64(16), "batches dispatched while batch 0 was held")
}
