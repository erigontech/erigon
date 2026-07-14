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

package integrity

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func TestParallelChunkCheckUsesHalfOpenRanges(t *testing.T) {
	sampler, err := NewSamplerCfg(1, 1)
	if err != nil {
		t.Fatalf("NewSamplerCfg: %v", err)
	}

	type chunk struct {
		from uint64
		to   uint64
	}

	var (
		mu  sync.Mutex
		got []chunk
	)
	err = parallelChunkCheck(context.Background(), sampler.NewSampler(), 1, 203, nil, nil, true, "test", func(_ context.Context, fromBlock, toBlock uint64, _ kv.TemporalRoDB, _ services.FullBlockReader, _ bool) error {
		mu.Lock()
		got = append(got, chunk{from: fromBlock, to: toBlock})
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("parallelChunkCheck: %v", err)
	}

	sort.Slice(got, func(i, j int) bool {
		return got[i].from < got[j].from
	})

	want := []chunk{
		{from: 1, to: 101},
		{from: 101, to: 201},
		{from: 201, to: 203},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d chunks, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d: expected %+v, got %+v", i, want[i], got[i])
		}
	}
}

func TestParallelChunkCheckSkipsEmptyHalfOpenRange(t *testing.T) {
	sampler, err := NewSamplerCfg(1, 1)
	if err != nil {
		t.Fatalf("NewSamplerCfg: %v", err)
	}

	called := false
	err = parallelChunkCheck(context.Background(), sampler.NewSampler(), 10, 10, nil, nil, true, "test", func(_ context.Context, fromBlock, toBlock uint64, _ kv.TemporalRoDB, _ services.FullBlockReader, _ bool) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("parallelChunkCheck: %v", err)
	}
	if called {
		t.Fatal("expected empty half-open range to skip all chunks")
	}
}
