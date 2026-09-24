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

package v4

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type meterCounts struct{ readBytes, writeBytes, writes atomic.Uint64 }

type meteredContext struct {
	commitment.PatriciaContext
	*meterCounts
}

func (c *meteredContext) BranchOwned(prefix []byte) ([]byte, kv.Step, error) {
	data, step, err := branchOwned(c.PatriciaContext, prefix)
	if len(data) != 0 {
		c.readBytes.Add(uint64(len(data)))
	}
	return data, step, err
}

type ownedBrancher interface {
	BranchOwned(prefix []byte) ([]byte, kv.Step, error)
}

func branchOwned(ctx commitment.PatriciaContext, prefix []byte) ([]byte, kv.Step, error) {
	if o, ok := ctx.(ownedBrancher); ok {
		return o.BranchOwned(prefix)
	}
	data, step, err := ctx.Branch(prefix)
	return bytes.Clone(data), step, err
}

func (c *meteredContext) countDeltas(parts deltaParts) {
	size := 0
	for _, part := range parts {
		c.writes.Add(uint64(len(part)))
		for i := range part {
			size += len(part[i].Data)
		}
	}
	c.writeBytes.Add(uint64(size))
}

func (c *meteredContext) wrapFactory(f commitment.TrieContextFactory) commitment.TrieContextFactory {
	if f == nil {
		return nil
	}
	return func(ctx context.Context) (commitment.PatriciaContext, func()) {
		inner, cleanup := f(ctx)
		if inner == nil {
			return nil, cleanup
		}
		return &meteredContext{inner, c.meterCounts}, cleanup
	}
}

func (c *meteredContext) publish(start time.Time, keys uint64) {
	m := commitment.NewMetrics("")
	m.AddRoundKeys(keys)
	m.AddBranchRead(int(c.readBytes.Load()))
	commitment.ObserveRound(m, start)
	commitment.PublishBranchWrites(int(c.writes.Load()), int(c.writeBytes.Load()), nil)
}
