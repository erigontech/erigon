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
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/kv"
)

type invertedIndexPrefetcher struct {
	db      kv.RoDB
	table   string
	workers uint64
	txs     []kv.Tx
}

func newInvertedIndexPrefetcher(db kv.RoDB, table string, workers uint64) *invertedIndexPrefetcher {
	return &invertedIndexPrefetcher{db: db, table: table, workers: workers}
}

func (p *invertedIndexPrefetcher) open(ctx context.Context) error {
	p.txs = make([]kv.Tx, 0, p.workers)
	for range p.workers {
		tx, err := p.db.BeginRo(kv.WithNonBlockingAcquire(ctx)) //nolint:gocritic // owned until the flush closes the prefetcher.
		if err != nil {
			p.close()
			return err
		}
		p.txs = append(p.txs, tx)
	}
	return nil
}

func (p *invertedIndexPrefetcher) close() {
	for _, tx := range p.txs {
		tx.Rollback()
	}
	p.txs = nil
}

func (p *invertedIndexPrefetcher) prefetch(ctx context.Context, pairs [][2][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	parts := min(len(p.txs), len(pairs))
	var g errgroup.Group
	for part := range parts {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.fetch(ctx, p.txs[part], pairs[len(pairs)*part/parts:len(pairs)*(part+1)/parts])
		})
	}
	return g.Wait()
}

func (p *invertedIndexPrefetcher) fetch(ctx context.Context, tx kv.Tx, pairs [][2][]byte) error {
	c, err := tx.CursorDupSort(p.table)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// we do SeekExact here on purpose to warm the first duplicate's leaf page,
		// which mdbx put reads even when the insertion position is on another page.
		if _, _, err := c.SeekExact(pair[0]); err != nil {
			return err
		}
		if _, err := c.SeekBothRange(pair[0], pair[1]); err != nil {
			return err
		}
	}
	return nil
}
