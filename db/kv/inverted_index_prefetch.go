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

package kv

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type InvertedIndexPrefetcher struct {
	db      RoDB
	table   string
	workers uint64
}

func NewInvertedIndexPrefetcher(db RoDB, table string, workers uint64) *InvertedIndexPrefetcher {
	return &InvertedIndexPrefetcher{db: db, table: table, workers: workers}
}

func (p *InvertedIndexPrefetcher) fetch(ctx context.Context, pairs [][2][]byte) error {
	tx, err := p.db.BeginRo(WithNonBlockingAcquire(ctx))
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.CursorDupSort(p.table)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, _, err := c.SeekExact(pair[0]); err != nil {
			return err
		}
		if _, err := c.SeekBothRange(pair[0], pair[1]); err != nil {
			return err
		}
	}
	return nil
}

func (p *InvertedIndexPrefetcher) Prefetch(ctx context.Context, pairs [][2][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	parts := int(min(p.workers, uint64(len(pairs))))
	var g errgroup.Group
	for part := range parts {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.fetch(ctx, pairs[len(pairs)*part/parts:len(pairs)*(part+1)/parts])
		})
	}
	return g.Wait()
}
