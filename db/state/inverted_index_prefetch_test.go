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
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestInvertedIndexPrefetchCursor(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, value := range []string{"one", "three"} {
			if err := tx.Put(kv.TblTracesToIdx, []byte("key"), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	}))
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	c, err := tx.RwCursorDupSort(kv.TblTracesToIdx)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Put([]byte("uncommitted"), []byte("value")))
	k, v, err := c.SeekBothExact([]byte("key"), []byte("three"))
	require.NoError(t, err)
	wantKey, wantValue := string(k), string(v)
	pairs := [][2][]byte{
		{[]byte("key"), []byte("one")},
		{[]byte("key"), []byte("two")},
		{[]byte("missing"), []byte("value")},
		{[]byte("uncommitted"), []byte("value")},
	}
	p := newInvertedIndexPrefetcher(db, kv.TblTracesToIdx, 3)
	require.NoError(t, p.open(t.Context()))
	defer p.close()
	require.NoError(t, p.prefetch(t.Context(), pairs))
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, wantKey, string(k))
	require.Equal(t, wantValue, string(v))
	v, err = tx.GetOne(kv.TblTracesToIdx, []byte("uncommitted"))
	require.NoError(t, err)
	require.Equal(t, "value", string(v))
	require.NoError(t, p.prefetch(t.Context(), nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, p.prefetch(ctx, pairs), context.Canceled)
}

type prefetchTestDB struct {
	kv.RoDB
	newCursor func(context.Context) kv.CursorDupSort
	rollback  func()
}

func (db *prefetchTestDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	return &prefetchTestTx{newCursor: func() kv.CursorDupSort { return db.newCursor(ctx) }, rollback: db.rollback}, nil
}

type prefetchTestTx struct {
	kv.Tx
	newCursor func() kv.CursorDupSort
	rollback  func()
}

func (tx *prefetchTestTx) CursorDupSort(string) (kv.CursorDupSort, error) {
	return tx.newCursor(), nil
}

func (tx *prefetchTestTx) Rollback() {
	if tx.rollback != nil {
		tx.rollback()
	}
}

type prefetchTestCursor struct {
	kv.CursorDupSort
	seekExact func([]byte) error
	seek      func([]byte, []byte) error
	close     func()
}

func (c *prefetchTestCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	if c.seekExact != nil {
		return nil, nil, c.seekExact(key)
	}
	return nil, nil, nil
}

func (c *prefetchTestCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	return nil, c.seek(key, value)
}

func (c *prefetchTestCursor) Close() {
	if c.close != nil {
		c.close()
	}
}

func TestInvertedIndexPrefetcherBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		started := make(chan [][2][]byte, workers)
		release, releaseReads := context.WithCancel(t.Context())
		defer releaseReads()
		db := &prefetchTestDB{newCursor: func(context.Context) kv.CursorDupSort {
			var batch [][2][]byte
			return &prefetchTestCursor{
				seek: func(key, value []byte) error {
					batch = append(batch, [2][]byte{key, value})
					return nil
				},
				close: func() {
					started <- batch
					<-release.Done()
				},
			}
		}}
		p := newInvertedIndexPrefetcher(db, "index", workers)
		require.NoError(t, p.open(t.Context()))
		defer p.close()
		var pairs [][2][]byte
		for key := range 7 {
			pairs = append(pairs, [2][]byte{{byte(key)}, nil})
		}
		done := make(chan error, 1)
		go func() { done <- p.prefetch(t.Context(), pairs) }()
		synctest.Wait()
		require.Len(t, started, workers)
		require.Empty(t, done, "Prefetch must wait for its readers")
		var got [][2][]byte
		for range workers {
			batch := <-started
			for i := 1; i < len(batch); i++ {
				require.Equal(t, batch[i-1][0][0]+1, batch[i][0][0])
			}
			got = append(got, batch...)
		}
		require.ElementsMatch(t, pairs, got)
		releaseReads()
		synctest.Wait()
		require.Len(t, done, 1)
		require.NoError(t, <-done)

		require.NoError(t, p.prefetch(t.Context(), pairs[:2]))
		require.Len(t, started, 2, "worker count must not exceed pair count")
		require.Len(t, <-started, 1)
		require.Len(t, <-started, 1)
		require.NoError(t, p.prefetch(t.Context(), nil))
		require.Empty(t, started)
	})
}

func TestInvertedIndexPrefetcherErrorWaitsForReaders(t *testing.T) {
	for _, lookup := range []string{"first", "range"} {
		t.Run(lookup, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				wantErr := errors.New("read failed")
				release, releaseReads := context.WithCancel(ctx)
				defer releaseReads()
				db := &prefetchTestDB{newCursor: func(context.Context) kv.CursorDupSort {
					return &prefetchTestCursor{
						seekExact: func(key []byte) error {
							if lookup == "first" && key[0] == 0 {
								return wantErr
							}
							return nil
						},
						seek: func(key, value []byte) error {
							switch key[0] {
							case 0:
								if lookup == "range" {
									return wantErr
								}
							case 1:
								<-release.Done()
							}
							return nil
						},
					}
				}}
				p := newInvertedIndexPrefetcher(db, "index", 2)
				require.NoError(t, p.open(ctx))
				defer p.close()
				done := make(chan error, 1)
				go func() { done <- p.prefetch(ctx, [][2][]byte{{{0}, nil}, {{1}, nil}}) }()
				synctest.Wait()
				require.Empty(t, done, "an error must not release buffers still used by another reader")
				releaseReads()
				synctest.Wait()
				require.Len(t, done, 1)
				require.ErrorIs(t, <-done, wantErr)
				require.NoError(t, p.prefetch(ctx, [][2][]byte{{{2}, nil}}))
			})
		})
	}
}

func TestInvertedIndexPrefetcherCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		db := &prefetchTestDB{newCursor: func(ctx context.Context) kv.CursorDupSort {
			return &prefetchTestCursor{seek: func(key, value []byte) error {
				if key[0] == 0 {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}}
		}}
		p := newInvertedIndexPrefetcher(db, "index", 1)
		require.NoError(t, p.open(ctx))
		defer p.close()
		done := make(chan error, 1)
		go func() { done <- p.prefetch(ctx, [][2][]byte{{{0}, nil}}) }()
		synctest.Wait()
		require.Empty(t, done)
		cancel()
		synctest.Wait()
		require.Len(t, done, 1)
		require.ErrorIs(t, <-done, context.Canceled)
		require.ErrorIs(t, p.prefetch(ctx, nil), context.Canceled)
		require.NoError(t, p.prefetch(t.Context(), [][2][]byte{{{1}, nil}}))
	})
}
