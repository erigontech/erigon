// Copyright 2021 The Erigon Authors
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

package downloader

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

const (
	complete   = "c"
	incomplete = "i"
)

type mdbxPieceCompletion struct {
	db        *mdbx.MdbxKV
	mu        sync.RWMutex
	completed map[infohash.T]*roaring.Bitmap
	flushed   map[infohash.T]*roaring.Bitmap
	logger    log.Logger
}

var _ storage.PieceCompletion = (*mdbxPieceCompletion)(nil)

func NewMdbxPieceCompletion(db kv.RwDB, logger log.Logger) (ret storage.PieceCompletion, err error) {
	ret = &mdbxPieceCompletion{
		db:        db.(*mdbx.MdbxKV),
		logger:    logger,
		completed: map[infohash.T]*roaring.Bitmap{},
		flushed:   map[infohash.T]*roaring.Bitmap{}}
	return
}

func (m *mdbxPieceCompletion) Get(pk metainfo.PieceKey) (cn storage.Completion, err error) {
	m.mu.RLock()
	if completed, ok := m.completed[pk.InfoHash]; ok {
		if completed.Contains(uint32(pk.Index)) {
			m.mu.RUnlock()
			return storage.Completion{
				Complete: true,
				Ok:       true,
			}, nil
		}
	}
	m.mu.RUnlock()

	err = m.db.View(context.Background(), func(tx kv.Tx) error {
		var key [infohash.Size + 4]byte
		copy(key[:], pk.InfoHash[:])
		binary.BigEndian.PutUint32(key[infohash.Size:], uint32(pk.Index))
		cn.Ok = true
		v, err := tx.GetOne(kv.BittorrentCompletion, key[:])
		if err != nil {
			return err
		}
		switch string(v) {
		case complete:
			cn.Complete = true
		case incomplete:
			cn.Complete = false
		default:
			cn.Ok = false
		}
		return nil
	})
	return
}

func (m *mdbxPieceCompletion) Set(pk metainfo.PieceKey, b bool) error {
	awaitFlush := true

	if c, err := m.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}

	persist := func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		if b {
			completed, ok := m.completed[pk.InfoHash]

			if !ok {
				completed = &roaring.Bitmap{}
				m.completed[pk.InfoHash] = completed
			}

			completed.Add(uint32(pk.Index))

			// when files are being downloaded flushing is asynchronous so we want to wait for
			// the confirm before committing to the DB.  This means that if the program is
			// abnormally terminated the piece will be re-downloaded
			if awaitFlush {
				if flushed, ok := m.flushed[pk.InfoHash]; !ok || !flushed.Contains(uint32(pk.Index)) {
					return false
				}
			}
		} else {
			if completed, ok := m.completed[pk.InfoHash]; ok {
				completed.Remove(uint32(pk.Index))
			}
		}
		return true
	}()

	if !persist {
		return nil
	}

	// if we're awaiting flush update the DB immediately so it does not
	// interfere with the timing of the background commit - may not be
	// necessary - in which case the batch can be used
	if awaitFlush {
		return m.db.Update(context.Background(), func(tx kv.RwTx) error {
			return putCompletion(tx, pk.InfoHash, uint32(pk.Index), b)
		})
	}

	// batch updates for non flushed updated as they may happen in validation and
	// there may be many if fast succession which can be slow if handled individually
	return m.db.Batch(func(tx kv.RwTx) error {
		return putCompletion(tx, pk.InfoHash, uint32(pk.Index), b)
	})
}

func putCompletion(tx kv.RwTx, infoHash infohash.T, index uint32, c bool) error {
	var key [infohash.Size + 4]byte
	copy(key[:], infoHash[:])
	binary.BigEndian.PutUint32(key[infohash.Size:], index)

	v := []byte(incomplete)
	if c {
		v = []byte(complete)
	}
	//fmt.Println("PUT", infoHash, index, c)
	return tx.Put(kv.BittorrentCompletion, key[:], v)
}

func (m *mdbxPieceCompletion) Flushed(infoHash infohash.T, flushed *roaring.Bitmap) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.db.BeginRw(context.Background())

	if err != nil {
		m.logger.Warn("[snapshots] failed to flush piece completions", "hash", infoHash, "err", err)
		return
	}

	defer tx.Rollback()

	m.putFlushed(tx, infoHash, flushed)

	err = tx.Commit()

	if err != nil {
		m.logger.Warn("[snapshots] failed to flush piece completions", "hash", infoHash, "err", err)
	}
}

func (m *mdbxPieceCompletion) putFlushed(tx kv.RwTx, infoHash infohash.T, flushed *roaring.Bitmap) {
	if completed, ok := m.completed[infoHash]; ok {
		setters := flushed.Clone()
		setters.And(completed)

		if setters.GetCardinality() > 0 {
			setters.Iterate(func(piece uint32) bool {
				// TODO deal with error (? don't remove from bitset ?)
				_ = putCompletion(tx, infoHash, piece, true)
				return true
			})
		}

		completed.AndNot(setters)

		if completed.IsEmpty() {
			delete(m.completed, infoHash)
		}
	}

	allFlushed, ok := m.flushed[infoHash]

	if !ok {
		allFlushed = &roaring.Bitmap{}
		m.flushed[infoHash] = allFlushed
	}

	allFlushed.Or(flushed)
}

func (m *mdbxPieceCompletion) Close() error {
	m.db.Close()
	return nil
}
