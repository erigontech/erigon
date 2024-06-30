/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloader

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/log/v3"
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
	if c, err := m.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var tx kv.RwTx
	var err error
	// On power-off recent "no-sync" txs may be lost.
	// It will cause 2 cases of in-consistency between files on disk and db metadata:
	//  - Good piece on disk and recent "complete"   db marker lost. Self-Heal by re-download.
	//  - Bad  piece on disk and recent "incomplete" db marker lost. No Self-Heal. Means: can't afford loosing recent "incomplete" markers.
	// FYI: Fsync of torrent pieces happenng before storing db markers: https://github.com/anacrolix/torrent/blob/master/torrent.go#L2026
	//
	// Mainnet stats:
	//  call amount 2 minutes complete=100K vs incomple=1K
	//  1K fsyncs/2minutes it's quite expensive, but even on cloud (high latency) drive it allow download 100mb/s
	//  and Erigon doesn't do anything when downloading snapshots
	if b {
		completed, ok := m.completed[pk.InfoHash]

		if !ok {
			completed = &roaring.Bitmap{}
			m.completed[pk.InfoHash] = completed
		}

		completed.Add(uint32(pk.Index))

		if flushed, ok := m.flushed[pk.InfoHash]; !ok || !flushed.Contains(uint32(pk.Index)) {
			return nil
		}
	}

	tx, err = m.db.BeginRw(context.Background())
	if err != nil {
		return err
	}

	defer tx.Rollback()

	err = putCompletion(tx, pk.InfoHash, uint32(pk.Index), b)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func putCompletion(tx kv.RwTx, infoHash infohash.T, index uint32, c bool) error {
	fmt.Println("PUT", infoHash, index)
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
		m.logger.Warn("[snapshots] failed to flush piece completions", "hash", infoHash, err, err)
		return
	}

	defer tx.Rollback()

	m.putFlushed(tx, infoHash, flushed)

	err = tx.Commit()

	if err != nil {
		m.logger.Warn("[snapshots] failed to flush piece completions", "hash", infoHash, err, err)
	}
}

func (m *mdbxPieceCompletion) putFlushed(tx kv.RwTx, infoHash infohash.T, flushed *roaring.Bitmap) {
	flushed.Iterate(func(piece uint32) bool {
		fmt.Println("FLSH", infoHash, piece)
		return true
	})

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
