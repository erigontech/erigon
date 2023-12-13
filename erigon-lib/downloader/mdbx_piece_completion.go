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

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	complete   = "c"
	incomplete = "i"
)

type mdbxPieceCompletion struct {
	db kv.RwDB
}

var _ storage.PieceCompletion = (*mdbxPieceCompletion)(nil)

func NewMdbxPieceCompletion(db kv.RwDB) (ret storage.PieceCompletion, err error) {
	ret = &mdbxPieceCompletion{db: db}
	return
}

func (m mdbxPieceCompletion) Get(pk metainfo.PieceKey) (cn storage.Completion, err error) {
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

func (m mdbxPieceCompletion) Set(pk metainfo.PieceKey, b bool) error {
	if c, err := m.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}

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
		tx, err = m.db.BeginRwNosync(context.Background())
		if err != nil {
			return err
		}
	} else {
		tx, err = m.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
	}
	defer tx.Rollback()

	var key [infohash.Size + 4]byte
	copy(key[:], pk.InfoHash[:])
	binary.BigEndian.PutUint32(key[infohash.Size:], uint32(pk.Index))

	v := []byte(incomplete)
	if b {
		v = []byte(complete)
	}
	err = tx.Put(kv.BittorrentCompletion, key[:], v)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (m *mdbxPieceCompletion) Close() error {
	m.db.Close()
	return nil
}
