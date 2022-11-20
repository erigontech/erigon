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
	ret = &mdbxPieceCompletion{db}
	return
}

func (me mdbxPieceCompletion) Get(pk metainfo.PieceKey) (cn storage.Completion, err error) {
	err = me.db.View(context.Background(), func(tx kv.Tx) error {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))
		cn.Ok = true
		v, err := tx.GetOne(kv.BittorrentCompletion, append(pk.InfoHash[:], key[:]...))
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

func (me mdbxPieceCompletion) Set(pk metainfo.PieceKey, b bool) error {
	if c, err := me.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}
	return me.db.Update(context.Background(), func(tx kv.RwTx) error {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))

		v := []byte(incomplete)
		if b {
			v = []byte(complete)
		}
		err := tx.Put(kv.BittorrentCompletion, append(pk.InfoHash[:], key[:]...), v)
		if err != nil {
			return err
		}
		return nil
	})
}

func (me *mdbxPieceCompletion) Close() error {
	me.db.Close()
	return nil
}
