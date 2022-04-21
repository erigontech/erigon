package torrentcfg

import (
	"context"
	"encoding/binary"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	boltDbCompleteValue   = "c"
	boltDbIncompleteValue = "i"
)

type boltPieceCompletion struct {
	db kv.RwDB
}

var _ storage.PieceCompletion = (*boltPieceCompletion)(nil)

func NewBoltPieceCompletion(db kv.RwDB) (ret storage.PieceCompletion, err error) {
	ret = &boltPieceCompletion{db}
	return
}

func (me boltPieceCompletion) Get(pk metainfo.PieceKey) (cn storage.Completion, err error) {
	err = me.db.View(context.Background(), func(tx kv.Tx) error {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))
		cn.Ok = true
		v, err := tx.GetOne(kv.BittorrentCompletion, append(pk.InfoHash[:], key[:]...))
		if err != nil {
			return err
		}
		switch string(v) {
		case boltDbCompleteValue:
			cn.Complete = true
		case boltDbIncompleteValue:
			cn.Complete = false
		default:
			cn.Ok = false
		}
		return nil
	})
	return
}

func (me boltPieceCompletion) Set(pk metainfo.PieceKey, b bool) error {
	if c, err := me.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}
	return me.db.Update(context.Background(), func(tx kv.RwTx) error {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))

		v := []byte(boltDbIncompleteValue)
		if b {
			v = []byte(boltDbCompleteValue)
		}
		err := tx.Put(kv.BittorrentCompletion, append(pk.InfoHash[:], key[:]...), v)
		if err != nil {
			return err
		}
		return nil
	})
}

func (me *boltPieceCompletion) Close() error {
	me.db.Close()
	return nil
}
