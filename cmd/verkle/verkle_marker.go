package main

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

type VerkleMarker struct {
	db kv.RwDB
	tx kv.RwTx
}

func NewVerkleMarker() *VerkleMarker {
	markedSlotsDb, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		panic(err)
	}

	tx, err := markedSlotsDb.BeginRw(context.TODO())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	return &VerkleMarker{
		db: markedSlotsDb,
		tx: tx,
	}
}

func (v *VerkleMarker) MarkAsDone(key []byte) error {
	return v.tx.Put(kv.Headers, key, []byte{0})
}

func (v *VerkleMarker) IsMarked(key []byte) (bool, error) {
	return v.tx.Has(kv.Headers, key)
}

func (v *VerkleMarker) Close() {
	v.tx.Rollback()
	v.db.Close()
}
