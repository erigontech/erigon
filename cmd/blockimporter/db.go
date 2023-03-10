package main

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/log/v3"
)

type DB struct {
	chain     kv.RwDB
	pool      kv.RwDB
	consensus kv.RwDB
}

func NewDB(path string, logger log.Logger) (*DB, error) {
	db := DB{}
	var err error

	db.chain, err = openDatabase(path, logger, kv.ChainDB)
	if err != nil {
		db.Close()
		return nil, err
	}

	db.pool, err = openDatabase(path, logger, kv.TxPoolDB)
	if err != nil {
		db.Close()
		return nil, err
	}

	db.consensus, err = openDatabase(path, logger, kv.ConsensusDB)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &db, nil
}

func (db *DB) Close() {
	close := func(db kv.RwDB) {
		if db != nil {
			db.Close()
		}
	}

	close(db.chain)
	close(db.pool)
	close(db.consensus)
}

func (db *DB) GetChain() kv.RwDB {
	return db.chain
}

func openDatabase(path string, logger log.Logger, label kv.Label) (kv.RwDB, error) {
	config := nodecfg.DefaultConfig
	config.Dirs.DataDir = path
	return node.OpenDatabase(&config, logger, label)
}
