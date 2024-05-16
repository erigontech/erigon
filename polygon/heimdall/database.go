package heimdall

import (
	"context"
	"sync"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type OpenDatabaseFunc func(ctx context.Context, name string, logger log.Logger) (kv.RwDB, error)

type Database struct {
	db kv.RwDB

	openDatabase OpenDatabaseFunc
	openOnce     sync.Once

	logger log.Logger
}

func NewDatabase(
	openDatabase OpenDatabaseFunc,
	logger log.Logger,
) *Database {
	return &Database{openDatabase: openDatabase, logger: logger}
}

func (db *Database) open(ctx context.Context) error {
	var err error
	db.db, err = db.openDatabase(ctx, "heimdall", db.logger)
	return err
}

func (db *Database) OpenOnce(ctx context.Context) error {
	var err error
	db.openOnce.Do(func() {
		err = db.open(ctx)
	})
	return err
}

func (db *Database) Close() {
	if db.db != nil {
		db.db.Close()
	}
}

func (db *Database) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.db.BeginRo(ctx)
}

func (db *Database) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.db.BeginRw(ctx)
}
