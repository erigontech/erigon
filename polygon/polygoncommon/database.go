// Copyright 2024 The Erigon Authors
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

package polygoncommon

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

type Database struct {
	db        kv.RoDB
	dataDir   string
	label     kv.Label
	tableCfg  kv.TableCfg
	openOnce  sync.Once
	logger    log.Logger
	accede    bool
	roTxLimit int64
}

func NewDatabase(dataDir string, label kv.Label, tableCfg kv.TableCfg, logger log.Logger, accede bool, roTxLimit int64) *Database {
	return &Database{
		dataDir:   dataDir,
		label:     label,
		tableCfg:  tableCfg,
		logger:    logger,
		accede:    accede,
		roTxLimit: roTxLimit,
	}
}

func AsDatabase(db kv.RoDB) *Database {
	return &Database{db: db}
}

func (db *Database) open(ctx context.Context) error {
	dbPath := filepath.Join(db.dataDir, string(db.label))
	db.logger.Info("Opening Database", "label", db.label, "path", dbPath)

	var txLimiter *semaphore.Weighted

	if db.roTxLimit > 0 {
		txLimiter = semaphore.NewWeighted(db.roTxLimit)
	}

	var err error
	db.db, err = mdbx.New(db.label, db.logger).
		Path(dbPath).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return db.tableCfg }).
		MapSize(16 * datasize.GB).
		GrowthStep(16 * datasize.MB).
		RoTxsLimiter(txLimiter).
		Accede(db.accede).
		Open(ctx)
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

func (db *Database) RoDB() kv.RoDB {
	return db.db
}

func (db *Database) RwDB() kv.RwDB {
	return db.db.(kv.RwDB)
}

func (db *Database) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.db.BeginRo(ctx)
}

func (db *Database) BeginRw(ctx context.Context) (kv.RwTx, error) {
	if db, ok := db.db.(kv.RwDB); ok {
		return db.BeginRw(ctx)
	}

	return nil, errors.New("db is read only")
}

func (db *Database) View(ctx context.Context, f func(tx kv.Tx) error) error {
	return db.db.View(ctx, f)
}
