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
	"path/filepath"
	"sync"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

type Database struct {
	db        kv.RwDB
	dataDir   string
	label     kv.Label
	tableCfg  kv.TableCfg
	openOnce  sync.Once
	logger    log.Logger
	accede    bool
	rwTxLimit int64
}

func NewDatabase(dataDir string, label kv.Label, tableCfg kv.TableCfg, logger log.Logger, accede bool, rwTxLimit int64) *Database {
	return &Database{
		dataDir:   dataDir,
		label:     label,
		tableCfg:  tableCfg,
		logger:    logger,
		accede:    accede,
		rwTxLimit: rwTxLimit,
	}
}

func (db *Database) open(ctx context.Context) error {
	dbPath := filepath.Join(db.dataDir, db.label.String())
	db.logger.Info("Opening Database", "label", db.label.String(), "path", dbPath)

	var err error
	opts := mdbx.NewMDBX(db.logger).
		Label(db.label).
		Path(dbPath).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return db.tableCfg }).
		MapSize(16 * datasize.GB).
		GrowthStep(16 * datasize.MB).
		RoTxsLimiter(semaphore.NewWeighted(db.rwTxLimit))

	if db.accede {
		opts = opts.Accede()
	}

	db.db, err = opts.Open(ctx)
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
