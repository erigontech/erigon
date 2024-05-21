package heimdall

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

type Database struct {
	db kv.RwDB

	dataDir  string
	openOnce sync.Once

	logger log.Logger
}

func NewDatabase(
	dataDir string,
	logger log.Logger,
) *Database {
	return &Database{dataDir: dataDir, logger: logger}
}

var databaseTablesCfg = kv.TableCfg{
	kv.BorCheckpoints: {},
	kv.BorMilestones:  {},
	kv.BorSpans:       {},
}

func (db *Database) open(ctx context.Context) error {
	label := kv.HeimdallDB
	dbPath := filepath.Join(db.dataDir, label.String())
	db.logger.Info("Opening Database", "label", label.String(), "path", dbPath)

	var err error
	db.db, err = mdbx.NewMDBX(db.logger).
		Label(label).
		Path(dbPath).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return databaseTablesCfg }).
		MapSize(16 * datasize.GB).
		GrowthStep(16 * datasize.MB).
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

func (db *Database) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.db.BeginRo(ctx)
}

func (db *Database) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.db.BeginRw(ctx)
}
