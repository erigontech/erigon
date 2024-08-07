package stages

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/state"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	smtNs "github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type stageDb struct {
	ctx context.Context
	db  kv.RwDB

	tx          kv.RwTx
	hermezDb    *hermez_db.HermezDb
	eridb       *db2.EriDb
	stateReader *state.PlainStateReader
	smt         *smtNs.SMT
}

func newStageDb(ctx context.Context, db kv.RwDB) (sdb *stageDb, err error) {
	var tx kv.RwTx
	if tx, err = db.BeginRw(ctx); err != nil {
		return nil, err
	}

	sdb = &stageDb{
		ctx: ctx,
		db:  db,
	}
	sdb.SetTx(tx)
	return sdb, nil
}

func (sdb *stageDb) SetTx(tx kv.RwTx) {
	sdb.tx = tx
	sdb.hermezDb = hermez_db.NewHermezDb(tx)
	sdb.eridb = db2.NewEriDb(tx)
	sdb.stateReader = state.NewPlainStateReader(tx)
	sdb.smt = smtNs.NewSMT(sdb.eridb, false)
}

func (sdb *stageDb) CommitAndStart() (err error) {
	if err = sdb.tx.Commit(); err != nil {
		return err
	}

	tx, err := sdb.db.BeginRw(sdb.ctx)
	if err != nil {
		return err
	}

	sdb.SetTx(tx)
	return nil
}
