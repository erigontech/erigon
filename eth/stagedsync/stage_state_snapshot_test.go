package stagedsync

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/log"
	"testing"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/memdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotGeneration(t *testing.T) {
	ctx, assert := context.Background(), assert.New(t)
	db := memdb.NewTestDB(t)
	db = snapshotdb.NewSnapshotKV().DB(db).Open()
	_, tx1to4Expected := memdb.NewTestTx(t)
	_, tx1to6Expected := memdb.NewTestTx(t)
	_, txWriteDB4to6Expected := memdb.NewTestTx(t)
	defer txWriteDB4to6Expected.Rollback()

	//generate 50 block for snapshot and 50 in tmp db
	tx1to6WithSnapshot, err := db.BeginRw(ctx)
	assert.NoError(err)
	generateBlocks(t, 1, 3, plainWriterGen(tx1to6WithSnapshot), changeCodeWithIncarnations)
	err = tx1to6WithSnapshot.Commit()
	assert.NoError(err)
	tmpDB := memdb.NewTestDB(t)
	db.(*snapshotdb.SnapshotKV).SetTempDB(tmpDB, snapshotsync.StateSnapshotBuckets)

	tx1to6WithSnapshot, err = db.BeginRw(ctx)
	assert.NoError(err)
	generateBlocks(t, 1, 3, plainWriterGen(tx1to4Expected), changeCodeWithIncarnations)
	generateBlocks(t, 4, 2, plainWriterGen(tx1to6WithSnapshot), changeCodeWithIncarnations)
	generateBlocks(t, 4, 2, plainWriterGen(txWriteDB4to6Expected), changeCodeWithIncarnations)
	err = stages.SaveStageProgress(tx1to6WithSnapshot, stages.Execution, 5)
	assert.NoError(err)

	err = tx1to6WithSnapshot.Commit()
	assert.NoError(err)

	generateBlocks(t, 1, 5, plainWriterGen(tx1to6Expected), changeCodeWithIncarnations)

	s := &StageState{ID: stages.CreateStateSnapshot, BlockNumber: 3}
	err = SpawnStateSnapshotGenerationStage(s, nil, SnapshotStateCfg{
		enabled:          true,
		db:               db,
		snapshotDir:      t.TempDir(),
		tmpDir:           t.TempDir(),
		client:           nil,
		snapshotMigrator: nil,
		log:              log.New(),
		epochSize: 3,
	}, ctx, true,)
	if err != nil {
		t.Fatal(err)
	}

	stateSnapshotTX, err := db.(*snapshotdb.SnapshotKV).StateSnapshot().BeginRo(ctx)
	assert.NoError(err)
	defer stateSnapshotTX.Rollback()
	compareCurrentState(t, stateSnapshotTX, tx1to4Expected, kv.PlainState, kv.PlainContractCode)
	fmt.Println("tx1to6WithSnapshot")

	tx1to6WithSnapshot, err = db.(*snapshotdb.SnapshotKV).BeginRw(ctx)
	assert.NoError(err)
	defer tx1to6WithSnapshot.Rollback()

	compareCurrentState(t, tx1to6WithSnapshot, tx1to6Expected, kv.PlainState, kv.PlainContractCode)
	tx1to6WithSnapshot.Rollback()
	writeDB, err := db.(*snapshotdb.SnapshotKV).WriteDB().BeginRo(ctx)
	assert.NoError(err)
	defer writeDB.Rollback()

	compareCurrentState(t, writeDB, txWriteDB4to6Expected, kv.PlainState, kv.PlainContractCode)
}
