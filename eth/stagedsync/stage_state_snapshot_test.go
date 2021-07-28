package stagedsync

import (
	"context"
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
	_, tx2 := memdb.NewTestTx(t)
	_, tx3 := memdb.NewTestTx(t)

	//generate 50 block for snapshot and 50 in tmp db
	tx1, err := db.BeginRw(ctx)
	assert.NoError(err)
	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeWithIncarnations)
	err = tx1.Commit()
	assert.NoError(err)
	tmpDB := memdb.NewTestDB(t)
	db.(*snapshotdb.SnapshotKV).SetTempDB(tmpDB, snapshotsync.StateSnapshotBuckets)

	tx1, err = db.BeginRw(ctx)
	assert.NoError(err)
	generateBlocks(t, 51, 70, plainWriterGen(tx1), changeCodeWithIncarnations)
	err = stages.SaveStageProgress(tx1, stages.Execution, 70)
	assert.NoError(err)

	err = tx1.Commit()
	assert.NoError(err)

	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 70, plainWriterGen(tx3), changeCodeWithIncarnations)

	s := &StageState{ID: stages.CreateStateSnapshot, BlockNumber: 50}
	err = SpawnStateSnapshotGenerationStage(s, nil, SnapshotStateCfg{
		enabled:          true,
		db:               db,
		snapshotDir:      t.TempDir(),
		tmpDir:           t.TempDir(),
		client:           nil,
		snapshotMigrator: nil,
	}, ctx, true, 50)
	if err != nil {
		t.Fatal(err)
	}

	stateSnapshotTX, err := db.(*snapshotdb.SnapshotKV).StateSnapshot().BeginRo(ctx)
	assert.NoError(err)
	defer stateSnapshotTX.Rollback()
	compareCurrentState(t, stateSnapshotTX, tx2, kv.PlainStateBucket, kv.PlainContractCode)

	tx1, err = db.(*snapshotdb.SnapshotKV).WriteDB().BeginRw(ctx)
	assert.NoError(err)
	defer tx1.Rollback()

	compareCurrentState(t, tx1, tx3, kv.PlainStateBucket)
}
