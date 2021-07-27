package stagedsync

import (
	"context"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSnapshotGeneration(t *testing.T) {
	ctx, assert := context.Background(), assert.New(t)
	rwKV:= kv.NewTestKV(t)
	rwKV = kv.NewSnapshotKV().DB(rwKV).Open()
	_, tx2 := kv.NewTestTx(t)
	_, tx3 := kv.NewTestTx(t)

	//generate 50 block for snapshot and 50 in tmp db
	tx1,err := rwKV.BeginRw(ctx)
	assert.NoError(err)
	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeWithIncarnations)
	err = tx1.Commit()
	assert.NoError(err)
	tmpDB:= kv.NewTestKV(t)
	rwKV.(*kv.SnapshotKV).SetTempDB(tmpDB, snapshotsync.StateSnapshotBuckets)

	tx1,err = rwKV.BeginRw(ctx)
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
		enabled: true,
		db: rwKV,
		snapshotDir: t.TempDir(),
		tmpDir: t.TempDir(),
		client: nil,
		snapshotMigrator: nil,
	}, ctx, true, 50)
	if err!=nil {
		t.Fatal(err)
	}

	stateSnapshotTX,err:=rwKV.(*kv.SnapshotKV).StateSnapshot().BeginRo(ctx)
	assert.NoError(err)
	defer stateSnapshotTX.Rollback()
	compareCurrentState(t, stateSnapshotTX, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)

	tx1,err = rwKV.(*kv.SnapshotKV).WriteDB().BeginRw(ctx)
	assert.NoError(err)
	defer tx1.Rollback()

	compareCurrentState(t, tx1, tx3, dbutils.PlainStateBucket)
}
