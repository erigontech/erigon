package stagedsync

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func getTmpDir() string {
	name, err := ioutil.TempDir("", "geth-tests-staged-sync")
	if err != nil {
		panic(err)
	}
	return name
}

func TestPromoteHashedStateClearState(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	generateBlocks(t, 51, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 50}, 50, 101, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 100, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{}, 50, 101, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}

func TestUnwindHashed(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	u := &UnwindState{UnwindPoint: 50}
	s := &StageState{BlockNumber: 100}
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}



func TestSnapshot2WalkByEmptyDB(t *testing.T) {
	data := []ethdb.KvData{
		{K: common.Address{1}.Bytes(), V: []byte{1}},
		{K: common.Address{2}.Bytes(), V: []byte{2}},
		{K: common.Address{3}.Bytes(), V: []byte{3}},
		{K: common.Address{4}.Bytes(), V: []byte{4}},
		{K: common.Address{5}.Bytes(), V: []byte{5}},
	}
	snapshotDB, err := ethdb.GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := ethdb.NewLMDB().InMem().MustOpen()
	kv := ethdb.NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	db:=ethdb.NewObjectDatabase(kv)
	tx,err:=db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}

	//tx, err := kv.Begin(context.Background(), nil, RW)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//c:=tx.Cursor(dbutils.PlainStateBucket)
	//i:=0
	//err = Walk(c, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println(common.Bytes2Hex(k),  " => ", common.Bytes2Hex(v))
	//	checkKV(t, k, v, data[i].K, data[i].V)
	//	i++
	//	return true, nil
	//})
	//if err!=nil {
	//	t.Fatal(err)
	//}


	err=PromoteHashedStateCleanly("", tx, os.TempDir(), nil)
	if err!=nil {
		t.Fatal(err)
	}
}