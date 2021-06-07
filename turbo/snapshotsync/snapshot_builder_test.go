package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
	"math"
	"math/big"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

//Testcase plan
//Step 1. Generate headers from 0 to 11.
//Step 2. Run in a separate goroutine snapshot with epoch 10 blocks.
//Step 3. Wait until the snapshot builder passes the first cycle. It must generate a new snapshot and remove duplicate data from
//the main database. After it we must check that headers from 0 to 10 is in snapshot and headers 11 is in the main DB.
//Stap 4. Begin new Ro tx and generate data from 11 to 20. Snapshot migration must be blocked by Ro tx on the replace snapshot stage.
//After 3 seconds, we rollback Ro tx, and migration must continue without any errors.
// Step 5. We need to check that the new snapshot contains headers from 0 to 20, the headers bucket in the main database is empty,
// it started seeding a new snapshot and removed the old one.
func TestSnapshotMigratorStage(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	var err error
	dir := t.TempDir()

	defer func() {
		if err != nil {
			t.Log(err, dir)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Log(err)
		}

	}()
	snapshotsDir := path.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}

	db := ethdb.NewSnapshotKV().DB(ethdb.MustOpenKV(path.Join(dir, "chaindata"))).Open()
	quit := make(chan struct{})
	defer func() {
		close(quit)
	}()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
		replaceChan:  make(chan struct{}),
		useMdbx:      true,
	}
	currentSnapshotBlock := uint64(10)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Error(err)
		panic(err)
	}
	defer tx.Rollback()
	err = GenerateHeaderData(tx, 0, 11)
	if err != nil {
		t.Error(err)
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Error(err)
		panic(err)
	}
	generateChan := make(chan int)
	StageSyncStep := func() {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Error(err)
			panic(err)
		}
		defer tx.Rollback()

		select {
		case newHeight := <-generateChan:
			err = GenerateHeaderData(tx, int(currentSnapshotBlock), newHeight)
			if err != nil {
				t.Error(err)
			}
			currentSnapshotBlock = CalculateEpoch(uint64(newHeight), 10)
		default:

		}

		err = sb.AsyncStages(currentSnapshotBlock, db, tx, btCli, true)
		if err != nil {
			t.Error(err)
			panic(err)
		}

		err = sb.SyncStages(currentSnapshotBlock, db, tx)
		if err != nil {
			t.Error(err)
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Error(err)
			panic(err)
		}
		roTX,err:=db.BeginRo(context.Background())
		if err!=nil {
		    t.Error(err)
		}

		err = sb.Final(roTX)
		if err != nil {
			t.Error(err)
			panic(err)
		}
		roTX.Rollback()
		time.Sleep(time.Second)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		so := sync.Once{}
		//this gorutine emulates staged sync.
		for {
			select {
			case <-quit:
				db.Close()
				return
			default:
				StageSyncStep()
				//mark that migration started
				so.Do(func() {
					wg.Done()
				})
			}
		}
	}()
	//wait until migration start
	wg.Wait()
	for atomic.LoadUint64(&sb.started) > 0 {
		time.Sleep(time.Millisecond * 100)
	}

	//note. We need here only main database.
	rotx, err := db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	roc, err := rotx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	var headerNumber uint64
	headerNumber = 11

	err = ethdb.Walk(roc, []byte{}, 0, func(k, v []byte) (bool, error) {
		require.Equal(t, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)}), k)
		headerNumber++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}
	rotx.Rollback()

	snokv := db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)
	snRoTx, err := snokv.BeginRo(context.Background())
	require.NoError(t, err)
	headersCursor, err := snRoTx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	headerNumber = 0
	err = ethdb.Walk(headersCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 11 {
		t.Fatal(headerNumber)
	}

	headerNumber = 0
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		headersC, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer headersC.Close()

		return ethdb.ForEach(headersC, func(k, v []byte) (bool, error) {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return true, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}

	trnts := btCli.Torrents()
	if len(trnts) != 1 {
		t.Fatal("incorrect len", trnts)
	}

	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(v) != 10 {
			t.Fatal("incorrect snapshot")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	roTX, err := db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	//just start snapshot transaction
	// it can't be empty slice but shouldn't be in main db
	_, err = roTX.GetOne(dbutils.HeadersBucket, []byte{112, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer roTX.Rollback()

	generateChan <- 20

	rollbacked := false
	//3s - just to be sure that it blocks here
	c := time.After(time.Second * 5)
	for atomic.LoadUint64(&sb.started) > 0 || atomic.LoadUint64(&sb.HeadersCurrentSnapshot) != 20 {
		select {
		case <-c:
			roTX.Rollback()
			rollbacked = true
		default:
		}
		time.Sleep(time.Millisecond * 100)
	}
	if !rollbacked {
		t.Log("it's not possible to close db without rollback. something went wrong")
	}

	rotx, err = db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	roc, err = rotx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)

	err = ethdb.Walk(roc, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here", k)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	headerNumber = 0
	snRoTx, err = db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV).BeginRo(context.Background())
	require.NoError(t, err)
	headersCursor, err = snRoTx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	err = ethdb.Walk(headersCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}
	headerNumber = 0
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		return ethdb.ForEach(c, func(k, v []byte) (bool, error) {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++

			return true, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}

	trnts = btCli.Torrents()
	if len(trnts) != 1 {
		t.Fatal("incorrect len", trnts)
	}
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(v) != 20 {
			t.Fatal("incorrect snapshot")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	} else {
		//just not to confuse defer
		err = nil
	}
}

func TestSnapshotMigratorStageSyncMode(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	var err error
	dir := t.TempDir()

	defer func() {
		if err != nil {
			t.Log(err, dir)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Log(err)
		}

	}()
	snapshotsDir := path.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}

	db := ethdb.NewSnapshotKV().DB(ethdb.MustOpenKV(path.Join(dir, "chaindata"))).Open()
	quit := make(chan struct{})
	defer func() {
		close(quit)
	}()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
		replaceChan:  make(chan struct{}),
		useMdbx:      true,
	}

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Error(err)
		panic(err)
	}
	defer tx.Rollback()
	err = GenerateHeaderData(tx, 0, 11)
	if err != nil {
		t.Error(err)
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Error(err)
		panic(err)
	}

	writeStep := func(currentSnapshotBlock uint64) {
		writeTX, writeErr := db.BeginRw(context.Background())
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		defer writeTX.Rollback()
		writeErr = sb.SyncStages(currentSnapshotBlock, db, writeTX)
		if writeErr != nil {
			t.Fatal(writeErr)
		}

		writeErr = writeTX.Commit()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	StageSyncStep := func(currentSnapshotBlock uint64) {
		t.Helper()
		rotx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Error(err)
			panic(err)
		}
		defer rotx.Rollback()

		err = sb.AsyncStages(currentSnapshotBlock, db, rotx, btCli, false)
		if err != nil {
			t.Fatal(err)
		}
		rotx.Rollback()
	}
	StageSyncStep(10)
	for !sb.Replaced() {
		//wait until all txs of old snapshot closed
	}
	writeStep(10)

	for atomic.LoadUint64(&sb.started) > 0 {
		roTx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = sb.Final(roTx)
		if err != nil {
			t.Fatal(err)
		}
		roTx.Rollback()
		time.Sleep(time.Millisecond * 100)
	}

	//note. We need here only main database.
	rotx, err := db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	roc, err := rotx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	var headerNumber uint64
	headerNumber = 11

	err = ethdb.Walk(roc, []byte{}, 0, func(k, v []byte) (bool, error) {
		require.Equal(t, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)}), k)
		headerNumber++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}
	rotx.Rollback()

	snokv := db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)
	snRoTx, err := snokv.BeginRo(context.Background())
	require.NoError(t, err)
	headersCursor, err := snRoTx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	headerNumber = 0
	err = ethdb.Walk(headersCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 11 {
		t.Fatal(headerNumber)
	}

	headerNumber = 0
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		headersC, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer headersC.Close()

		return ethdb.ForEach(headersC, func(k, v []byte) (bool, error) {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return true, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}

	trnts := btCli.Torrents()
	if len(trnts) != 1 {
		t.Fatal("incorrect len", trnts)
	}

	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(v) != 10 {
			t.Fatal("incorrect snapshot")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = GenerateHeaderData(tx, 12, 20)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	rollbacked := false
	go func() {
		c := time.After(time.Second * 3)

		roTX, err := db.BeginRo(context.Background())
		if err != nil {
			wg.Done()

			t.Error(err)
		}
		//just start snapshot transaction
		// it can't be empty slice but shouldn't be in main db
		_, err = roTX.GetOne(dbutils.HeadersBucket, []byte{1})
		if err != nil {
			wg.Done()
			t.Error(err)
		}
		wg.Done()
		<-c
		rollbacked = true
		roTX.Rollback()
	}()
	//wait until read tx start
	wg.Wait()

	StageSyncStep(20)
	for !sb.Replaced() {
		//wait until all txs of old snapshot closed
	}
	writeStep(20)

	for atomic.LoadUint64(&sb.started) > 0 {
		roTx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		err = sb.Final(roTx)
		if err != nil {
			t.Fatal(err)
		}
		roTx.Rollback()
		time.Sleep(time.Millisecond * 1000)
	}

	if !rollbacked {
		t.Log("it's not possible to close db without rollback. something went wrong")
	}

	rotx, err = db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	roc, err = rotx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)

	err = ethdb.Walk(roc, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here", k)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rotx.Rollback()
	headerNumber = 0
	snRoTx, err = db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV).BeginRo(context.Background())
	require.NoError(t, err)
	headersCursor, err = snRoTx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	err = ethdb.Walk(headersCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}
	headerNumber = 0
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		return ethdb.ForEach(c, func(k, v []byte) (bool, error) {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++

			return true, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}

	trnts = btCli.Torrents()
	if len(trnts) != 1 {
		t.Fatal("incorrect len", trnts)
	}
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(v) != 20 {
			t.Fatal("incorrect snapshot")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	} else {
		//just not to confuse defer
		err = nil
	}

}

func GenerateHeaderData(tx ethdb.RwTx, from, to int) error {
	var err error
	if to > math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i := from; i <= to; i++ {
		err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(uint64(i), common.Hash{uint8(i)}), []byte{uint8(i), uint8(i), uint8(i)})
		if err != nil {
			return err
		}
		err = tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(uint64(i)), common.Hash{uint8(i)}.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func GenerateBodyData(tx ethdb.RwTx, from, to uint64) error {
	var err error
	if to > math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i := from; i <= to; i++ {
		for blockNum := 1; blockNum < 4; blockNum++ {
			bodyForStorage := new(types.BodyForStorage)
			baseTxId, err := tx.IncrementSequence(dbutils.EthTx, 3)
			if err != nil {
				return err
			}
			bodyForStorage.BaseTxId = baseTxId
			bodyForStorage.TxAmount = 3
			body, err := rlp.EncodeToBytes(bodyForStorage)
			if err != nil {
				return err
			}
			err = tx.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(uint64(i), common.Hash{uint8(i), uint8(blockNum)}), body)
			if err != nil {
				return err
			}
			header := &types.Header{
				Number: big.NewInt(int64(i)),
			}
			headersBytes, err := rlp.EncodeToBytes(header)
			if err != nil {
				return err
			}
			//fmt.Println("block", uint64(i), common.Hash{uint8(i), uint8(blockNum)})
			err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(uint64(i), common.Hash{uint8(i), uint8(blockNum)}), headersBytes)
			if err != nil {
				return err
			}

			genTx := func(a common.Address) ([]byte, error) {
				return rlp.EncodeToBytes(types.NewTransaction(1, a, uint256.NewInt(), 1, uint256.NewInt(), nil))
			}
			txBytes, err := genTx(common.Address{uint8(i), uint8(blockNum), 1})
			if err != nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId), txBytes)
			if err != nil {
				return err
			}
			txBytes, err = genTx(common.Address{uint8(i), uint8(blockNum), 2})
			if err != nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+1), txBytes)
			if err != nil {
				return err
			}

			txBytes, err = genTx(common.Address{uint8(i), uint8(blockNum), 3})
			if err != nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+2), txBytes)
			if err != nil {
				return err
			}
		}

		err = tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(i), common.Hash{uint8(i), uint8(i%3) + 1}.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// check snapshot data based on GenerateBodyData
func verifyBodiesSnapshot(t *testing.T, bodySnapshotTX ethdb.Tx, snapshotTo uint64) {
	t.Helper()
	bodyCursor, err := bodySnapshotTX.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bodyCursor.Close()

	var blockNum uint64
	err = ethdb.Walk(bodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		//fmt.Println(common.Bytes2Hex(k))
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect blocknum", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), uint8(blockNum%3) + 1}.Bytes()) {
			t.Fatal("block is not canonical", blockNum, common.Bytes2Hex(k))
		}
		bfs := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bfs)
		if err != nil {
			t.Fatal(err, v)
		}
		transactions, err := rawdb.ReadTransactions(bodySnapshotTX, bfs.BaseTxId, bfs.TxAmount)
		if err != nil {
			t.Fatal(err)
		}

		var txNum uint8 = 1
		for _, tr := range transactions {
			expected := common.Address{uint8(blockNum), uint8(blockNum%3 + 1), txNum}
			if *tr.GetTo() != expected {
				t.Fatal(*tr.GetTo(), expected)
			}
			txNum++
		}
		blockNum++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blockNum-1 != snapshotTo {
		t.Fatal(blockNum)
	}
}

func verifyFullBodiesData(t *testing.T, bodySnapshotTX ethdb.Tx, dataTo uint64) {
	t.Helper()
	bodyCursor, err := bodySnapshotTX.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bodyCursor.Close()
	var blockNum uint64
	var numOfDuplicateBlocks uint8
	err = ethdb.Walk(bodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		numOfDuplicateBlocks++
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect blocknum", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), uint8(numOfDuplicateBlocks)}.Bytes()) {
			t.Fatal("incorrect block hash", blockNum, numOfDuplicateBlocks, common.Bytes2Hex(k))
		}
		bfs := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bfs)
		if err != nil {
			t.Fatal(err, v)
		}
		transactions, err := rawdb.ReadTransactions(bodySnapshotTX, bfs.BaseTxId, bfs.TxAmount)
		if err != nil {
			t.Fatal(err)
		}

		if len(transactions) != 3 {
			t.Fatal("incorrect tx num", len(transactions))
		}
		expected := common.Address{uint8(blockNum), numOfDuplicateBlocks, 1}
		if *transactions[0].GetTo() != expected {
			t.Fatal(*transactions[0].GetTo(), expected)
		}
		expected = common.Address{uint8(blockNum), numOfDuplicateBlocks, 2}
		if *transactions[1].GetTo() != expected {
			t.Fatal(*transactions[1].GetTo(), expected)
		}
		expected = common.Address{uint8(blockNum), numOfDuplicateBlocks, 3}
		if *transactions[2].GetTo() != expected {
			t.Fatal(*transactions[2].GetTo(), expected)
		}

		if numOfDuplicateBlocks%3 == 0 {
			blockNum++
			numOfDuplicateBlocks = 0
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBlocks(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	var err error
	dir := t.TempDir()

	defer func() {
		if err != nil {
			t.Log(err, dir)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Log(err)
		}
	}()
	snapshotsDir := path.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}

	db := ethdb.NewSnapshotKV().DB(ethdb.MustOpenKV(path.Join(dir, "chaindata"))).Open()
	quit := make(chan struct{})
	defer func() {
		close(quit)
	}()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Error(err)
		panic(err)
	}
	defer tx.Rollback()
	dataTo := uint64(15)
	snapshotTo := uint64(10)
	err = GenerateBodyData(tx, 0, dataTo)
	if err != nil {
		t.Error(err)
		panic(err)
	}
	verifyFullBodiesData(t, tx, dataTo)
	fmt.Println("Print origin bodies")
	PrintBodyBuckets(t, tx)
	err = tx.Commit()
	if err != nil {
		t.Error(err)
		panic(err)
	}

	readTX, err := db.BeginRo(context.Background())
	if err != nil {
		t.Error(err)
		panic(err)
	}
	defer readTX.Rollback()

	bodySnapshotPath := path.Join(snapshotsDir, SnapshotName(snapshotsDir, "bodies", snapshotTo))

	err = CreateBodySnapshot(readTX, 10, bodySnapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	kvSnapshot, err := OpenBodiesSnapshot(bodySnapshotPath, true)
	if err != nil {
		t.Fatal(err)
	}

	bodySnapshotTX, err := kvSnapshot.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	verifyBodiesSnapshot(t, bodySnapshotTX, snapshotTo)

	fmt.Println("Print snapshot bodies")
	PrintBodyBuckets(t, bodySnapshotTX)

	bodySnapshotTX.Rollback()
	ch := make(chan struct{})
	db.UpdateSnapshots([]string{dbutils.BlockBodyPrefix, dbutils.EthTx}, kvSnapshot, ch)
	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		t.Fatal("timeout on snapshot replace")
	}
	withbodySnapshotTX, err := db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	verifyFullBodiesData(t, withbodySnapshotTX, dataTo)
	withbodySnapshotTX.Rollback()

	rwTX, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = RemoveBlocksData(db, rwTX, 0, snapshotTo)
	if err != nil {
		t.Fatal(err)
	}

	err = rwTX.Commit()
	if err != nil {
		t.Fatal(err)
	}

	withbodySnapshotTX, err = db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Print after remove bodies")
	PrintBodyBuckets(t, withbodySnapshotTX)

	verifyFullBodiesData(t, withbodySnapshotTX, dataTo)
	withbodySnapshotTX.Rollback()
}

func PrintBodyBuckets(t *testing.T, tx ethdb.Tx) {
	bodyCursor, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	err = ethdb.Walk(bodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		bfs := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bfs)
		if err != nil {
			t.Fatal(err, v)
		}
		fmt.Println(binary.BigEndian.Uint64(k), k[8:], bfs.BaseTxId, bfs.TxAmount)

		transactions, err := rawdb.ReadTransactions(tx, bfs.BaseTxId, bfs.TxAmount)
		if err != nil {
			t.Fatal(err)
		}
		for _, transaction := range transactions {
			fmt.Println("----", transaction.GetTo())
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
