package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
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
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
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

		err = sb.Final(tx)
		if err != nil {
			t.Error(err)
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Error(err)
			panic(err)
		}
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
	err = GenerateBodyData(tx, 0, 10)
	if err != nil {
		t.Error(err)
		panic(err)
	}

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


	bodySnapshotPath:=path.Join(snapshotsDir, "body10")

	err = CreateBodySnapshot(readTX, 10, bodySnapshotPath)
	if err!=nil {
		t.Fatal(err)
	}

	kvSnapshot, err :=OpenBodiesSnapshot(bodySnapshotPath, true)
	if err!=nil {
		t.Fatal(err)
	}

	bodySnapshotTX,err:=kvSnapshot.BeginRo(context.Background())
	if err!=nil {
		t.Fatal(err)
	}
	bodyCursor,err:=bodySnapshotTX.Cursor(dbutils.BlockBodyPrefix)
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println("check snapshot")
	var blockNum uint64
	err = ethdb.Walk(bodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		//fmt.Println(common.Bytes2Hex(k))
		if binary.BigEndian.Uint64(k[:8])!=blockNum {
			t.Fatal("incorrect blocknum", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), uint8(blockNum%3)+1}.Bytes()) {
			t.Fatal("block is not canonical", blockNum, common.Bytes2Hex(k))
		}
		bfs:=types.BodyForStorage{}
		err = rlp.DecodeBytes(v,&bfs)
		if err!=nil {
			t.Fatal(err, v)
		}
		transactions, err := rawdb.ReadTransactions(bodySnapshotTX, bfs.BaseTxId, bfs.TxAmount)
		if err!=nil {
			t.Fatal(err)
		}
		//
		var txNum uint8 = 1
		for _,tr:=range transactions {
			expected:=common.Address{uint8(blockNum),uint8(blockNum%3+1), txNum}
			if *tr.GetTo() != expected {
				t.Fatal(*tr.GetTo(), expected )
			}
			txNum++
		}
		blockNum++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if blockNum-1!=10 {
		t.Fatal(blockNum)
	}

}

func GenerateBodyData(tx ethdb.RwTx, from, to uint64) error {
	var err error
	if to > math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i := from; i <= to; i++ {
		for blockNum:=1; blockNum<4; blockNum++ {
			bodyForStorage := new(types.BodyForStorage)
			baseTxId,err:=tx.IncrementSequence(dbutils.EthTx, 3)
			if err!=nil {
				return err
			}
			bodyForStorage.BaseTxId = baseTxId
			bodyForStorage.TxAmount = 3
			body, err:=rlp.EncodeToBytes(bodyForStorage)
			if err!=nil {
				return err
			}
			err = tx.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(uint64(i), common.Hash{uint8(i), uint8(blockNum)}), body)
			if err!=nil {
				return err
			}
			header:=&types.Header{
				Number: big.NewInt(int64(i)),
			}
			headersBytes,err:= rlp.EncodeToBytes(header)
			if err!=nil {
				return err
			}
			//fmt.Println("block", uint64(i), common.Hash{uint8(i), uint8(blockNum)})
			err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(uint64(i), common.Hash{uint8(i), uint8(blockNum)}), headersBytes)
			if err != nil {
				return err
			}

			genTx:= func(a common.Address) ([]byte, error) {
				return rlp.EncodeToBytes(types.NewTransaction(1, a, uint256.NewInt(), 1, uint256.NewInt(), nil))
			}
			txBytes,err:=genTx(common.Address{uint8(i), uint8(blockNum), 1})
			if err!=nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId), txBytes)
			if err!=nil {
				return err
			}
			txBytes,err=genTx(common.Address{uint8(i), uint8(blockNum), 2})
			if err!=nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+1), txBytes)
			if err!=nil {
				return err
			}

			txBytes,err=genTx(common.Address{uint8(i), uint8(blockNum), 3})
			if err!=nil {
				return err
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+2), txBytes)
			if err!=nil {
				return err
			}
		}

		err = tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(i), common.Hash{uint8(i), uint8(i%3)+1}.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}


func GenerateBodiesSnapshot(ctx context.Context, readTX ethdb.Tx, writeTX ethdb.RwTx, toBlock uint64) error {
	readBodyCursor,err:=readTX.Cursor(dbutils.BlockBodyPrefix)
	if err!=nil {
		return err
	}

	writeBodyCursor,err:=writeTX.RwCursor(dbutils.BlockBodyPrefix)
	if err!=nil {
		return err
	}
	writeEthTXCursor,err:=writeTX.RwCursor(dbutils.EthTx)
	if err!=nil {
		return err
	}
	readEthTXCursor,err:=readTX.Cursor(dbutils.EthTx)
	if err!=nil {
		return err
	}


	var expectedBaseTxId uint64
	err = ethdb.Walk(readBodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println(binary.BigEndian.Uint64(k), common.Bytes2Hex(k))
		canonocalHash,err:=readTX.GetOne(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(k)))
		if err!=nil {
			return false, err
		}
		if !bytes.Equal(canonocalHash, k[8:]) {
			return true, nil
		}
		bd:=&types.BodyForStorage{}
		err = rlp.DecodeBytes(v, bd)
		if err!=nil {
			return false, fmt.Errorf("block %s decode err %w", common.Bytes2Hex(k), err)
		}
		baseTxId:=bd.BaseTxId
		amount:=bd.TxAmount

		bd.BaseTxId = expectedBaseTxId
		newV,err:=rlp.EncodeToBytes(bd)
		if err!=nil {
			return false, err
		}
		err = writeBodyCursor.Append(common.CopyBytes(k), newV)
		if err!=nil {
			return false, err
		}

		newExpectedTx:=expectedBaseTxId
		err = ethdb.Walk(readEthTXCursor, dbutils.EncodeBlockNumber(baseTxId), 0, func(k, v []byte) (bool, error) {
			if  newExpectedTx>=expectedBaseTxId+uint64(amount) {
				return false, nil
			}
			err = writeEthTXCursor.Append(dbutils.EncodeBlockNumber(newExpectedTx), common.CopyBytes(v))
			if err!=nil {
				return false, err
			}
			newExpectedTx++
			return true,nil
		})
		if err!=nil {
			return false, err
		}
		if newExpectedTx > expectedBaseTxId+uint64(amount) {
			fmt.Println("newExpectedTx > expectedBaseTxId+amount", newExpectedTx, expectedBaseTxId, amount, "block", common.Bytes2Hex(k))
			return false, errors.New("newExpectedTx > expectedBaseTxId+amount")
		}
		expectedBaseTxId+=uint64(amount)
		return true, nil
	})
	 if err!=nil {
	 	return err
	 }
	//var first bool
	//var expectedBaseTxId uint64
	//var prevBaseTx, prevAmount uint64
	//tt:=time.Now()
	//ttt:=time.Now()
	//for i:=uint64(0); i<= toBlock;i++ {
	//	if i%100000 == 0 {
	//		fmt.Println(i, "block", time.Since(ttt), "all", time.Since(tt), "expectedBaseTx", expectedBaseTxId)
	//		ttt=time.Now()
	//	}
	//	hash, err:=rawdb.ReadCanonicalHash(readTX, i)
	//	if err!=nil {
	//		return err
	//	}
	//	nextBaseTx:=prevBaseTx+prevAmount
	//	v,err:=readTX.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(i, hash))
	//	if err!=nil {
	//		return err
	//	}
	//	bd:=&types.BodyForStorage{}
	//	err = rlp.DecodeBytes(v, bd)
	//	if err!=nil {
	//		return fmt.Errorf("block %d decode err %w", i, err)
	//	}
	//	baseTxId:=bd.BaseTxId
	//	amount:=bd.TxAmount
	//
	//	if !first {
	//		if expectedBaseTxId!=baseTxId {
	//			fmt.Println("diff on", i)
	//			first=true
	//		}
	//	}
	//	if nextBaseTx!=baseTxId {
	//		fmt.Println("block",i, "expected",nextBaseTx, "got",baseTxId,amount)
	//		c,err:=readTX.Cursor(dbutils.BlockBodyPrefix)
	//		if err!=nil {
	//			return err
	//		}
	//		err = ethdb.Walk(c,dbutils.BlockBodyKey(i-1,common.Hash{}),8*8, func(k, v []byte) (bool, error) {
	//			bodyForStorage := new(types.BodyForStorage)
	//			err := rlp.DecodeBytes(v, bodyForStorage)
	//			if err != nil {
	//				return false, err
	//			}
	//
	//			fmt.Println(binary.BigEndian.Uint64(k), common.Bytes2Hex(k), bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
	//			return true,nil
	//		})
	//		if err!=nil {
	//			return err
	//		}
	//		err = ethdb.Walk(c,dbutils.BlockBodyKey(i,common.Hash{}),8*8, func(k, v []byte) (bool, error) {
	//			bodyForStorage := new(types.BodyForStorage)
	//			err := rlp.DecodeBytes(v, bodyForStorage)
	//			if err != nil {
	//				return false, err
	//			}
	//
	//			fmt.Println(binary.BigEndian.Uint64(k), common.Bytes2Hex(k), bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
	//			return true,nil
	//		})
	//		if err!=nil {
	//			return err
	//		}
	//		//break
	//	}
	//	bd.BaseTxId = expectedBaseTxId
	//	newV,err:=rlp.EncodeToBytes(bd)
	//	if err!=nil {
	//		return err
	//	}
	//	err = bodyCursor.Append(dbutils.HeaderKey(i, hash), newV)
	//	if err!=nil {
	//		return err
	//	}
	//	txsCursor,err:=readTX.Cursor(dbutils.EthTx)
	//	if err!=nil {
	//		return err
	//	}
	//
	//	newExpectedTx:=expectedBaseTxId
	//	err = ethdb.Walk(txsCursor, dbutils.EncodeBlockNumber(baseTxId), 0, func(k, v []byte) (bool, error) {
	//		if  newExpectedTx>=expectedBaseTxId+uint64(amount) {
	//			return false, nil
	//		}
	//		err = txsWriteCursor.Append(dbutils.EncodeBlockNumber(newExpectedTx), common.CopyBytes(v))
	//		if err!=nil {
	//			return false, err
	//		}
	//		newExpectedTx++
	//		return true,nil
	//	})
	//	if err!=nil {
	//		return err
	//	}
	//	if newExpectedTx > expectedBaseTxId+uint64(amount) {
	//		fmt.Println("newExpectedTx > expectedBaseTxId+amount", newExpectedTx, expectedBaseTxId, amount, "block", i)
	//		continue
	//	}
	//	prevBaseTx=baseTxId
	//	prevAmount=uint64(amount)
	//	expectedBaseTxId+=uint64(amount)
	//}

	return nil
}

func CreateBodySnapshot(readTx ethdb.Tx, lastBlock uint64, snapshotDir string) error  {
	kv, err := ethdb.NewMDBX().Path(snapshotDir).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketsConfigs[dbutils.BlockBodyPrefix],
			dbutils.EthTx: dbutils.BucketsConfigs[dbutils.EthTx],
		}
	}).Open()
	if err!=nil {
		return err
	}

	defer kv.Close()
	writeTX,err :=kv.BeginRw(context.Background())
	if err!=nil {
		return err
	}
	defer writeTX.Rollback()
	err = GenerateBodiesSnapshot(context.TODO(), readTx, writeTX, lastBlock)
	if err!=nil {
		return err
	}
	return writeTX.Commit()
}






func TestBlocksSnapshot(t *testing.T) {
	chaindataDir := "/media/b00ris/nvme/goerly/tg/chaindata"
	snapshotDir := "/media/b00ris/nvme/snapshots/body4500000"

	err:=os.RemoveAll(snapshotDir)
	if err!=nil {
		t.Fatal(err)
	}
	//
	//info,err:=snapshotsync.BuildInfoBytesForSnapshot(snapshotDir,snapshotsync.MdbxFilename)
	//if err!=nil {
	//	//t.Fatal(err)
	//}
	//infoBytes, err := bencode.Marshal(info)
	//if err != nil {
	////	t.Fatal(err)
	//}
	//t.Log("hash was:",metainfo.HashBytes(infoBytes).String())


	chaindata, err := ethdb.Open(chaindataDir, true)
	if err != nil {
		t.Fatal(err)
	}
	tmpDb:=ethdb.NewMemDatabase()

	kv, err := ethdb.NewMDBX().Path(snapshotDir).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketsConfigs[dbutils.BlockBodyPrefix],
			dbutils.EthTx: dbutils.BucketsConfigs[dbutils.EthTx],
		}
	}).Open()
	if err != nil {
		t.Fatal(err)
	}
	writeTX,err:=kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	//tmpDb := ethdb.NewObjectDatabase(kv)
	snkv := ethdb.NewSnapshotKV().DB(tmpDb.RwKV()).SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeaderNumberBucket, dbutils.HeaderCanonicalBucket,  dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.EthTx}, chaindata.RwKV()).Open()
	defer func() {
		fmt.Println("close db")
		snkv.Close()
		fmt.Println("closed db")
	}()
	tx, err := snkv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fmt.Println("rollback")
		tx.Rollback()
		fmt.Println("rollbacked")
	}()
	//currentHead:=rawdb.ReadCurrentHeader(tx)
	lastBlock:=uint64(4500000)//currentHead.Number.Uint64()

	bodyCursor,err:=writeTX.RwCursor(dbutils.BlockBodyPrefix)
	if err!=nil {
		t.Fatal(err)
	}
	txsWriteCursor,err:=writeTX.RwCursor(dbutils.EthTx)
	if err!=nil {
		t.Fatal(err)
	}
	var first bool
	var expectedBaseTxId uint64
	var prevBaseTx, prevAmount uint64
	tt:=time.Now()
	ttt:=time.Now()
	for i:=uint64(0); i<lastBlock;i++ {
		if i%100000 == 0 {
			fmt.Println(i, "block", time.Since(ttt), "all", time.Since(tt), "expectedBaseTx", expectedBaseTxId)
			ttt=time.Now()
		}
		hash, err:=rawdb.ReadCanonicalHash(tx, i)
		if err!=nil {
			t.Fatal(err)
		}
		nextBaseTx:=prevBaseTx+prevAmount
		v,err:=tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(i, hash))
		if err!=nil {
			t.Fatal(err)
		}
		bd:=&types.BodyForStorage{}
		err = rlp.DecodeBytes(v, bd)
		if err!=nil {
			t.Fatal(err)
		}
		baseTxId:=bd.BaseTxId
		amount:=bd.TxAmount

		if !first {
			if expectedBaseTxId!=baseTxId {
				fmt.Println("diff on", i)
				first=true
			}
		}
		if nextBaseTx!=baseTxId {
			fmt.Println("block",i, "expected",nextBaseTx, "got",baseTxId,amount)
			c,err:=tx.Cursor(dbutils.BlockBodyPrefix)
			if err!=nil {
				t.Fatal(err)
			}
			err = ethdb.Walk(c,dbutils.BlockBodyKey(i-1,common.Hash{}),8*8, func(k, v []byte) (bool, error) {
				bodyForStorage := new(types.BodyForStorage)
				err := rlp.DecodeBytes(v, bodyForStorage)
				if err != nil {
					return false, err
				}

				fmt.Println(binary.BigEndian.Uint64(k), common.Bytes2Hex(k), bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
				return true,nil
			})
			if err!=nil {
				t.Fatal(err)
			}
			err = ethdb.Walk(c,dbutils.BlockBodyKey(i,common.Hash{}),8*8, func(k, v []byte) (bool, error) {
				bodyForStorage := new(types.BodyForStorage)
				err := rlp.DecodeBytes(v, bodyForStorage)
				if err != nil {
					return false, err
				}

				fmt.Println(binary.BigEndian.Uint64(k), common.Bytes2Hex(k), bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
				return true,nil
			})
			if err!=nil {
				t.Fatal(err)
			}
			//break
		}
		bd.BaseTxId = expectedBaseTxId
		newV,err:=rlp.EncodeToBytes(bd)
		if err!=nil {
			t.Fatal(err)
		}
		err = bodyCursor.Append(dbutils.HeaderKey(i, hash), newV)
		if err!=nil {
			t.Fatal(err)
		}
		txsCursor,err:=tx.Cursor(dbutils.EthTx)
		if err!=nil {
			t.Fatal(err)
		}

		newExpectedTx:=expectedBaseTxId
		err = ethdb.Walk(txsCursor, dbutils.EncodeBlockNumber(baseTxId), 0, func(k, v []byte) (bool, error) {
			if  newExpectedTx>=expectedBaseTxId+uint64(amount) {
				return false, nil
			}
			err = txsWriteCursor.Append(dbutils.EncodeBlockNumber(newExpectedTx), common.CopyBytes(v))
			if err!=nil {
				return false, err
			}
			newExpectedTx++
			return true,nil
		})
		if err!=nil {
			t.Fatal(err)
		}
		if newExpectedTx > expectedBaseTxId+uint64(amount) {
			fmt.Println("newExpectedTx > expectedBaseTxId+amount", newExpectedTx, expectedBaseTxId, amount, "block", i)
			continue
		}
		prevBaseTx=baseTxId
		prevAmount=uint64(amount)
		expectedBaseTxId+=uint64(amount)
	}

	err=writeTX.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	info,err:=BuildInfoBytesForSnapshot(snapshotDir,MdbxFilename)
	if err!=nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("hash is:",metainfo.HashBytes(infoBytes).String())
}


