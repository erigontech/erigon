package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

//Testcase plan
//Step 1. Generate headers from 0 to 11.
//Step 2. Run in a separate goroutine snapshot with epoch 10 blocks.
//Step 3. Wait until the snapshot builder passes the first cycle. It must generate a new snapshot and remove duplicate data from
//the main database. After it we must check that headers from 0 to 10 is in snapshot and headers 11 is in the main DB.
//Step 4. Begin new Ro tx and generate data from 11 to 20. Snapshot migration must be blocked by Ro tx on the replace snapshot stage.
//After 3 seconds, we rollback Ro tx, and migration must continue without any errors.
// Step 5. We need to check that the new snapshot contains headers from 0 to 20, the headers bucket in the main database is empty,
// it started seeding a new snapshot and removed the old one.
func TestSnapshotMigratorStageAsync(t *testing.T) {
	t.Skip("often fails on CI")
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := log.New()
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
	snapshotsDir := filepath.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	defer btCli.Close()
	btCli.trackers = [][]string{}

	db := snapshotdb.NewSnapshotKV().DB(mdbx.MustOpen(filepath.Join(dir, "chaindata"))).Open()
	quit := make(chan struct{})
	defer func() {
		close(quit)
	}()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
		replaceChan:  make(chan struct{}),
	}
	currentSnapshotBlock := uint64(10)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = GenerateHeaderData(tx, 0, 11)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	generateChan := make(chan int)
	StageSyncStep := func() {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Error(err)
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

		err = sb.AsyncStages(currentSnapshotBlock, logger, db, tx, btCli, true)
		if err != nil {
			t.Error(err)
		}

		err = sb.SyncStages(currentSnapshotBlock, db, tx)
		if err != nil {
			t.Error(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Error(err)
		}
		roTX, err := db.BeginRo(context.Background())
		if err != nil {
			t.Error(err)
		}

		err = sb.Final(roTX)
		if err != nil {
			t.Error(err)
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
				//mark that migration started and changed started flag(for the first usage)
				so.Do(func() {
					wg.Done()
				})
			}
		}
	}()
	//wait until migration start
	wg.Wait()
	tm := time.After(time.Second * 1000)
	for atomic.LoadUint64(&sb.started) > 0 && atomic.LoadUint64(&sb.HeadersCurrentSnapshot) != 10 {
		select {
		case <-tm:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	//note. We need here only main database.
	rotx, err := db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	var headerNumber uint64
	headerNumber = 11

	err = rotx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		require.Equal(t, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)}), k)
		headerNumber++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}
	rotx.Rollback()

	snokv := db.HeadersSnapshot()
	snRoTx, err := snokv.BeginRo(context.Background())
	require.NoError(t, err)
	headerNumber = 0
	err = snRoTx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 11 {
		t.Fatal(headerNumber)
	}

	headerNumber = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, nil, func(k, v []byte) error {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return nil
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

	err = db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
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
	_, err = roTX.GetOne(kv.Headers, []byte{112, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer roTX.Rollback()

	generateChan <- 20

	rollbacked := false
	//3s - just to be sure that it blocks here
	c := time.After(time.Second * 3)
	tm = time.After(time.Second * 20)

	for atomic.LoadUint64(&sb.started) > 0 || atomic.LoadUint64(&sb.HeadersCurrentSnapshot) != 20 {
		select {
		case <-c:
			roTX.Rollback()
			rollbacked = true
		case <-tm:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
	if !rollbacked {
		t.Log("it's not possible to close db without rollback. something went wrong")
	}

	rotx, err = db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()

	err = rotx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		t.Fatal("main db must be empty here", k)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	headerNumber = 0
	err = db.HeadersSnapshot().View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, nil, func(k, v []byte) error {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++

			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}
	headerNumber = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, nil, func(k, v []byte) error {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return nil
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
	err = db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
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
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := log.New()
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
	snapshotsDir := filepath.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}
	defer btCli.Close()

	db := snapshotdb.NewSnapshotKV().DB(mdbx.MustOpen(filepath.Join(dir, "chaindata"))).Open()
	defer db.Close()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
		replaceChan:  make(chan struct{}),
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
			t.Fatal()
		}
		defer rotx.Rollback()

		err = sb.AsyncStages(currentSnapshotBlock, logger, db, rotx, btCli, false)
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

	tm := time.After(time.Second * 10)
	for atomic.LoadUint64(&sb.started) > 0 && atomic.LoadUint64(&sb.HeadersCurrentSnapshot) != 10 {
		err = db.View(context.Background(), func(tx kv.Tx) error { return sb.Final(tx) })
		if err != nil {
			t.Fatal(err)
		}

		select {
		case <-tm:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	//note. We need here only main database.
	rotx, err := db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	var headerNumber uint64
	headerNumber = 11

	err = rotx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		require.Equal(t, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)}), k)
		headerNumber++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 12 {
		t.Fatal(headerNumber)
	}
	rotx.Rollback()

	snokv := db.HeadersSnapshot()
	snRoTx, err := snokv.BeginRo(context.Background())
	require.NoError(t, err)
	headerNumber = 0
	err = snRoTx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return nil
	})

	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 11 {
		t.Fatal(headerNumber)
	}

	headerNumber = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, nil, func(k, v []byte) error {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return nil
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

	err = db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
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
		defer rotx.Rollback()
		//just start snapshot transaction
		// it can't be empty slice but shouldn't be in main db
		_, err = roTX.GetOne(kv.Headers, []byte{1})
		if err != nil {
			wg.Done()
			t.Error(err)
		}
		wg.Done()
		<-c
		//reproducable datarace between Rollback and Close
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

	tm = time.After(time.Second * 10)

	for atomic.LoadUint64(&sb.started) > 0 && atomic.LoadUint64(&sb.HeadersCurrentSnapshot) == 20 {
		roTx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		err = sb.Final(roTx)
		if err != nil {
			t.Fatal(err)
		}
		roTx.Rollback()
		select {
		case <-tm:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	if !rollbacked {
		t.Log("it's not possible to close db without rollback. something went wrong")
	}

	rotx, err = db.WriteDB().BeginRo(context.Background())
	require.NoError(t, err)
	err = rotx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		t.Fatal("main db must be empty here", k)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rotx.Rollback()
	headerNumber = 0
	snRoTx, err = db.HeadersSnapshot().BeginRo(context.Background())
	require.NoError(t, err)
	err = snRoTx.ForEach(kv.Headers, nil, func(k, v []byte) error {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return nil
	})
	snRoTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}
	headerNumber = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, nil, func(k, v []byte) error {
			if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
				t.Fatal(k)
			}
			headerNumber++
			return nil
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
	err = db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotHash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, trnts[0].Bytes()) {
			t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
		}

		v, err = tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
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
		t.Fatal("snapshot exist")
	} else {
		//just not to confuse defer
		err = nil
	}
}

func GenerateHeaderData(tx kv.RwTx, from, to int) error {
	var err error
	if to > math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i := from; i <= to; i++ {
		err = tx.Put(kv.Headers, dbutils.HeaderKey(uint64(i), common.Hash{uint8(i)}), []byte{uint8(i), uint8(i), uint8(i)})
		if err != nil {
			return err
		}
		err = tx.Put(kv.HeaderCanonical, dbutils.EncodeBlockNumber(uint64(i)), common.Hash{uint8(i)}.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func GenerateBodyData(tx kv.RwTx, from, to uint64) error {
	var err error
	if to > math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i := from; i <= to; i++ {
		for blockNum := 1; blockNum < 4; blockNum++ {
			bodyForStorage := new(types.BodyForStorage)
			baseTxId, err := tx.IncrementSequence(kv.EthTx, 3)
			if err != nil {
				return err
			}
			bodyForStorage.BaseTxId = baseTxId
			bodyForStorage.TxAmount = 3
			body, err := rlp.EncodeToBytes(bodyForStorage)
			if err != nil {
				return err
			}
			err = tx.Put(kv.BlockBody, dbutils.BlockBodyKey(i, common.Hash{uint8(i), uint8(blockNum)}), body)
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

			err = tx.Put(kv.Headers, dbutils.HeaderKey(i, common.Hash{uint8(i), uint8(blockNum)}), headersBytes)
			if err != nil {
				return err
			}

			genTx := func(a common.Address) ([]byte, error) {
				return rlp.EncodeToBytes(types.NewTransaction(1, a, uint256.NewInt(1), 1, uint256.NewInt(1), nil))
			}
			txBytes, err := genTx(common.Address{uint8(i), uint8(blockNum), 1})
			if err != nil {
				return err
			}

			err = tx.Put(kv.EthTx, dbutils.EncodeBlockNumber(baseTxId), txBytes)
			if err != nil {
				return err
			}
			txBytes, err = genTx(common.Address{uint8(i), uint8(blockNum), 2})
			if err != nil {
				return err
			}

			err = tx.Put(kv.EthTx, dbutils.EncodeBlockNumber(baseTxId+1), txBytes)
			if err != nil {
				return err
			}

			txBytes, err = genTx(common.Address{uint8(i), uint8(blockNum), 3})
			if err != nil {
				return err
			}

			err = tx.Put(kv.EthTx, dbutils.EncodeBlockNumber(baseTxId+2), txBytes)
			if err != nil {
				return err
			}
		}

		err = tx.Put(kv.HeaderCanonical, dbutils.EncodeBlockNumber(i), common.Hash{uint8(i), uint8(i%3) + 1}.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// check snapshot data based on GenerateBodyData
func verifyBodiesSnapshot(t *testing.T, bodySnapshotTX kv.Tx, snapshotTo uint64) {
	t.Helper()

	var blockNum uint64
	err := bodySnapshotTX.ForEach(kv.BlockBody, nil, func(k, v []byte) error {
		//fmt.Println(common.Bytes2Hex(k))
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect block number", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), uint8(blockNum%3) + 1}.Bytes()) {
			t.Fatal("block is not canonical", blockNum, common.Bytes2Hex(k))
		}
		bfs := types.BodyForStorage{}
		err := rlp.DecodeBytes(v, &bfs)
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
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blockNum-1 != snapshotTo {
		t.Fatal(blockNum, snapshotTo)
	}
}

// check headers snapshot data based on GenerateBodyData
func verifyHeadersSnapshot(t *testing.T, headersSnapshotTX kv.Tx, snapshotTo uint64) {
	t.Helper()

	var blockNum uint64
	err := headersSnapshotTX.ForEach(kv.Headers, nil, func(k, v []byte) error {
		//fmt.Println(common.Bytes2Hex(k))
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect block number", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), uint8(blockNum%3) + 1}.Bytes()) {
			t.Fatal("block is not canonical", blockNum, common.Bytes2Hex(k))
		}
		blockNum++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blockNum-1 != snapshotTo {
		t.Fatal(blockNum, snapshotTo)
	}
}

func verifyFullBodiesData(t *testing.T, bodySnapshotTX kv.Tx, dataTo uint64) {
	t.Helper()
	var blockNum uint64
	var numOfDuplicateBlocks uint8
	err := bodySnapshotTX.ForEach(kv.BlockBody, nil, func(k, v []byte) error {
		numOfDuplicateBlocks++
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect block number", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		if !bytes.Equal(k[8:], common.Hash{uint8(blockNum), numOfDuplicateBlocks}.Bytes()) {
			t.Fatal("incorrect block hash", blockNum, numOfDuplicateBlocks, common.Bytes2Hex(k))
		}
		bfs := types.BodyForStorage{}
		err := rlp.DecodeBytes(v, &bfs)
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
			t.Fatal(k, blockNum, numOfDuplicateBlocks, bfs.BaseTxId, *transactions[0].GetTo(), expected.Bytes())
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

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blockNum-1 != dataTo {
		t.Fatal("incorrect last block", blockNum, dataTo)
	}
}

func verifyPrunedBlocksData(t *testing.T, tx kv.Tx, dataFrom, dataTo, snapshotTxTo uint64) {
	t.Helper()
	bodyCursor, err := tx.Cursor(kv.BlockBody)
	if err != nil {
		t.Fatal(err)
	}
	defer bodyCursor.Close()
	var blockNum uint64
	var numOfDuplicateBlocks uint8
	err = tx.ForEach(kv.BlockBody, nil, func(k, v []byte) error {
		numOfDuplicateBlocks++
		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect block number", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}
		canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			t.Fatal(err)
		}
		if blockNum <= dataFrom {
			if bytes.Equal(k[8:], canonicalHash.Bytes()) {
				t.Fatal("snapshot data hasn't deleted", k, canonicalHash.Bytes())
			}
		}

		bfs := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bfs)
		if err != nil {
			t.Fatal(err, v)
		}
		if bfs.BaseTxId <= snapshotTxTo {
			t.Fatal("txid must be after last snapshot txid")
		}
		transactions, err := rawdb.ReadTransactions(tx, bfs.BaseTxId, bfs.TxAmount)
		if err != nil {
			t.Fatal(err)
		}

		if len(transactions) != 3 {
			t.Fatal("incorrect tx num", len(transactions))
		}

		switch {
		case blockNum <= dataFrom && numOfDuplicateBlocks == 2:
			blockNum++
			numOfDuplicateBlocks = 0
		//after snapshot data
		case blockNum > dataFrom && numOfDuplicateBlocks == 3:
			blockNum++
			numOfDuplicateBlocks = 0
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blockNum-1 != dataTo {
		t.Fatal("incorrect last block", blockNum, dataTo)
	}
}

func TestPruneBlocks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := log.New()
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
	snapshotsDir := filepath.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}
	defer btCli.Close()

	db := snapshotdb.NewSnapshotKV().DB(mdbx.MustOpen(filepath.Join(dir, "chaindata"))).Open()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	dataTo := uint64(15)
	snapshotTo := uint64(10)
	err = GenerateBodyData(tx, 0, dataTo)
	if err != nil {
		t.Fatal(err)
	}
	verifyFullBodiesData(t, tx, dataTo)
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	readTX, err := db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer readTX.Rollback()

	bodySnapshotPath := filepath.Join(snapshotsDir, SnapshotName(snapshotsDir, "bodies", snapshotTo))
	err = CreateBodySnapshot(readTX, logger, snapshotTo, bodySnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	readTX.Rollback()
	kvSnapshot, err := OpenBodiesSnapshot(logger, bodySnapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	bodySnapshotTX, err := kvSnapshot.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer bodySnapshotTX.Rollback()

	verifyBodiesSnapshot(t, bodySnapshotTX, snapshotTo)
	ethTXCursor, err := bodySnapshotTX.Cursor(kv.EthTx)
	if err != nil {
		t.Fatal(err)
	}
	lastTxID, _, err := ethTXCursor.Last()
	if err != nil {
		t.Fatal(err)
	}
	bodySnapshotTX.Rollback()

	ch := make(chan struct{})
	db.UpdateSnapshots("bodies", kvSnapshot, ch)
	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		t.Fatal("timeout on snapshot replace")
	}
	withBodySnapshotTX, err := db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer withBodySnapshotTX.Rollback()

	verifyFullBodiesData(t, withBodySnapshotTX, dataTo)
	withBodySnapshotTX.Rollback()

	rwTX, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rwTX.Rollback()

	err = RemoveBlocksData(db, rwTX, snapshotTo)
	if err != nil {
		t.Fatal(err)
	}

	err = rwTX.Commit()
	if err != nil {
		t.Fatal(err)
	}

	withBodySnapshotTX, err = db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer withBodySnapshotTX.Rollback()

	verifyFullBodiesData(t, withBodySnapshotTX, dataTo)
	withBodySnapshotTX.Rollback()

	writeDBKV := db.WriteDB()

	writeDBKVRoTX, err := writeDBKV.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer writeDBKVRoTX.Rollback()

	verifyPrunedBlocksData(t, writeDBKVRoTX, snapshotTo, dataTo, binary.BigEndian.Uint64(lastTxID))
	writeDBKVRoTX.Rollback()

	tx, err = db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	//snapshot to 20
	dataFrom := uint64(16)
	dataTo = uint64(20)
	snapshotTo = uint64(20)
	err = GenerateBodyData(tx, dataFrom, dataTo)
	if err != nil {
		t.Fatal(err)
	}
	verifyFullBodiesData(t, tx, dataTo)
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	bodySnapshotPath = filepath.Join(snapshotsDir, SnapshotName(snapshotsDir, "bodies", snapshotTo))
	readTX, err = db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer readTX.Rollback()

	err = CreateBodySnapshot(readTX, logger, snapshotTo, bodySnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	readTX.Rollback()

	kvSnapshot, err = OpenBodiesSnapshot(logger, bodySnapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	bodySnapshotTX, err = kvSnapshot.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer bodySnapshotTX.Rollback()

	verifyBodiesSnapshot(t, bodySnapshotTX, snapshotTo)
	ethTXCursor, err = bodySnapshotTX.Cursor(kv.EthTx)
	if err != nil {
		t.Fatal(err)
	}
	lastTxID, _, err = ethTXCursor.Last()
	if err != nil {
		t.Fatal(err)
	}

	bodySnapshotTX.Rollback()
	ch = make(chan struct{})
	db.UpdateSnapshots("bodies", kvSnapshot, ch)
	select {
	case <-ch:
	case <-time.After(time.Second * 5000):
		t.Fatal("timeout on snapshot replace")
	}
	withBodySnapshotTX, err = db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer withBodySnapshotTX.Rollback()

	verifyFullBodiesData(t, withBodySnapshotTX, dataTo)
	withBodySnapshotTX.Rollback()

	rwTX, err = db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rwTX.Rollback()

	err = RemoveBlocksData(db, rwTX, snapshotTo)
	if err != nil {
		t.Fatal(err)
	}

	err = rwTX.Commit()
	if err != nil {
		t.Fatal(err)
	}

	withBodySnapshotTX, err = db.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer withBodySnapshotTX.Rollback()

	verifyFullBodiesData(t, withBodySnapshotTX, dataTo)
	withBodySnapshotTX.Rollback()

	writeDBKV = db.WriteDB()

	writeDBKVRoTX, err = writeDBKV.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer writeDBKVRoTX.Rollback()
	verifyPrunedBlocksData(t, writeDBKVRoTX, snapshotTo, dataTo, binary.BigEndian.Uint64(lastTxID))
}

func PrintBodyBuckets(t *testing.T, tx kv.Tx) { //nolint: deadcode
	err := tx.ForEach(kv.BlockBody, nil, func(k, v []byte) error {
		bfs := types.BodyForStorage{}
		err := rlp.DecodeBytes(v, &bfs)
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
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBodySnapshotSyncMigration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := log.New()
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
	snapshotsDir := filepath.Join(dir, "snapshots")
	err = os.Mkdir(snapshotsDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	btCli, err := New(snapshotsDir, true, "12345123451234512345")
	if err != nil {
		t.Fatal(err)
	}
	btCli.trackers = [][]string{}
	defer btCli.Close()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
		replaceChan:  make(chan struct{}),
	}

	db := snapshotdb.NewSnapshotKV().DB(mdbx.MustOpen(filepath.Join(dir, "chaindata"))).Open()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	dataTo := uint64(11)
	err = GenerateBodyData(tx, 0, dataTo)
	if err != nil {
		t.Fatal(err)
	}
	verifyFullBodiesData(t, tx, dataTo)
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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

	readStep := func(currentSnapshotBlock uint64) {
		t.Helper()
		rotx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Error(err)
		}
		defer rotx.Rollback()

		err = sb.AsyncStages(currentSnapshotBlock, logger, db, rotx, btCli, false)
		if err != nil {
			t.Fatal(err)
		}
		rotx.Rollback()
	}
	readStep(10)
	for !sb.Replaced() {
		//wait until all txs of old snapshot closed
	}
	writeStep(10)

	tm := time.After(time.Second * 5)
	for atomic.LoadUint64(&sb.started) > 0 && atomic.LoadUint64(&sb.HeadersCurrentSnapshot) != 10 {
		roTx, err := db.BeginRo(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = sb.Final(roTx)
		if err != nil {
			t.Fatal(err)
		}
		roTx.Rollback()
		select {
		case <-tm:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	readStep(10)
	for !sb.Replaced() {
		//wait until all txs of old snapshot closed
	}
	writeStep(10)

	for atomic.LoadUint64(&sb.started) > 0 && atomic.LoadUint64(&sb.BodiesCurrentSnapshot) != 10 {
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
	headersKV := db.HeadersSnapshot()
	if headersKV == nil {
		t.Fatal("empty headers snapshot")
	}
	bodiesKV := db.BodiesSnapshot()
	if bodiesKV == nil {
		t.Fatal("empty bodies snapshot")
	}

	htx, err := headersKV.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	verifyHeadersSnapshot(t, htx, 10)
	htx.Rollback()

	btx, err := bodiesKV.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	verifyBodiesSnapshot(t, btx, 10)
	ethTX, err := btx.Cursor(kv.EthTx)
	if err != nil {
		t.Fatal(err)
	}
	lastEthTX, _, err := ethTX.Last()
	if err != nil {
		t.Fatal(err)
	}
	btx.Rollback()

	writeKV := db.WriteDB()
	roWriteDBTX, err := writeKV.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roWriteDBTX.Rollback()

	var blockNum uint64
	var numOfDuplicateBlocks uint64
	dataFrom := uint64(10)
	err = roWriteDBTX.ForEach(kv.Headers, []byte{}, func(k, v []byte) error {
		numOfDuplicateBlocks++

		if binary.BigEndian.Uint64(k[:8]) != blockNum {
			t.Fatal("incorrect block number", blockNum, binary.BigEndian.Uint64(k[:8]), common.Bytes2Hex(k))
		}

		canonicalHash, err := rawdb.ReadCanonicalHash(roWriteDBTX, blockNum)
		if err != nil {
			t.Fatal(err)
		}
		if blockNum <= dataFrom {
			if bytes.Equal(k[8:], canonicalHash.Bytes()) {
				t.Fatal("snapshot data hasn't deleted", k, canonicalHash.Bytes())
			}
		}

		switch {
		case blockNum <= dataFrom && numOfDuplicateBlocks == 2:
			blockNum++
			numOfDuplicateBlocks = 0
		//after snapshot data
		case blockNum > dataFrom && numOfDuplicateBlocks == 3:
			blockNum++
			numOfDuplicateBlocks = 0
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	verifyPrunedBlocksData(t, roWriteDBTX, 10, dataTo, binary.BigEndian.Uint64(lastEthTX))
}
