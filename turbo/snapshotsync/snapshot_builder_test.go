package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestSnapshotMigratorStage(t *testing.T) {
	t.Skip("Cannot fix this")
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	dir, err := ioutil.TempDir(os.TempDir(), "tst")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err = os.RemoveAll(dir); err != nil {
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

	db := ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
	}
	generateChan := make(chan int)
	currentSnapshotBlock := uint64(10)
	tx, err := db.Begin(context.Background(), ethdb.RW)
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

	for !sb.Finished(10) {
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			tx.Rollback()
			t.Error(err)
		}
		var dbtx ethdb.RwTx
		if hasTx, ok := tx.(ethdb.HasTx); ok {
			dbtx = hasTx.Tx().(ethdb.RwTx)
		}

		select {
		case newHeight := <-generateChan:
			err = GenerateHeaderData(tx, int(currentSnapshotBlock), newHeight)
			if err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			currentSnapshotBlock = CalculateEpoch(uint64(newHeight), 10)
		default:

		}

		err = sb.Migrate(db, dbtx, currentSnapshotBlock, btCli)
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
	}

	sa := db.RwKV().(ethdb.SnapshotUpdater)
	wodb := ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber = 11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Errorf("wrong header key: %x, expected %x", k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)}))
		}
		headerNumber++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 12 {
		t.Fatalf("wrong number of headers: %d, expected %d", headerNumber, 12)
	}

	snodb := ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if headerNumber != 11 {
		t.Fatal(headerNumber)
	}

	headerNumber = 0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
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
	v, err := db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, trnts[0].Bytes()) {
		t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
	}

	v, err = db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(v) != 10 {
		t.Fatal("incorrect snapshot")
	}

	roTX, err := db.Begin(context.Background(), ethdb.RO)
	if err != nil {
		t.Fatal(err)
	}
	//just start snapshot transaction
	roTX.Get(dbutils.HeadersBucket, []byte{})
	defer roTX.Rollback()

	generateChan <- 20

	rollbacked := false
	c := time.After(time.Second * 3)
	for !(sb.Finished(20)) {
		select {
		case <-c:
			roTX.Rollback()
			rollbacked = true
		default:
		}
		time.Sleep(time.Second)
	}

	if !rollbacked {
		t.Fatal("it's not possible to close db without rollback. something went wrong")
	}

	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	headerNumber = 0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if headerNumber != 21 {
		t.Fatal(headerNumber)
	}
	headerNumber = 0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
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
	v, err = db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, trnts[0].Bytes()) {
		t.Fatal("incorrect bytes", common.Bytes2Hex(v), common.Bytes2Hex(trnts[0].Bytes()))
	}

	v, err = db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(v) != 20 {
		t.Fatal("incorrect snapshot")
	}

	if _, err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}

}

func GenerateHeaderData(tx ethdb.DbWithPendingMutations, from, to int) error {
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
