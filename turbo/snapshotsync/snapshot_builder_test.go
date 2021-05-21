package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

func TestSnapshotMigratorStage(t *testing.T) {
	t.Skip("flaky test")
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
	defer db.Close()

	sb := &SnapshotMigrator{
		snapshotsDir: snapshotsDir,
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

		err = sb.Migrate(db, tx, currentSnapshotBlock, btCli)
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
	go func() {
		//this gorutine emulates staged sync.
		for {
			StageSyncStep()
		}
	}()

	for !(sb.Finished(10)) {
		time.Sleep(time.Second)
	}

	rotx, err := db.BeginRo(context.Background())
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

	snodb := ethdb.NewObjectDatabase(db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
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
	roTX.GetOne(dbutils.HeadersBucket, []byte{})
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

	rotx, err = db.BeginRo(context.Background())
	require.NoError(t, err)
	defer rotx.Rollback()
	roc, err = rotx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)

	err = ethdb.Walk(roc, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	headerNumber = 0
	err = ethdb.NewObjectDatabase(db.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
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
