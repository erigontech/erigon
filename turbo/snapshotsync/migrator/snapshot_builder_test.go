package migrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

/*
todo test when snapshot hasn't generated
 */
func TestBuildHeadersSnapshotAsync(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(dir)
	defer func() {
		err = os.RemoveAll(dir)
		t.Log(err)
	}()
	snapshotsDir:=path.Join(dir, "snapshots")
	err= os.Mkdir(snapshotsDir, os.ModePerm)
	if err!=nil {
		t.Fatal(err)
	}
	db:=ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator{
		SnapshotDir: snapshotsDir,
		toRemove: make(map[string]struct{}),
	}
	currentSnapshotBlock:=uint64(10)
	go func() {
		for {
			snBlock:=atomic.LoadUint64(&currentSnapshotBlock)
			err = sb.CreateHeadersSnapshot(db, atomic.LoadUint64(&snBlock))
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.ReplaceHeadersSnapshot(db)
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.RemoveHeadersData(db)
			if err!=nil {
				t.Fatal(err)
			}

			err = sb.RemovePreviousVersion()
			if err!=nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !(sb.IsFinished(10) && sb.Cleaned(10)) {}
	fmt.Println("finished", time.Since(tt), sb.IsFinished(10), sb.Cleaned(10) )
	sa:=db.RwKV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber=11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
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
	if err!=nil {
		t.Fatal(err)
	}

	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()
	for !(sb.IsFinished(20) && sb.Cleaned(20)){}
	fmt.Println("finished 20", time.Since(tt))



	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}

	err = GenerateHeaderData(db, 21, 31)
	if err!=nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&currentSnapshotBlock, 30)

	tt=time.Now()
	for !(sb.IsFinished(30) && sb.Cleaned(30)){}
	fmt.Println("finished 30", time.Since(tt))
	headerNumber = 31
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=32 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	if headerNumber!=31 {
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
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=32 {
		t.Fatal(headerNumber)
	}

	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 20)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}
}

func TestBuildHeadersSnapshotAsyncWithNotStoppedTx(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(dir)
	defer func() {
		err = os.RemoveAll(dir)
		t.Log(err)
	}()
	snapshotsDir:=path.Join(dir, "snapshots")
	err= os.Mkdir(snapshotsDir, os.ModePerm)
	if err!=nil {
		t.Fatal(err)
	}
	db:=ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator{
		SnapshotDir: snapshotsDir,
		toRemove: make(map[string]struct{}),
	}
	currentSnapshotBlock:=uint64(10)
	go func() {
		for {
			snBlock:=atomic.LoadUint64(&currentSnapshotBlock)
			err = sb.CreateHeadersSnapshot(db, snBlock)
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.ReplaceHeadersSnapshot(db)
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.RemoveHeadersData(db)
			if err!=nil {
				t.Fatal(err)
			}

			err = sb.RemovePreviousVersion()
			if err!=nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !(sb.IsFinished(10) && sb.Cleaned(10)) {}
	fmt.Println("finished", time.Since(tt), sb.IsFinished(10), sb.Cleaned(10) )
	sa:=db.RwKV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber=11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
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
	if err!=nil {
		t.Fatal(err)
	}

	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}



	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}

	tx,err:=db.Begin(context.Background(), ethdb.RO)
	if err!=nil {
		t.Fatal(err)
	}
	tx.Get(dbutils.HeadersBucket, []byte{})
	defer tx.Rollback()


	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()

	fmt.Println("wait finished")
	c:=time.After(time.Second*5)
	for !(sb.IsFinished(20) && sb.Cleaned(20)){
		select {
			case <-c:
				fmt.Println("+Rollback")
				tx.Rollback()
				fmt.Println("-Rollback", sb.Replacing)
			default:

		}
	}
	fmt.Println("finished 20", time.Since(tt))



	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}

}

func TestSimplifiedBuildHeadersSnapshotAsyncWithNotStoppedTx(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(dir)
	defer func() {
		err = os.RemoveAll(dir)
		t.Log(err)
	}()
	snapshotsDir:=path.Join(dir, "snapshots")
	err= os.Mkdir(snapshotsDir, os.ModePerm)
	if err!=nil {
		t.Fatal(err)
	}
	db:=ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator{
		SnapshotDir: snapshotsDir,
		toRemove: make(map[string]struct{}),
	}
	currentSnapshotBlock:=uint64(10)
	go func() {
		for {
			snBlock:=atomic.LoadUint64(&currentSnapshotBlock)
			err = sb.CreateHeadersSnapshot(db, snBlock)
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.ReplaceHeadersSnapshot(db)
			if err!=nil {
				t.Fatal(err)
			}
			err = sb.RemoveHeadersData(db)
			if err!=nil {
				t.Fatal(err)
			}

			err = sb.RemovePreviousVersion()
			if err!=nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !(sb.IsFinished(10) && sb.Cleaned(10)) {}
	fmt.Println("finished", time.Since(tt), sb.IsFinished(10), sb.Cleaned(10) )
	sa:=db.RwKV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber=11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
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
	if err!=nil {
		t.Fatal(err)
	}

	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}



	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}

	tx,err:=db.Begin(context.Background(), ethdb.RO)
	if err!=nil {
		t.Fatal(err)
	}
	tx.Get(dbutils.HeadersBucket, []byte{})
	defer tx.Rollback()


	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()

	fmt.Println("wait finished")
	c:=time.After(time.Second*5)
	for !(sb.IsFinished(20) && sb.Cleaned(20)){
		select {
			case <-c:
				fmt.Println("+Rollback")
				tx.Rollback()
				fmt.Println("-Rollback", sb.Replacing)
			default:

		}
	}
	fmt.Println("finished 20", time.Since(tt))



	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}

}
func TestSnapshotMigrator2(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(dir)
	defer func() {
		err = os.RemoveAll(dir)
		t.Log(err)
	}()
	snapshotsDir:=path.Join(dir, "snapshots")
	err= os.Mkdir(snapshotsDir, os.ModePerm)
	if err!=nil {
		t.Fatal(err)
	}
	btCli,err:=bittorrent.New(snapshotsDir, true, "12345123451234512345")
	if err!=nil {
		t.Fatal(err)
	}
	db:=ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator2{
		snapshotsDir: snapshotsDir,
	}
	currentSnapshotBlock:=uint64(10)
	go func() {
		for {
			tx, err := db.Begin(context.Background(), ethdb.RW)
			if err!=nil {
				tx.Rollback()
				t.Fatal(err)
			}
			snBlock:=atomic.LoadUint64(&currentSnapshotBlock)
			err = sb.Migrate(db, tx, snBlock, btCli)
			if err!=nil {
				tx.Rollback()
				t.Fatal(err)
			}
			err = tx.Commit()
			if err!=nil {
				t.Fatal(err)
			}
			tx.Rollback()
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !(sb.Finished(10)) {
		fmt.Println("processing", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage),!(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
		time.Sleep(time.Second)
	}
	fmt.Println("finished", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage), !(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
	sa:=db.RwKV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber=11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Log(k)
		}
		headerNumber++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
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
	if err!=nil {
		t.Fatal(err)
	}

	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	trnts:=btCli.Torrents()
	fmt.Println("trnts", trnts)


	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}

	tx,err:=db.Begin(context.Background(), ethdb.RO)
	if err!=nil {
		t.Fatal(err)
	}
	tx.Get(dbutils.HeadersBucket, []byte{})
	defer tx.Rollback()


	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()

	rollbacked:=false
	fmt.Println("wait finished")
	c:=time.After(time.Second*3)
	for !(sb.Finished(20)) {
		select {
		case <-c:
			tx.Rollback()
			rollbacked=true
		default:
		}
		fmt.Println("processing", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage),!(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
		time.Sleep(time.Second)
	}
	fmt.Println("finished 20", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage), !(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))

	if !rollbacked {
		t.Fatal("it's not possible to close db without rollback. something went wrong")
	}



	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	//pringSbState(sb)
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	trnts = btCli.Torrents()
	fmt.Println("trnts 20 ",trnts)
	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}
	t.Log("Success")

}

func TestSnapshotMigratorStage(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(dir)
	defer func() {
		err = os.RemoveAll(dir)
		t.Log(err)
	}()
	snapshotsDir:=path.Join(dir, "snapshots")
	err= os.Mkdir(snapshotsDir, os.ModePerm)
	if err!=nil {
		t.Fatal(err)
	}
	btCli,err:=bittorrent.New(snapshotsDir, true, "12345123451234512345")
	if err!=nil {
		t.Fatal(err)
	}
	db:=ethdb.MustOpen(path.Join(dir, "chaindata"))
	db.SetRwKV(ethdb.NewSnapshotKV().DB(db.RwKV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator2{
		snapshotsDir: snapshotsDir,
	}
	currentSnapshotBlock:=uint64(10)
	go func() {
		for {
			tx, err := db.Begin(context.Background(), ethdb.RW)
			if err!=nil {
				tx.Rollback()
				t.Fatal(err)
			}
			snBlock:=atomic.LoadUint64(&currentSnapshotBlock)
			err = sb.Migrate(db, tx, snBlock, btCli)
			if err!=nil {
				tx.Rollback()
				t.Fatal(err)
			}
			err = tx.Commit()
			if err!=nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !(sb.Finished(10)) {
		fmt.Println("processing", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage),!(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
		time.Sleep(time.Second)
	}
	fmt.Println("finished", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage), !(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
	sa:=db.RwKV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())

	var headerNumber uint64
	headerNumber=11
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Log(k)
		}
		headerNumber++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV))
	headerNumber = 0
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
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
	if err!=nil {
		t.Fatal(err)
	}

	if headerNumber!=12 {
		t.Fatal(headerNumber)
	}


	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}

	tx,err:=db.Begin(context.Background(), ethdb.RO)
	if err!=nil {
		t.Fatal(err)
	}
	tx.Get(dbutils.HeadersBucket, []byte{})
	defer tx.Rollback()


	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()

	rollbacked:=false
	fmt.Println("wait finished")
	c:=time.After(time.Second*3)
	for !(sb.Finished(20)) {
		select {
		case <-c:
			tx.Rollback()
			rollbacked=true
		default:
		}
		fmt.Println("processing", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage),!(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))
		time.Sleep(time.Second)
	}
	fmt.Println("finished 20", time.Since(tt),atomic.LoadUint64(&sb.HeadersNewSnapshot), atomic.LoadUint64(&sb.HeadersCurrentSnapshot), atomic.LoadUint64(&sb.Stage), !(atomic.LoadUint64(&sb.HeadersNewSnapshot)!= atomic.LoadUint64(&sb.HeadersCurrentSnapshot) && sb.Stage!=StageStart&& atomic.LoadUint64(&sb.HeadersCurrentSnapshot)!=currentSnapshotBlock))

	if !rollbacked {
		t.Fatal("it's not possible to close db without rollback. something went wrong")
	}



	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		t.Fatal("main db must be empty here")
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	headerNumber=0
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket).(ethdb.RwKV)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	//pringSbState(sb)
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	headerNumber=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if !bytes.Equal(k, dbutils.HeaderKey(headerNumber, common.Hash{uint8(headerNumber)})) {
			t.Fatal(k)
		}
		headerNumber++

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	if headerNumber!=21 {
		t.Fatal(headerNumber)
	}
	if _,err = os.Stat(SnapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}
	t.Log("Success")

}

func pringSbState(sb *SnapshotMigrator)  {
	fmt.Println("to block", sb.MigrateToHeadersSnapshotBlock)
	fmt.Println("current block", sb.CurrentHeadersSnapshotBlock)
	fmt.Println("HeadersSnapshotGeneration", sb.HeadersSnapshotGeneration)
	fmt.Println("HeadersSnapshotReady", sb.HeadersSnapshotReady)
	fmt.Println("Replacing", sb.Replacing)
	fmt.Println("Claned to", sb.CleanedTo)
	fmt.Println("to clean", sb.toClean)
	fmt.Println("to remove", sb.toRemove)
}
func GenerateHeaderData(db ethdb.Database, from, to int) error  {
	tx,err:=db.Begin(context.Background(),ethdb.RW)
	if err!=nil {
		return err
	}
	defer tx.Rollback()
	if to>math.MaxInt8 {
		return errors.New("greater than uint8")
	}
	for i:=from; i<=to; i++ {
		err =tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(uint64(i), common.Hash{uint8(i)}), []byte{uint8(i)})
		if err!=nil {
			return err
		}
		err =tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(uint64(i)), common.Hash{uint8(i)}.Bytes())
		if err!=nil {
			return err
		}
	}
	return tx.Commit()
}

type torrentStub struct{
	SeedStub func(db ethdb.Database, networkID uint64, path string) (metainfo.Hash, error)
	StopStub func(hash metainfo.Hash) error
}

func (s *torrentStub) SeedSnapshot(db ethdb.Database, networkID uint64, path string) (metainfo.Hash, error) {
	return s.SeedStub(db, networkID, path)
}

func (s *torrentStub) StopSeeding(hash metainfo.Hash) error {
	return s.StopStub(hash)
}


