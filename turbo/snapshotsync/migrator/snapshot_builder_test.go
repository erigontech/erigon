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

func TestBuildHeadersSnapshot(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
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
	db.SetKV(ethdb.NewSnapshotKV().DB(db.KV()).Open())
	err=GenerateHeaderData(db,0, 11)
	if err!=nil {
		t.Fatal(err)
	}
	sb:=&SnapshotMigrator{
		SnapshotDir: snapshotsDir,
		toRemove: make(map[string]struct{}),
	}
	err = sb.CreateHeadersSnapshot(db, 10)
	if err!=nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second*10)
	fmt.Println("after CreateHeadersSnapshot")
	pringSbState(sb)
	err = sb.ReplaceHeadersSnapshot(db)
	if err!=nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second*10)
	fmt.Println("after ReplaceHeadersSnapshot")
	pringSbState(sb)
	err = sb.RemoveHeadersData(db)
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("after RemoveHeadersData")
	pringSbState(sb)
	sa:=db.KV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket))
	fmt.Println("walk through snapshot")
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println("add some data ----------------------------------------")

	err = GenerateHeaderData(db, 12, 15)
	if err!=nil {
		t.Fatal(err)
	}
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("walk through snapshot")
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}


	fmt.Println("add some data to 25 ----------------------------------------")

	err = GenerateHeaderData(db, 16, 25)
	if err!=nil {
		t.Fatal(err)
	}

	//err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println("main", k)
	//	return true, nil
	//})
	//if err!=nil {
	//	t.Fatal(err)
	//}
	//fmt.Println("walk through snapshot")
	//err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println("sn", k)
	//	return true, nil
	//})
	//if err!=nil {
	//	t.Fatal(err)
	//}
	//pringSbState(sb)
	//err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println("full", k)
	//	return true, nil
	//})
	//if err!=nil {
	//	t.Fatal(err)
	//}


	fmt.Println("Create snapshot 20 ----------------------------------------")

	err = sb.CreateHeadersSnapshot(db, 20)
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println("after CreateHeadersSnapshot")
	pringSbState(sb)
	time.Sleep(time.Second*10)

	err = sb.ReplaceHeadersSnapshot(db)
	if err!=nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second*10)
	fmt.Println("after ReplaceHeadersSnapshot")
	pringSbState(sb)
	err = sb.RemoveHeadersData(db)
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("after RemoveHeadersData")

	snodb=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket))

	fmt.Println("verify data to 20 ----------------------------------------")

	pringSbState(sb)

	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("walk through snapshot")
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

}

func TestBuildHeadersSnapshotAsync(t *testing.T) {
	dir,err:=ioutil.TempDir(os.TempDir(), "tst")
	if err!=nil {
		t.Fatal(err)
	}
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
	db.SetKV(ethdb.NewSnapshotKV().DB(db.KV()).Open())
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
		ttt:=time.Now()
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
			if time.Since(ttt)> 20*time.Second {
				pringSbState(sb)
				ttt = time.Now()
			}
			time.Sleep(time.Second)
		}
	}()
	tt:=time.Now()
	for !sb.IsFinished(10) && !sb.Cleaned(10) {}
	pringSbState(sb)
	fmt.Println("finished", time.Since(tt))
	sa:=db.KV().(ethdb.SnapshotUpdater)
	wodb:=ethdb.NewObjectDatabase(sa.WriteDB())
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket))
	fmt.Println("walk through snapshot")
	err = snodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println("add data to 20 ----------------------------------------")

	err = GenerateHeaderData(db, 12, 20)
	if err!=nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&currentSnapshotBlock, 20)
	tt=time.Now()
	for !(sb.IsFinished(20) && sb.Cleaned(20)) {}
	fmt.Println("finished 20", time.Since(tt))


	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("walk through snapshot")
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}


	fmt.Println("add some data to 31 ----------------------------------------")

	err = GenerateHeaderData(db, 21, 31)
	if err!=nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&currentSnapshotBlock, 30)

	tt=time.Now()
	for !(sb.IsFinished(30) && sb.Cleaned(30)) {}
	fmt.Println("finished 30", time.Since(tt))
	err = wodb.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("main", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("walk through snapshot")
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("sn", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	pringSbState(sb)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println("full", k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

}

func TestBuildHeadersSnapshotAsync2(t *testing.T) {
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
	db.SetKV(ethdb.NewSnapshotKV().DB(db.KV()).Open())
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
	sa:=db.KV().(ethdb.SnapshotUpdater)
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

	snodb:=ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket))
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
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
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
	if _,err = os.Stat(snapshotName(snapshotsDir, "headers", 10)); os.IsExist(err) {
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
	err = ethdb.NewObjectDatabase(sa.SnapshotKV(dbutils.HeadersBucket)).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
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

	if _,err = os.Stat(snapshotName(snapshotsDir, "headers", 20)); os.IsExist(err) {
		t.Fatal("snapshot exsists")
	}
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

