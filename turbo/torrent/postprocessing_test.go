package torrent

import (
	"context"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"math/big"
	"os"
	"testing"
)

func Test1(t *testing.T) {
	{
		env, err := lmdb.NewEnv()
		if err != nil {
			panic(err)
		}
		err = env.SetMaxDBs(100)
		if err != nil {
			panic(err)
		}

		err = env.Open("./a", 0, 0664)
		if err != nil {
			panic(err)
		}

		tx, err := env.BeginTxn(nil, 0)
		if err != nil {
			panic(err)
		}

		dbi, err := tx.OpenDBI("alex", lmdb.Create)
		if err != nil {
			panic(err)
		}
		err = tx.Put(dbi, []byte{1}, []byte{1}, 0)
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		err = env.Close()
		if err != nil {
			panic(err)
		}
	}

	{
		env, err := lmdb.NewEnv()
		if err != nil {
			panic(err)
		}
		err = env.SetMaxDBs(100)
		if err != nil {
			panic(err)
		}
		err = env.Open("./a", lmdb.Readonly, 0664)
		if err != nil {
			panic(err)
		}

		tx, err := env.BeginTxn(nil, lmdb.Readonly)
		if err != nil {
			panic(err)
		}
		dbi, err := tx.OpenDBI("alex", 0)
		if err != nil {
			panic(err)
		}

		c, err := tx.OpenCursor(dbi)
		if err != nil {
			panic(err)
		}
		_, _, err = c.Get([]byte{0}, nil, lmdb.SetRange)
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		err = env.Close()
		if err != nil {
			panic(err)
		}
	}
}

func Test2(t *testing.T) {
	{
		kv := ethdb.NewLMDB().Path("./c").MustOpen()
		tx, err := kv.Begin(context.Background(), nil, true)
		if err != nil {
			panic(err)
		}
		c := tx.Cursor(dbutils.HeaderPrefix)
		err = c.Put([]byte{1}, []byte{1})
		if err != nil {
			panic(err)
		}
		err = tx.Commit(context.Background())
		if err != nil {
			panic(err)
		}
		kv.Close()
	}

	{
		kv := ethdb.NewLMDB().Path("./c").ReadOnly().MustOpen()
		tx, err := kv.Begin(context.Background(), nil, false)
		if err != nil {
			panic(err)
		}
		c := tx.Cursor(dbutils.HeaderPrefix)
		_, _, err = c.Seek([]byte{0})
		if err != nil {
			panic(err)
		}
		tx.Rollback()
		kv.Close()
	}
}

func TestHeadersGenerateIndex(t *testing.T) {
	snPath := os.TempDir() + "/sn"
	snVK := ethdb.NewLMDB().Path(snPath).MustOpen()
	defer os.RemoveAll(snPath)
	headers := generateHeaders(10)
	err := snVK.Update(context.Background(), func(tx ethdb.Tx) error {
		for _, header := range headers {
			headerBytes, err := rlp.EncodeToBytes(header)
			if err != nil {
				panic(err)
			}
			//fmt.Println("Put", header.Number, common.Bytes2Hex(dbutils.HeaderKey(header.Number.Uint64(), header.Hash())), len(headerBytes))
			err = tx.Cursor(dbutils.HeaderPrefix).Put(dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), headerBytes)
			if err != nil {
				panic(err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snVK.Close()

	//ethdb.NewLMDB().WithBucketsConfig()
	db := ethdb.NewLMDB().InMem().WithBucketsConfig(ethdb.DefaultBucketConfigs).MustOpen()
	snKV := ethdb.NewSnapshotKV().For(dbutils.HeaderPrefix).Path(snPath).DB(db).Open()
	err = GenerateHeaderIndexes(context.Background(), ethdb.NewObjectDatabase(snKV))
	if err != nil {
		t.Fatal(err)
	}
	snDB := ethdb.NewObjectDatabase(snKV)
	td := big.NewInt(0)
	for i, header := range headers {
		td = td.Add(td, header.Difficulty)
		canonical := rawdb.ReadCanonicalHash(snDB, header.Number.Uint64())
		if canonical != header.Hash() {
			t.Error(i, "canonical not correct", canonical)
		}

		hasHeader := rawdb.HasHeader(snDB, header.Hash(), header.Number.Uint64())
		if !hasHeader {
			t.Error(i, header.Hash(), header.Number.Uint64(), "not exists")
		}
		headerNumber := rawdb.ReadHeaderNumber(snDB, header.Hash())
		if headerNumber == nil {
			t.Error(i, "empty header number")
		} else if *headerNumber != header.Number.Uint64() {
			t.Error(i, header.Hash(), header.Number.Uint64(), "header number incorrect")
		}
		if td == nil {
			t.Error(i, "empty td")
		} else if td.Cmp(rawdb.ReadTd(snDB, header.Hash(), header.Number.Uint64())) != 0 {
			t.Error(i, header.Hash(), header.Number.Uint64(), "td incorrect")
		}
	}
}

func TestName22(t *testing.T) {
	os.RemoveAll(os.TempDir() + "/tm1")
	//os.RemoveAll(os.TempDir()+"/tm2")
	db1 := ethdb.NewLMDB().Path(os.TempDir() + "/tm1").MustOpen()
	db2 := ethdb.NewLMDB().Path(os.TempDir() + "/tm2").ReadOnly().MustOpen()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	tx, err := db1.Begin(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db2.Begin(context.Background(), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Cursor(dbutils.HeaderPrefix)
	c2 := tx2.Cursor(dbutils.HeaderPrefix)
	_, _, err1 := c.Seek([]byte("sa"))
	_, _, err2 := c2.Seek([]byte("sa"))
	t.Log(err)
	t.Log(err1)
	t.Log(err2)

}
func generateHeaders(n int) []types.Header {
	headers := make([]types.Header, n)
	for i := uint64(0); i < uint64(n); i++ {
		headers[i] = types.Header{Difficulty: new(big.Int).SetUint64(i), Number: new(big.Int).SetUint64(i)}
	}
	return headers
}
