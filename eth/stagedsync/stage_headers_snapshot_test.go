package stagedsync

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"math/big"
	"os"
	"testing"
)

func TestHeadersGenerateIndex(t *testing.T) {
	snPath:=os.TempDir()+"sn"
	snVK:=ethdb.NewLMDB().Path(snPath).MustOpen()
	defer os.RemoveAll(snPath)
	headers:=generateHeaders(10)
	err:=snVK.Update(context.Background(), func(tx ethdb.Tx) error {
		for _,header:=range headers {
			headerBytes,err:=rlp.EncodeToBytes(header)
			if err!=nil {
				panic(err)
			}
			fmt.Println("Put", header.Number, len(headerBytes))
			err = tx.Cursor(dbutils.HeaderPrefix).Put(dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), headerBytes)
			if err!=nil {
				panic(err)
			}
		}
		return nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	snVK.Close()

	db:=ethdb.NewLMDB().InMem().MustOpen()
	snKV:=ethdb.NewSnapshotKV().Path(snPath).DB(db).Open()
	err=GenerateHeaderIndexes(ethdb.NewObjectDatabase(snKV))
	if err!=nil {
		t.Fatal(err)
	}
	snDB:=ethdb.NewObjectDatabase(snKV)
	td:=big.NewInt(0)
	for i, header:=range headers {
		td=td.Add(td, header.Difficulty)
		canonical:=rawdb.ReadCanonicalHash(snDB,header.Number.Uint64())
		fmt.Println(canonical)
		if canonical!=header.Hash() {
			t.Error(i, "canonical not correct", canonical)
		}

		hasHeader:=rawdb.HasHeader(snDB, header.Hash(), header.Number.Uint64())
		if !hasHeader {
			t.Error(i, header.Hash(), header.Number.Uint64(),"not exists")
		}
		headerNumber:=rawdb.ReadHeaderNumber(snDB, header.Hash())
		if headerNumber==nil {
			t.Error(i,"empty header number")
		} else if *headerNumber!= header.Number.Uint64() {
			t.Error(i, header.Hash(), header.Number.Uint64(),"header number incorrect")
		}
		if td==nil {
			t.Error(i, "empty td")
		} else  if td.Cmp(rawdb.ReadTd(snDB, header.Hash(), header.Number.Uint64()))!=0{
			t.Error(i, header.Hash(), header.Number.Uint64(),"td incorrect")
		}
	}
}

func generateHeaders(n int) []types.Header  {
	headers:=make([]types.Header, n)
	for i:=uint64(0);i<uint64(n); i++ {
		headers[i]=types.Header{Difficulty: new(big.Int).SetUint64(i),Number: new(big.Int).SetUint64(i)}
	}
	return headers
}