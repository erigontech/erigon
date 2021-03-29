package snapshotsync

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func TestHeadersGenerateIndex(t *testing.T) {
	snPath := os.TempDir() + "/sn"
	snVK := ethdb.NewLMDB().Path(snPath).MustOpen()
	defer os.RemoveAll(snPath)
	headers := generateHeaders(10)
	err := snVK.Update(context.Background(), func(tx ethdb.RwTx) error {
		for _, header := range headers {
			headerBytes, innerErr := rlp.EncodeToBytes(header)
			if innerErr != nil {
				panic(innerErr)
			}
			innerErr = tx.RwCursor(dbutils.HeadersBucket).Put(dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), headerBytes)
			if innerErr != nil {
				panic(innerErr)
			}
		}
		c := tx.RwCursor(dbutils.HeadersSnapshotInfoBucket)
		innerErr := c.Put([]byte(dbutils.SnapshotHeadersHeadHash), headers[len(headers)-1].Hash().Bytes())
		if innerErr != nil {
			return innerErr
		}
		innerErr = c.Put([]byte(dbutils.SnapshotHeadersHeadNumber), headers[len(headers)-1].Number.Bytes())
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snVK.Close()

	db := ethdb.NewLMDB().InMem().WithBucketsConfig(ethdb.DefaultBucketConfigs).MustOpen()
	//we need genesis
	err = rawdb.WriteCanonicalHash(ethdb.NewObjectDatabase(db), headers[0].Hash(), headers[0].Number.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	snKV := ethdb.NewLMDB().Path(snPath).Flags(func(flags uint) uint { return flags | lmdb.Readonly }).WithBucketsConfig(ethdb.DefaultBucketConfigs).MustOpen()

	snKV = ethdb.NewSnapshotKV().SnapshotDB([]string{dbutils.HeadersSnapshotInfoBucket, dbutils.HeadersBucket}, snKV).DB(db).Open()
	err = GenerateHeaderIndexes(context.Background(), ethdb.NewObjectDatabase(snKV))
	if err != nil {
		t.Fatal(err)
	}
	snDB := ethdb.NewObjectDatabase(snKV)
	td := big.NewInt(0)
	for i, header := range headers {
		td = td.Add(td, header.Difficulty)
		canonical, err1 := rawdb.ReadCanonicalHash(snDB, header.Number.Uint64())
		if err1 != nil {
			t.Errorf("reading canonical hash for block %d: %v", header.Number.Uint64(), err1)
		}
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
		} else {
			td, err := rawdb.ReadTd(snDB, header.Hash(), header.Number.Uint64())
			if err != nil {
				panic(err)
			}
			if td.Cmp(td) != 0 {
				t.Error(i, header.Hash(), header.Number.Uint64(), "td incorrect")
			}
		}
	}
}

func generateHeaders(n int) []types.Header {
	headers := make([]types.Header, n)
	for i := uint64(0); i < uint64(n); i++ {
		headers[i] = types.Header{Difficulty: new(big.Int).SetUint64(i), Number: new(big.Int).SetUint64(i)}
	}
	return headers
}
