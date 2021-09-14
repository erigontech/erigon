package snapshotsync

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	mdbx2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

func TestHeadersGenerateIndex(t *testing.T) {
	snPath := t.TempDir()
	snKV := mdbx2.NewMDBX(log.New()).Path(snPath).MustOpen()
	defer os.RemoveAll(snPath)
	headers := generateHeaders(10)
	err := snKV.Update(context.Background(), func(tx kv.RwTx) error {
		for _, header := range headers {
			headerBytes, innerErr := rlp.EncodeToBytes(header)
			if innerErr != nil {
				panic(innerErr)
			}
			innerErr = tx.Put(kv.Headers, dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), headerBytes)
			if innerErr != nil {
				panic(innerErr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snKV.Close()

	db := mdbx2.NewMDBX(log.New()).InMem().WithTablessCfg(mdbx2.WithChaindataTables).MustOpen()
	defer db.Close()
	//we need genesis
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteCanonicalHash(tx, headers[0].Hash(), headers[0].Number.Uint64())

	}); err != nil {
		t.Fatal(err)
	}

	snKV = mdbx2.NewMDBX(log.New()).Path(snPath).Flags(func(flags uint) uint { return flags | mdbx.Readonly }).WithTablessCfg(mdbx2.WithChaindataTables).MustOpen()
	defer snKV.Close()

	snKV = snapshotdb.NewSnapshotKV().HeadersSnapshot(snKV).DB(db).Open()
	snTx, err := snKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer snTx.Rollback()

	err = GenerateHeaderIndexes(context.Background(), snTx)
	if err != nil {
		t.Fatal(err)
	}
	td := big.NewInt(0)
	for i, header := range headers {
		td = td.Add(td, header.Difficulty)
		canonical, err1 := rawdb.ReadCanonicalHash(snTx, header.Number.Uint64())
		if err1 != nil {
			t.Errorf("reading canonical hash for block %d: %v", header.Number.Uint64(), err1)
		}
		if canonical != header.Hash() {
			t.Error(i, "canonical not correct", canonical)
		}

		hasHeader := rawdb.HasHeader(snTx, header.Hash(), header.Number.Uint64())
		if !hasHeader {
			t.Error(i, header.Hash(), header.Number.Uint64(), "not exists")
		}
		headerNumber := rawdb.ReadHeaderNumber(snTx, header.Hash())
		if headerNumber == nil {
			t.Error(i, "empty header number")
		} else if *headerNumber != header.Number.Uint64() {
			t.Error(i, header.Hash(), header.Number.Uint64(), "header number incorrect")
		}
		if td == nil {
			t.Error(i, "empty td")
		} else {
			td, err := rawdb.ReadTd(snTx, header.Hash(), header.Number.Uint64())
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
