package torrent

import (
	"bytes"
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"math/big"
	"sort"
)
//
//func SpawnHeadersSnapshotDownload(s *StageState, db ethdb.Database, dataDir string, quitCh <-chan struct{}) error {
//	if v, _:=db.Get(dbutils.DatabaseInfoBucket, []byte(dbutils.SnapshotBodyHeadNumber)); len(v)>0 {
//
//	}
//	err:=fileutil.CopyDirs("/media/b00ris/nvme/snapshots/headers2","/media/b00ris/nvme/snapshots/headers")
//	if err!=nil {
//		log.Error("Fail to copy")
//		return err
//	}
//
//	return GenerateHeaderIndexes(db, quitCh, s)
//}

func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	toCommit:=uint64(200000)
	currentKey:=[]byte{}
	tuple:=make(ethdb.MultiPutTuples, 0, toCommit*(3+3))
	td := big.NewInt(0)
	var number uint64
	var hash common.Hash
	var i uint64
	for {
		select {
		case <-ctx.Done():
			err:=db.Put(dbutils.SnapshotInfoBucket, []byte("headersProcessing"), currentKey)
			if err!=nil {
				return err
			}
			return ctx.Err()
		default:
		}

		stop:=true
		err:=db.Walk(dbutils.HeaderPrefix,currentKey, 0, func(k []byte, v []byte) (bool, error) {
			if bytes.Equal(k, currentKey) {
				return true, nil
			}

			if len(k)!=8+common.HashLength {
				return true, nil
			}
			header:=&types.Header{}
			err:=rlp.DecodeBytes(v,header)
			if err!=nil {
				return false, err
			}
			number=header.Number.Uint64()
			hash=header.Hash()

			//write blocknum to header hash index
			tuple = append(tuple, []byte(dbutils.HeaderNumberPrefix), header.Hash().Bytes(), dbutils.EncodeBlockNumber(header.Number.Uint64()))
			td = td.Add(td, header.Difficulty)
			td, err := rlp.EncodeToBytes(td)
			if err != nil {
				log.Crit("Failed to RLP encode block total difficulty", "err", err)
				return false, err
			}
			//write header number to td index
			tuple=append(tuple,[]byte(dbutils.HeaderPrefix), dbutils.HeaderTDKey(header.Number.Uint64(), header.Hash()), td)
			//write canonical
			tuple=append(tuple,[]byte(dbutils.HeaderPrefix), dbutils.HeaderHashKey(header.Number.Uint64()), header.Hash().Bytes())
			i++
			if i%toCommit==0 {
				currentKey=common.CopyBytes(k)
				stop=false
				return false, nil
			}
			return true, nil
		})
		if err!=nil {
			return err
		}
		//err = etl.Transform(db,dbutils.HeaderPrefix,dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		//
		//}, func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
		//
		//})
		sort.Sort(tuple)
		log.Info("Commit ", "i", i, "number", number)
		_, err = db.MultiPut(tuple...)
		if err!=nil {
			return err
		}

		if stop {
			break
		}
	}
	rawdb.WriteHeadHeaderHash(db, hash)

	return nil
}