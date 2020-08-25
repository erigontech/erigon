package stagedsync

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"math/big"
	"sort"
)

func SpawnHeadersSnapshotDownload(s *StageState, db ethdb.Database, dataDir string, quitCh <-chan struct{}) error {
	return GenerateHeaderIndexes(db, quitCh, s)
}

func GenerateHeaderIndexes(db ethdb.Database, quitCh <-chan struct{}, s *StageState) error {
	toCommit:=uint64(20000)
	currentKey:=[]byte{}
	tuple:=make(ethdb.MultiPutTuples, 0, toCommit*(3+3))
	td := big.NewInt(0)
	var number uint64
	var i uint64
	for {
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		stop:=true
		err:=db.Walk(dbutils.HeaderPrefix,currentKey, 0, func(k []byte, v []byte) (bool, error) {
		//	log.Info("Key","k", common.Bytes2Hex(k))
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
			//write blocknum to header hash index
			//todo check that moved to separated stage
			//tuple = append(tuple, []byte(dbutils.HeaderNumberPrefix), header.Hash().Bytes(), dbutils.EncodeBlockNumber(header.Number.Uint64()))
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

	return s.DoneAndUpdate(db, number)
}