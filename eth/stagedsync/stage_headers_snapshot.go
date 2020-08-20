package stagedsync

import (
	"bytes"
	"fmt"
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
	//todo download

	return GenerateHeaderIndexes(db)
}

func GenerateHeaderIndexes(db ethdb.Database) error {
	toCommit:=uint64(20000)
	currentKey:=[]byte{}
	tuple:=make(ethdb.MultiPutTuples, 0, toCommit*(3+3+3))
	td := big.NewInt(0)
	var i uint64
	for {
		stop:=true
		err:=db.Walk(dbutils.HeaderPrefix,currentKey, 0, func(k []byte, v []byte) (bool, error) {
			fmt.Println(string(k), common.Bytes2Hex(k))
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
			//write blocknum to header hash index
			tuple = append(tuple, []byte(dbutils.HeaderNumberPrefix), dbutils.EncodeBlockNumber(header.Number.Uint64()), header.Hash().Bytes())
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
		_, err = db.MultiPut(tuple...)
		if err!=nil {
			return err
		}

		if stop {
			break
		}
	}
	return nil
}