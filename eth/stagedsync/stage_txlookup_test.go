package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"golang.org/x/sync/errgroup"
	"math/big"
	"os"
	"testing"
)

func TestGenerateTxLookup(t *testing.T) {
	db,_:=ethdb.NewBoltDatabase("/media/b00ris/nvme/tgstaged/geth/chaindata")
	i:=1000
	b:=make([]byte, 0, 10000000*common.HashLength)
	err:=db.Walk(dbutils.HeaderPrefix,[]byte{}, 0, func(k, v []byte) (bool, error) {
		//fmt.Println(i, common.Bytes2Hex(k), len(v))
		if dbutils.CheckCanonicalKey(k) {
			//fmt.Println(i,len(v))
			b=append(b, v...)
		} else {
			return true, nil
		}
		if len(v) != common.HashLength {
			return false, errors.New("incorrect len")
		}

		if i==0 {
			return false, nil
		}
		//i--
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println(len(b))
}
func TestGenerateTxLookup3(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	db,_:=ethdb.NewBoltDatabase("/media/b00ris/nvme/tgstaged/geth/chaindata")
	txLookupBucket:=append(dbutils.TxLookupPrefix, []byte("test2")...)
	db.DeleteBucket(txLookupBucket)
	quit:=make(chan struct{})
	//timer:=time.After(time.Minute*3)
	//go func() {
	//	<-timer
	//	close(quit)
	//}()

	startKey:=dbutils.HeaderHashKey(7000000)
	endKey:=dbutils.HeaderHashKey(7300000)
	err:=etl.Transform(db,dbutils.HeaderPrefix,txLookupBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		if !dbutils.CheckCanonicalKey(k) {
			return nil
		}
		blocknum:=binary.BigEndian.Uint64(k)
		body := rawdb.ReadBody(db, common.BytesToHash(v), blocknum)
		if body == nil {
			log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
			return nil
		}
		blocknumBytes:=make([]byte,8)
		blocknumOrig:=make([]byte,8)
		for _, tx := range body.Transactions {
			binary.BigEndian.PutUint64(blocknumBytes, blocknum)
			binary.LittleEndian.PutUint64(blocknumOrig, blocknum)
			err:=next(blocknumOrig, tx.Hash().Bytes(), blocknumBytes)
			if err!=nil {
				return err
			}
		}
		return nil
	}, func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
		return next(k,value)
	}, etl.TransformArgs{Quit: quit, ExtractEndKey: endKey, ExtractStartKey: startKey})
	if err!=nil {
 		t.Fatal(err)
	}

}

func TestGenerateTxLookup3_Experiment(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	db,_:=ethdb.NewBoltDatabase("/media/b00ris/nvme/tgstaged/geth/chaindata")

	quit:=make(chan struct{})
	//timer:=time.After(time.Minute*3)
	//go func() {
	//	<-timer
	//	close(quit)
	//}()
	startKey:=dbutils.HeaderHashKey(7000000)
	endKey:=dbutils.HeaderHashKey(7100000)
	//loadTime 3m32.876286885s
	//loadTime 3m56.486721504s
	//  (382.46s)
	// from 7 to 8 loadTime 18m12.876813361s (1685.61s)
	//full loadTime 1.50  (11283.24s)
	//INFO [06-09|22:06:33.269] Extraction finished                      it took=1m35.243559479s
	//INFO [06-09|22:07:14.283] Collection finished                      it took=41.013359335s
	t.Run("1", func(t *testing.T) {
		txLookupBucket:=append(dbutils.TxLookupPrefix, []byte("test3")...)
		db.DeleteBucket(txLookupBucket)
		err:=etl.Transform(db,dbutils.HeaderPrefix,txLookupBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if !dbutils.CheckCanonicalKey(k) {
				return nil
			}
			blocknum:=binary.BigEndian.Uint64(k)
			body := rawdb.ReadBody(db, common.BytesToHash(v), blocknum)
			if body == nil {
				log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
				return nil
			}
			blocknumBytes:=make([]byte,8)
			for _, tx := range body.Transactions {
				binary.BigEndian.PutUint64(blocknumBytes, blocknum)
				err:=next(k, tx.Hash().Bytes(), blocknumBytes)
				if err!=nil {
					return err
				}
			}
			return nil
		},  func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
			return next(k,value)
		}, etl.TransformArgs{Quit: quit, ExtractEndKey: endKey, ExtractStartKey: startKey})
		if err!=nil {
			t.Fatal(err)
		}
	})
	//loadTime 4m3.514260032s
	//loadTime 2m47.481593132s
	//--- PASS: TestGenerateTxLookup3_Experiment/2 (300.68s)
	//from 7 to 8 loadTime 17m46.684844364s 1610.34s
	// full loadTime 2h1m54.454463304s (11844.04s)
	//INFO [06-09|22:09:12.458] Extraction finished                      it took=1m18.437096161s
	//INFO [06-09|22:09:52.042] Collection finished                      it took=39.583994281s

	t.Run("2", func(t *testing.T) {
		txLookupBucket:=append(dbutils.TxLookupPrefix, []byte("test4")...)
		db.DeleteBucket(txLookupBucket)

		//startKey2:=dbutils.HeaderHashKey(7150000)
		err:=etl.Transform(db,dbutils.HeaderPrefix,txLookupBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if !dbutils.CheckCanonicalKey(k) {
				return nil
			}
			blocknum:=binary.BigEndian.Uint64(k)
			body := rawdb.ReadBody(db, common.BytesToHash(v), blocknum)
			if body == nil {
				log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
				return nil
			}
			//blocknumBytes:=make([]byte,8)
			for _, tx := range body.Transactions {
				err:=next(k, tx.Hash().Bytes(), big.NewInt(0).SetUint64(blocknum).Bytes())
				if err!=nil {
					return err
				}
			}
			return nil
		},  func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
			//if len(value)!=8 {
			//	return errors.New("incorrect uint64 value")
			//}
			return next(k,value)
		}, etl.TransformArgs{
			Quit: quit,
			ExtractStartKey: startKey,
			ExtractEndKey:endKey,
			BufferType: etl.SortableAppendBuffer,
		})
		if err!=nil {
			t.Fatal(err)
		}
	})

	t.Run("check", func(t *testing.T) {
		txLookupBucket1:=append(dbutils.TxLookupPrefix, []byte("test3")...)
		txLookupBucket2:=append(dbutils.TxLookupPrefix, []byte("test4")...)
		db.Walk(txLookupBucket1,[]byte{}, 0, func(k, v []byte) (bool, error) {
			vv, err:=db.Get(txLookupBucket2, k)
			if err!=nil {
				log.Error("walk err", "err", err)
			}
			if !bytes.Equal(v, vv) {
				log.Warn("not eqal", "k", common.Bytes2Hex(k))
			}
			return true, nil
		})
	})
	t.Run("check1", func(t *testing.T) {
		txLookupBucket1:=append(dbutils.TxLookupPrefix, []byte("test3")...)
		validateTxLookups2(db,txLookupBucket1,7000000, 7100000, nil)
	})
	t.Run("check2", func(t *testing.T) {
		txLookupBucket1:=append(dbutils.TxLookupPrefix, []byte("test4")...)
		validateTxLookups2(db,txLookupBucket1,7000000, 7100000, nil)
	})


}


const emptyValBit uint64 = 0x8000000000000000
func TestAccountIndex(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	db,_:=ethdb.NewBoltDatabase("/media/b00ris/nvme/tgstaged/geth/chaindata")

	quit:=make(chan struct{})
	//timer:=time.After(time.Minute*3)
	//go func() {
	//	<-timer
	//	close(quit)
	//}()
	startKey:=dbutils.EncodeTimestamp(7000000)
	endKey:=dbutils.EncodeTimestamp(7100000)
	//loadTime 1m55.192892081s
	//loadTime 2m35.023655533s
	//loadTime 2m7.583543305s
	//(250.39s)
	//loadTime 2m52.227431655s (279.48s)
	//from 7 to 8 loadTime 11m4.51053412s (1071.20s)
	t.Run("1", func(t *testing.T) {
		txLookupBucket:=append(dbutils.AccountsHistoryBucket, []byte("test2")...)
		db.DeleteBucket(txLookupBucket)
		err:=etl.Transform(db,dbutils.PlainAccountChangeSetBucket,txLookupBucket, os.TempDir(), func(dbKey []byte, dbValue []byte, next etl.ExtractNextFunc) error {
			bufferMap := make(map[string][][]byte)
			blockNum, _ := dbutils.DecodeTimestamp(dbKey)
			err := changeset.AccountChangeSetPlainBytes(dbValue).Walk(func(changesetKey, changesetValue []byte) error {
				sKey := string(changesetKey)
				list := bufferMap[sKey]
				b := blockNum
				v:=make([]byte, 9)
				binary.BigEndian.PutUint64(v,b)
				if len(changesetValue) == 0 {
					v[8] = 1
				}
				list = append(list, v)

				bufferMap[sKey] = list
				return nil
			})
			if err != nil {
				return err
			}

			for k, v := range bufferMap {
				for i:=range v {
					err = next(dbKey, []byte(k), v[i])
				}
				if err != nil {
					return err
				}
			}
			return nil
		}, loadFunc, etl.TransformArgs{Quit: quit, ExtractStartKey: startKey, ExtractEndKey:endKey})
		if err!=nil {
			t.Fatal(err)
		}
	})
	//loadTime 54.347289346s
	//loadTime 56.038327065s
	//loadTime 38.168450753s
	//(99.13s)
	//loadTime 40.545391288s (79.03s)
	//without threads loadTime 58.042606815s (96.88s)
	//from 7 to 8 (448.26s) loadTime 5m13.628932969s
	t.Run("2", func(t *testing.T) {
		txLookupBucket:=append(dbutils.AccountsHistoryBucket, []byte("test3")...)
		db.DeleteBucket(txLookupBucket)
		//startKey2:=dbutils.EncodeTimestamp(7150000)
		err:=etl.Transform(db,dbutils.PlainAccountChangeSetBucket,txLookupBucket, os.TempDir(), func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
			bufferMap := make(map[string][][]byte)
			blockNum, _ := dbutils.DecodeTimestamp(dbKey)

			err := changeset.AccountChangeSetPlainBytes(dbValue).Walk(func(changesetKey, changesetValue []byte) error {
				sKey := string(changesetKey)
				list := bufferMap[sKey]
				b := blockNum
				v:=make([]byte, 9)
				binary.BigEndian.PutUint64(v,b)
				if len(changesetValue) == 0 {
					v[8] = 1
				}

				list = append(list, v)
				bufferMap[sKey] = list
				return nil
			})
			if err != nil {
				return err
			}

			for k, v := range bufferMap {
				for i:=range v {
					err = next(dbKey, []byte(k), v[i])
					if err != nil {
						return err
					}
				}
			}
			return nil
		}, loadFunc2, etl.TransformArgs{Quit: quit, ExtractStartKey: startKey, ExtractEndKey: endKey, BufferType: etl.SortableAppendBuffer})
		if err!=nil {
			t.Fatal(err)
		}
	})


}

func loadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error { //nolint
	if len(value)!=9 {
		log.Error("strange length", common.Bytes2Hex(k))
		return nil
	}
	blockNr:=binary.BigEndian.Uint64(value)
	vzero:=value[8]==1
	currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
	indexBytes, err1 := state.Get(currentChunkKey)
	if err1 != nil && err1 != ethdb.ErrKeyNotFound {
		return fmt.Errorf("find chunk failed: %w", err1)
	}
	var index dbutils.HistoryIndexBytes
	if len(indexBytes) == 0 {
		index = dbutils.NewHistoryIndex()
	} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
		// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
		index = dbutils.WrapHistoryIndex(indexBytes)
		indexKey, err3 := index.Key(k)
		if err3 != nil {
			return err3
		}
		// Flush the old chunk
		if err4 := next(indexKey, index); err4 != nil {
			return err4
		}
		// Start a new chunk
		index = dbutils.NewHistoryIndex()
	} else {
		index = dbutils.WrapHistoryIndex(indexBytes)
	}
	index = index.Append(blockNr, vzero)

	return next(currentChunkKey, index)
}

func loadFunc2(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error { //nolint
	if len(value)%9 != 0 {
		log.Error("Strange value", "ln", len(value), "k", common.Bytes2Hex(k))
		return nil
	}

	currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
	indexBytes, err1 := state.Get(currentChunkKey)
	if err1 != nil && err1 != ethdb.ErrKeyNotFound {
		return fmt.Errorf("find chunk failed: %w", err1)
	}

	currentIndex:=dbutils.WrapHistoryIndex(indexBytes)


	for i:=0;i<len(value); i+=9 {
		b:=binary.BigEndian.Uint64(value[i:])
		vzero := value[i+8]==1
		blockNr := b
		if err1 != nil && err1 != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err1)
		}

		if dbutils.CheckNewIndexChunk(currentIndex, blockNr) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			indexKey, err3 := currentIndex.Key(k)
			if err3 != nil {
				return err3
			}
			// Flush the old chunk
			if err4 := next(indexKey, currentIndex); err4 != nil {
				return err4
			}
			// Start a new chunk
			currentIndex = dbutils.NewHistoryIndex()
		}
		currentIndex = currentIndex.Append(blockNr, vzero)
	}
	err := next(currentChunkKey, currentIndex)
	if err != nil {
		return err
	}

	return nil
}


/*
go throw canonical
=== RUN   TestGenerateTxLookup
--- PASS: TestGenerateTxLookup (283.74s)

4 threads
=== RUN   TestGenerateTxLookup2
--- PASS: TestGenerateTxLookup2 (67.90s)
*/
func TestGenerateTxLookup2(t *testing.T) {
	db,_:=ethdb.NewBoltDatabase("/media/b00ris/nvme/tgstaged/geth/chaindata")

	wg,_:=errgroup.WithContext(context.TODO())
	startKeys:=[][]byte{dbutils.HeaderHashKey(0),dbutils.HeaderHashKey(2500000),dbutils.HeaderHashKey(5000000),dbutils.HeaderHashKey(7500000)}
	for i,v:=range startKeys {
		vv:=common.CopyBytes(v)
		withoutEnd:=false

		var endKey []byte
		if i==len(startKeys)-1 {
			withoutEnd=true
		}  else {
			endKey=common.CopyBytes(startKeys[i+1])
		}
		stop:= func(k []byte) bool {
			if withoutEnd {
				return false
			}
			return bytes.Compare(k, endKey) >= 0
		}
		wg.Go(func() error {
			b:=make([]byte, 0, 3000000*common.HashLength)

			err :=  db.Walk(dbutils.HeaderPrefix,vv, 0, func(k, v []byte) (bool, error) {
				if stop(k) {
					return false, nil
				}
				//fmt.Println(i, common.Bytes2Hex(k), len(v))
				if  dbutils.CheckCanonicalKey(k){
					//fmt.Println(i,len(v))
					b=append(b, v...)
				} else {
					return true, nil
				}
				if len(v) != common.HashLength {
					return false, errors.New("incorrect len")
				}

				if i==0 {
					return false, nil
				}
				//i--
				return true, nil
			})
			if err!=nil{
				return err
			}
			//fmt.Println(len(b))
			return nil
		})
	}
	err := wg.Wait()
	if err!=nil {
		t.Fatal(err)
	}
}


func validateTxLookups2(db *ethdb.BoltDatabase, txLookupBucket []byte, startBlock, endBlock uint64, interruptCh chan bool) {
	blockNum := startBlock
	iterations := 0
	var interrupt bool
	// Validation Process
	blockBytes := big.NewInt(0)
	for !interrupt {
		if blockNum >= endBlock {
			break
		}
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)

		if body == nil {
			break
		}

		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		blockBytes.SetUint64(blockNum)
		bn := blockBytes.Bytes()

		for _, tx := range body.Transactions {
			val, err := db.Get(txLookupBucket, tx.Hash().Bytes())
			iterations++
			if iterations%100000 == 0 {
				log.Info("Validated", "entries", iterations, "number", blockNum)
			}
			if bytes.Compare(val, bn) != 0 {
				if err != nil {
					panic(err)
				}
				panic(fmt.Sprintf("Validation process failed(%d). Expected %b, got %b", iterations, bn, val))
			}
		}
		blockNum++
	}
}




/*
=== RUN   TestAccountIndex/1
INFO [06-09|21:52:49.485] Extraction finished                      it took=1m7.758850506s
INFO [06-09|21:53:22.796] Collection finished                      it took=33.310956224s

=== RUN   TestAccountIndex/2
INFO [06-09|21:54:19.237] Extraction finished                      it took=16.22532524s
INFO [06-09|21:54:34.561] Collection finished                      it took=15.323835597s
PASS*/