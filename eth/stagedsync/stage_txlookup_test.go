package stagedsync

import (
	"bufio"
	"bytes"
	"container/heap"
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
	"io"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"
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

		//fmt.Println("eth/stagedsync/stage_txlookup_test.go:70", common.Bytes2Hex(k), common.Bytes2Hex(v))
		//return next(k,k,v)
		return nil
	}, func(k []byte, valueDecoder etl.Decoder, state etl.State, next etl.LoadNextFunc) error {
		var v []byte
		err:=valueDecoder.Decode(&v)
		if err!=nil {
			return err
		}
		vv:=new(big.Int).SetUint64(binary.BigEndian.Uint64(v))
		return next(k,vv.Bytes())
	}, etl.TransformArgs{Quit: quit, ExtractEndKey: endKey, ExtractStartKey: startKey})
	if err!=nil {
 		t.Fatal(err)
	}

}

func TestGenerateTxLookup3_Experiment(t *testing.T) {
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
	endKey:=dbutils.HeaderHashKey(8000000)
	//loadTime 3m32.876286885s
	//loadTime 3m56.486721504s
	//  (382.46s)
	// from 7 to 8 loadTime 18m12.876813361s (1685.61s)
	t.Run("1", func(t *testing.T) {
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
		},  func(k []byte, valueDecoder etl.Decoder, state etl.State, next etl.LoadNextFunc) error {
			var v []byte
			err:=valueDecoder.Decode(&v)
			if err!=nil {
				return err
			}
			vv:=new(big.Int).SetUint64(binary.BigEndian.Uint64(v))
			return next(k,vv.Bytes())
		}, etl.TransformArgs{Quit: quit, ExtractEndKey: endKey, ExtractStartKey: startKey})
		if err!=nil {
			t.Fatal(err)
		}
	})
	//loadTime 4m3.514260032s
	//loadTime 2m47.481593132s
	//--- PASS: TestGenerateTxLookup3_Experiment/2 (300.68s)
	//from 7 to 8 loadTime 17m46.684844364s 1610.34s
	t.Run("2", func(t *testing.T) {
		//startKey2:=dbutils.HeaderHashKey(7150000)
		err:=etl.Transform2(db,dbutils.HeaderPrefix,txLookupBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc2) error {
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

			//fmt.Println("eth/stagedsync/stage_txlookup_test.go:70", common.Bytes2Hex(k), common.Bytes2Hex(v))
			//return next(k,k,v)
			return nil
		},  func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
			if len(value)!=8 {
				fmt.Println(len(value))
			}
			vv:=new(big.Int).SetUint64(binary.BigEndian.Uint64(value))
			return next(k,vv.Bytes())
		}, etl.TransformArgs2{Quit: quit, ExtractStartKeys: [][]byte{ startKey}, ExtractEndKeys:[][]byte{endKey}})
		if err!=nil {
			t.Fatal(err)
		}
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
	endKey:=dbutils.EncodeTimestamp(8000000)
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
		err:=etl.Transform2(db,dbutils.PlainAccountChangeSetBucket,txLookupBucket, os.TempDir(), func(dbKey, dbValue []byte, next etl.ExtractNextFunc2) error {
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
		}, loadFunc2, etl.TransformArgs2{Quit: quit, ExtractStartKeys: [][]byte{ startKey}, ExtractEndKeys:[][]byte{endKey}})
		if err!=nil {
			t.Fatal(err)
		}
	})

	t.Run("check", func(t *testing.T) {
		txLookupBucket1:=append(dbutils.AccountsHistoryBucket, []byte("test2")...)
		txLookupBucket2:=append(dbutils.AccountsHistoryBucket, []byte("test3")...)
		db.Walk(txLookupBucket1,[]byte{}, 0, func(k, v []byte) (bool, error) {
			vv, err:=db.Get(txLookupBucket2, k)
			if err!=nil {
				log.Error("walk err", "err", err)
			}
			if !bytes.Equal(v, vv) {
				log.Warn("not eqal", "k", common.Bytes2Hex(k))
				vuint,_,err:=dbutils.WrapHistoryIndex(v).Decode()
				if err!=nil {
					t.Fatal(err)
				}
				log.Info("expected", "v", vuint)
				vuint,_,err=dbutils.WrapHistoryIndex(vv).Decode()
				if err!=nil {
					t.Fatal(err)
				}
				log.Info("exist", "v", vuint)
			}
			return true, nil
		})


	})

}

func loadFunc(k []byte, valueDecoder etl.Decoder, state etl.State, next etl.LoadNextFunc) error { //nolint
	blockNumber:=make([]byte, 9)
	err := valueDecoder.Decode(&blockNumber)
	if err != nil {
		fmt.Println(common.Bytes2Hex(k), blockNumber)
		return err
	}
	if len(blockNumber)>9 {
		log.Error("strange length", common.Bytes2Hex(k))
		return nil
	}
	blockNr:=binary.BigEndian.Uint64(blockNumber)
	vzero:=blockNumber[8]==1
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

	err = next(currentChunkKey, index)
	if err != nil {
		return err
	}

	return nil
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


func TestValidateTxLookups2(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	chaindata:="/media/b00ris/nvme/tgstaged/geth/chaindata"
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	validateTxLookups2(db, blockNum, interruptCh)
	log.Info("All done", "duration", time.Since(startTime))
}

func validateTxLookups2(db *ethdb.BoltDatabase, startBlock uint64, interruptCh chan bool) {
	txLookupBucket:=append(dbutils.TxLookupPrefix, []byte("test")...)

	blockNum := startBlock
	iterations := 0
	var interrupt bool
	// Validation Process
	blockBytes := big.NewInt(0)
	for !interrupt {
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
				check(err)
				panic(fmt.Sprintf("Validation process failed(%d). Expected %b, got %b", iterations, bn, val))
			}
		}
		blockNum++
	}
}














func GenerateTxLookups(chaindata string) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	var finished bool
	for !finished {
		blockNum, finished = generateLoop(db, blockNum, interruptCh)
	}
	log.Info("All done", "duration", time.Since(startTime))
}

func generateLoop(db ethdb.Database, startBlock uint64, interruptCh chan bool) (uint64, bool) {
	startTime := time.Now()
	var lookups []uint64
	var entry [8]byte
	var blockNum = startBlock
	var interrupt bool
	var finished = true
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for txIndex, tx := range body.Transactions {
			copy(entry[:2], tx.Hash().Bytes()[:2])
			binary.BigEndian.PutUint32(entry[2:6], uint32(blockNum))
			binary.BigEndian.PutUint16(entry[6:8], uint16(txIndex))
			lookups = append(lookups, binary.BigEndian.Uint64(entry[:]))
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum, "tx count", len(lookups))
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		if len(lookups) >= 100000000 {
			log.Info("Reached specified number of transactions")
			finished = false
			break
		}
	}
	log.Info("Processed", "blocks", blockNum, "tx count", len(lookups))
	log.Info("Filling up lookup array done", "duration", time.Since(startTime))
	startTime = time.Now()
	sort.Slice(lookups, func(i, j int) bool {
		return lookups[i] < lookups[j]
	})
	log.Info("Sorting lookup array done", "duration", time.Since(startTime))
	if len(lookups) == 0 {
		return blockNum, true
	}
	startTime = time.Now()
	var rangeStartIdx int
	var range2Bytes uint64
	for i, lookup := range lookups {
		// Find the range where lookups entries share the same first two bytes
		if i == 0 {
			rangeStartIdx = 0
			range2Bytes = lookup & 0xffff000000000000
			continue
		}
		twoBytes := lookup & 0xffff000000000000
		if range2Bytes != twoBytes {
			// Range finished
			fillSortRange(db, lookups, entry[:], rangeStartIdx, i)
			rangeStartIdx = i
			range2Bytes = twoBytes
		}
		if i%1000000 == 0 {
			log.Info("Processed", "transactions", i)
		}
	}
	fillSortRange(db, lookups, entry[:], rangeStartIdx, len(lookups))
	log.Info("Second roung of sorting done", "duration", time.Since(startTime))
	startTime = time.Now()
	batch := db.NewBatch()
	var n big.Int
	for i, lookup := range lookups {
		binary.BigEndian.PutUint64(entry[:], lookup)
		blockNumber := uint64(binary.BigEndian.Uint32(entry[2:6]))
		txIndex := int(binary.BigEndian.Uint16(entry[6:8]))
		blockHash := rawdb.ReadCanonicalHash(db, blockNumber)
		body := rawdb.ReadBody(db, blockHash, blockNumber)
		tx := body.Transactions[txIndex]
		n.SetInt64(int64(blockNumber))
		err := batch.Put(dbutils.TxLookupPrefix, tx.Hash().Bytes(), common.CopyBytes(n.Bytes()))
		check(err)
		if i != 0 && i%1000000 == 0 {
			_, err = batch.Commit()
			check(err)
			log.Info("Commited", "transactions", i)
		}
	}
	_, err := batch.Commit()
	check(err)
	log.Info("Commited", "transactions", len(lookups))
	log.Info("Tx committing done", "duration", time.Since(startTime))
	return blockNum, finished
}

func generateLoop1(db ethdb.Database, startBlock uint64, interruptCh chan bool) (uint64, bool) {
	f, _ := os.OpenFile(".lookups.tmp",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	entry := make([]byte, 36)
	var blockNum = startBlock
	var interrupt bool
	var finished = true
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for _, tx := range body.Transactions {
			copy(entry[:32], tx.Hash().Bytes())
			binary.BigEndian.PutUint32(entry[32:], uint32(blockNum))
			_, _ = f.Write(append(entry, '\n'))
		}
		blockNum++
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
	}
	return blockNum, finished
}

func fillSortRange(db rawdb.DatabaseReader, lookups []uint64, entry []byte, start, end int) {
	for j := start; j < end; j++ {
		binary.BigEndian.PutUint64(entry[:], lookups[j])
		blockNum := uint64(binary.BigEndian.Uint32(entry[2:6]))
		txIndex := int(binary.BigEndian.Uint16(entry[6:8]))
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		tx := body.Transactions[txIndex]
		copy(entry[:2], tx.Hash().Bytes()[2:4])
		lookups[j] = binary.BigEndian.Uint64(entry[:])
	}
	sort.Slice(lookups[start:end], func(i, j int) bool {
		return lookups[i] < lookups[j]
	})
}

func GenerateTxLookups1(chaindata string, block int) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	var interrupt bool
	var txcount int
	var n big.Int
	batch := db.NewBatch()
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for _, tx := range body.Transactions {
			txcount++
			n.SetInt64(int64(blockNum))
			err = batch.Put(dbutils.TxLookupPrefix, tx.Hash().Bytes(), common.CopyBytes(n.Bytes()))
			check(err)
			if txcount%100000 == 0 {
				_, err = batch.Commit()
				check(err)
			}
			if txcount%1000000 == 0 {
				log.Info("Commited", "transactions", txcount)
			}
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		if block != 1 && int(blockNum) > block {
			log.Info("Reached specified block count")
			break
		}
	}
	_, err = batch.Commit()
	check(err)
	log.Info("Commited", "transactions", txcount)
	log.Info("Processed", "blocks", blockNum)
	log.Info("Tx committing done", "duration", time.Since(startTime))
}

func GenerateTxLookups2(chaindata string) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	generateTxLookups2(db, blockNum, interruptCh)
	log.Info("All done", "duration", time.Since(startTime))
}

type LookupFile struct {
	reader io.Reader
	file   *os.File
	buffer []byte
	pos    uint64
}

type Entries []byte

type HeapElem struct {
	val   []byte
	index int
}

type Heap []HeapElem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	return bytes.Compare(h[i].val, h[j].val) < 0
}
func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapElem))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (a Entries) Len() int {
	return len(a) / 35
}

func (a Entries) Less(i, j int) bool {
	return bytes.Compare(a[35*i:35*i+35], a[35*j:35*j+35]) < 0
}

func (a Entries) Swap(i, j int) {
	tmp := common.CopyBytes(a[35*i : 35*i+35])
	copy(a[35*i:35*i+35], a[35*j:35*j+35])
	copy(a[35*j:35*j+35], tmp)
}

func insertInFileForLookups2(file *os.File, entries Entries, it uint64) {
	sorted := entries[:35*it]
	sort.Sort(sorted)
	_, err := file.Write(sorted)
	check(err)
	log.Info("File Insertion Occured")
}

func generateTxLookups2(db *ethdb.BoltDatabase, startBlock uint64, interruptCh chan bool) {
	var bufferLen int = 143360 // 35 * 4096
	var count uint64 = 5000000
	var entries Entries = make([]byte, count*35)
	bn := make([]byte, 3)
	var lookups []LookupFile
	var iterations uint64
	var blockNum = startBlock
	var interrupt bool
	filename := fmt.Sprintf(".lookups_%d.tmp", len(lookups))
	fileTmp, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)

		if body == nil {
			log.Info("Now Inserting to file")
			insertInFileForLookups2(fileTmp, entries, iterations)
			lookups = append(lookups, LookupFile{nil, nil, nil, 35})
			iterations = 0
			entries = nil
			fileTmp.Close()
			break
		}

		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		bn[0] = byte(blockNum >> 16)
		bn[1] = byte(blockNum >> 8)
		bn[2] = byte(blockNum)

		for _, tx := range body.Transactions {
			copy(entries[35*iterations:], tx.Hash().Bytes())
			copy(entries[35*iterations+32:], bn)
			iterations++
			if iterations == count {
				log.Info("Now Inserting to file")
				insertInFileForLookups2(fileTmp, entries, iterations)
				lookups = append(lookups, LookupFile{nil, nil, nil, 35})
				iterations = 0
				fileTmp.Close()
				filename = fmt.Sprintf(".lookups_%d.tmp", len(lookups))
				fileTmp, _ = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			}
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum, "iterations", iterations)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
	}
	batch := db.NewBatch()
	h := &Heap{}
	heap.Init(h)
	for i := range lookups {
		file, err := os.Open(fmt.Sprintf(".lookups_%d.tmp", i))
		check(err)
		lookups[i].file = file
		lookups[i].reader = bufio.NewReader(file)
		lookups[i].buffer = make([]byte, bufferLen)
		check(err)
		n, err := lookups[i].file.Read(lookups[i].buffer)
		if n != bufferLen {
			lookups[i].buffer = lookups[i].buffer[:n]
		}
		heap.Push(h, HeapElem{lookups[i].buffer[:35], i})
	}

	for !interrupt && len(*h) != 0 {
		val := (heap.Pop(h)).(HeapElem)
		if lookups[val.index].pos == uint64(bufferLen) {
			if val.val[32] != 0 {
				err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[32:]))
				check(err)
			} else {
				err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[33:]))
				check(err)
			}
			n, _ := lookups[val.index].reader.Read(lookups[val.index].buffer)
			iterations++
			if n == 0 {
				err := lookups[val.index].file.Close()
				check(err)
				os.Remove(fmt.Sprintf(".lookups_%d.tmp", val.index))
			} else {
				if n != bufferLen {
					lookups[val.index].buffer = lookups[val.index].buffer[:n]
				}
				lookups[val.index].pos = 35
				heap.Push(h, HeapElem{lookups[val.index].buffer[:35], val.index})
			}
			continue
		}

		heap.Push(h, HeapElem{lookups[val.index].buffer[lookups[val.index].pos : lookups[val.index].pos+35], val.index})
		lookups[val.index].pos += 35
		iterations++
		if val.val[32] != 0 {
			err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[32:]))
			check(err)
		} else {
			err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[33:]))
			check(err)
		}

		if iterations%1000000 == 0 {
			batch.Commit()
			log.Info("Commit Occured", "progress", iterations)
		}
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
	}
	batch.Commit()
	batch.Close()
}


func check(e error) {
	if e != nil {
		panic(e)
	}
}