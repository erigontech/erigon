package main

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func incrementStorage(vTx kv.RwTx, tx kv.Tx, cfg optionsCfg, to uint64) error {
	logInterval := time.NewTicker(30 * time.Second)
	// START
	var progress uint64
	var err error
	if progress, err = stages.GetStageProgress(vTx, stages.VerkleTrie); err != nil {
		return err
	}
	logPrefix := "IncrementVerkleStorage"

	collectorLookup := etl.NewCollector(PedersenHashedStorageLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	jobs := make(chan *regeneratePedersenStorageJob, batchSize)
	out := make(chan *regeneratePedersenStorageJob, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(cfg.workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(cfg.workersCount); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			pedersenStorageWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()

	storageCursor, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return err
	}
	defer storageCursor.Close()
	verkleWriter := NewVerkleTreeWriter(vTx, cfg.tmpdir)
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			if err := verkleWriter.Insert(o.storageVerkleKey[:], o.storageValue); err != nil {
				panic(err)
			}

			if err := collectorLookup.Collect(append(o.address[:], o.storageKey.Bytes()...), o.storageVerkleKey[:]); err != nil {
				panic(err)
			}
		}
	}()
	marker := NewVerkleMarker()
	defer marker.Close()
	for k, v, err := storageCursor.Seek(dbutils.EncodeBlockNumber(progress)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, changesetKey, _, err := changeset.DecodeStorage(k, v)
		if err != nil {
			return err
		}

		if blockNumber > to {
			break
		}

		marked, err := marker.IsMarked(changesetKey)
		if err != nil {
			return err
		}

		if marked {
			continue
		}

		address := common.BytesToAddress(changesetKey[:20])

		var acc accounts.Account
		has, err := rawdb.ReadAccount(tx, address, &acc)
		if err != nil {
			return err
		}

		storageIncarnation := binary.BigEndian.Uint64(changesetKey[20:28])
		// Storage and code deletion is handled due to self-destruct is handled in accounts
		if !has {
			if err := marker.MarkAsDone(changesetKey); err != nil {
				return err
			}
			continue
		}

		if acc.Incarnation != storageIncarnation {
			continue
		}

		storageValue, err := tx.GetOne(kv.PlainState, changesetKey)
		if err != nil {
			return err
		}
		storageKey := new(uint256.Int).SetBytes(changesetKey[28:])
		var storageValueFormatted []byte

		if len(storageValue) > 0 {
			storageValueFormatted32 := new(uint256.Int).SetBytes(storageValue).Bytes32()
			storageValueFormatted = storageValueFormatted32[:]
		}

		jobs <- &regeneratePedersenStorageJob{
			address:      address,
			storageKey:   storageKey,
			storageValue: storageValueFormatted[:],
		}
		if err := marker.MarkAsDone(changesetKey); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("Creating Verkle Trie Incrementally", "phase", "storage", "blockNum", blockNumber)
		default:
		}
	}
	close(jobs)
	wg.Wait()
	close(out)
	if err := collectorLookup.Load(vTx, PedersenHashedStorageLookup, identityFuncForVerkleTree, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}}); err != nil {
		return err
	}
	// Get root
	/*h := rawdb.ReadHeaderByNumber(tx, progress)
	return verkleWriter.CommitVerkleTree(h.Root)*/
	return verkleWriter.CommitVerkleTree(common.Hash{})
}
