package verkletrie

import (
	"context"
	"sync"
	"time"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/log/v3"
)

func IncrementStorage(vTx kv.RwTx, tx kv.Tx, workers uint64, verkleWriter *VerkleTreeWriter, from, to uint64) (common.Hash, error) {
	logInterval := time.NewTicker(30 * time.Second)
	logPrefix := "IncrementVerkleStorage"

	jobs := make(chan *regeneratePedersenStorageJob, batchSize)
	out := make(chan *regeneratePedersenStorageJob, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(workers))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workers); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			pedersenStorageWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()

	storageCursor, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return common.Hash{}, err
	}
	defer storageCursor.Close()
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			if err := verkleWriter.Insert(o.storageVerkleKey[:], o.storageValue); err != nil {
				panic(err)
			}
		}
	}()
	marker := NewVerkleMarker()
	defer marker.Rollback()

	for k, v, err := storageCursor.Seek(common2.EncodeTs(from)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return common.Hash{}, err
		}
		blockNumber, changesetKey, _, err := historyv2.DecodeStorage(k, v)
		if err != nil {
			return common.Hash{}, err
		}

		if blockNumber > to {
			break
		}

		marked, err := marker.IsMarked(changesetKey)
		if err != nil {
			return common.Hash{}, err
		}

		if marked {
			continue
		}

		address := common.BytesToAddress(changesetKey[:20])

		/*var acc accounts.Account
		_, err := rawdb.ReadAccount(tx, address, &acc)
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
		}*/

		storageValue, err := tx.GetOne(kv.PlainState, changesetKey)
		if err != nil {
			return common.Hash{}, err
		}
		storageKey := new(uint256.Int).SetBytes(changesetKey[28:])
		var storageValueFormatted []byte

		if len(storageValue) > 0 {
			storageValueFormatted = make([]byte, 32)
			int256ToVerkleFormat(new(uint256.Int).SetBytes(storageValue), storageValueFormatted)
		}

		jobs <- &regeneratePedersenStorageJob{
			address:      address,
			storageKey:   storageKey,
			storageValue: storageValueFormatted,
		}
		if err := marker.MarkAsDone(changesetKey); err != nil {
			return common.Hash{}, err
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
	// Get root
	root, err := rawdb.ReadVerkleRoot(vTx, from)
	if err != nil {
		return common.Hash{}, err
	}
	newRoot, err := verkleWriter.CommitVerkleTree(root)
	if err != nil {
		return common.Hash{}, err
	}
	log.Info("Computed verkle root", "root", common.Bytes2Hex(newRoot[:]))

	return newRoot, rawdb.WriteVerkleRoot(vTx, to, newRoot)
}
