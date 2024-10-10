package verkletrie

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func IncrementAccount(vTx kv.RwTx, tx kv.Tx, workers uint64, verkleWriter *VerkleTreeWriter, from, to uint64, tmpdir string) error {
	logInterval := time.NewTicker(30 * time.Second)
	logPrefix := "IncrementVerkleAccount"

	jobs := make(chan *regenerateIncrementalPedersenAccountsJob, batchSize)
	out := make(chan *regenerateIncrementalPedersenAccountsOut, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(workers))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workers); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			incrementalAccountWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()

	accountCursor, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
	defer accountCursor.Close()

	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			if o.absentInState {
				if err := verkleWriter.DeleteAccount(o.versionHash, o.isContract); err != nil {
					panic(err)
				}
				continue
			}
			if err := verkleWriter.UpdateAccount(o.versionHash, o.codeSize, o.isContract, o.account); err != nil {
				panic(err)
			}
			if err := verkleWriter.WriteContractCodeChunks(o.codeKeys, o.codeChunks); err != nil {
				panic(err)
			}
		}
	}()
	marker := NewVerkleMarker(tmpdir)
	defer marker.Rollback()

	for k, v, err := accountCursor.Seek(hexutility.EncodeTs(from)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, addressBytes, _, err := historyv2.DecodeAccounts(k, v)
		if err != nil {
			return err
		}

		if blockNumber > to {
			break
		}
		address := libcommon.BytesToAddress(addressBytes)

		marked, err := marker.IsMarked(addressBytes)
		if err != nil {
			return err
		}

		if marked {
			continue
		}

		encodedAccount, err := tx.GetOne(kv.PlainState, addressBytes)
		if err != nil {
			return err
		}

		incarnationBytes, err := tx.GetOne(kv.IncarnationMap, addressBytes)
		if err != nil {
			return err
		}
		isContract := len(incarnationBytes) > 0 && binary.BigEndian.Uint64(incarnationBytes) != 0
		// Start
		if len(encodedAccount) == 0 {
			jobs <- &regenerateIncrementalPedersenAccountsJob{
				address:       address,
				isContract:    isContract,
				absentInState: true,
			}
		} else {
			var acc accounts.Account
			if err := acc.DecodeForStorage(encodedAccount); err != nil {
				return err
			}

			// We need to update code.
			code, err := tx.GetOne(kv.Code, acc.CodeHash[:])
			if err != nil {
				return err
			}

			jobs <- &regenerateIncrementalPedersenAccountsJob{
				address:       address,
				account:       acc,
				code:          code,
				absentInState: false,
				isContract:    isContract,
			}
		}
		if err := marker.MarkAsDone(addressBytes); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("Creating Verkle Trie Incrementally", "phase", "account", "blockNum", blockNumber)
		default:
		}
	}
	close(jobs)
	wg.Wait()
	close(out)
	return nil
}
