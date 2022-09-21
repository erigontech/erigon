package main

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/log/v3"
)

func incrementAccount(vTx kv.RwTx, tx kv.Tx, cfg optionsCfg, verkleWriter *VerkleTreeWriter, from, to uint64) error {
	logInterval := time.NewTicker(30 * time.Second)
	logPrefix := "IncrementVerkleAccount"

	jobs := make(chan *regenerateIncrementalPedersenAccountsJob, batchSize)
	out := make(chan *regenerateIncrementalPedersenAccountsOut, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(cfg.workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(cfg.workersCount); i++ {
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
			// Remove all bad keys
			for _, badKey := range o.badKeys {
				if err := verkleWriter.Insert(badKey, nil); err != nil {
					panic(err)
				}
			}
			if o.absentInState {
				continue
			}
			if err := verkleWriter.UpdateAccount(o.versionHash, o.codeSize, true, o.account); err != nil {
				panic(err)
			}
			if err := verkleWriter.WriteContractCodeChunks(o.codeKeys, o.codeChunks); err != nil {
				panic(err)
			}
		}
	}()
	marker := NewVerkleMarker()
	defer marker.Rollback()

	accountProcessed := 0
	for k, v, err := accountCursor.Seek(dbutils.EncodeBlockNumber(from)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, addressBytes, _, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return err
		}

		if blockNumber > to {
			break
		}
		address := common.BytesToAddress(addressBytes)

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
		// Start
		if len(encodedAccount) == 0 {
			jobs <- &regenerateIncrementalPedersenAccountsJob{
				absentInState: true,
			}
		} else {
			var acc accounts.Account
			if err := acc.DecodeForStorage(encodedAccount); err != nil {
				return err
			}
			verkleIncarnation, err := ReadVerkleIncarnation(vTx, address)
			if err != nil {
				return err
			}
			var code []byte
			var badKeys [][]byte
			if verkleIncarnation != acc.Incarnation {
				// We need to update code.
				code, err = tx.GetOne(kv.Code, acc.CodeHash[:])
				if err != nil {
					return err
				}
			}
			jobs <- &regenerateIncrementalPedersenAccountsJob{
				address:       address,
				account:       acc,
				code:          code,
				absentInState: false,
				badKeys:       badKeys,
			}
		}
		if err := marker.MarkAsDone(addressBytes); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("Creating Verkle Trie Incrementally", "phase", "account", "blockNum", blockNumber, "accountsProcessed", accountProcessed)
		default:
		}
	}
	close(jobs)
	wg.Wait()
	close(out)
	return nil
}
