package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/log/v3"
)

func badKeysForAddress(tx kv.RwTx, address common.Address) ([][]byte, error) {
	var badKeys [][]byte
	// Delete also code and storage slots that are connected to that account (iterating over lookups is simpe)
	storageLookupCursor, err := tx.Cursor(PedersenHashedStorageLookup)
	if err != nil {
		return nil, err
	}
	defer storageLookupCursor.Close()

	codeLookupCursor, err := tx.Cursor(PedersenHashedCodeLookup)
	if err != nil {
		return nil, err
	}
	defer codeLookupCursor.Close()

	for k, treeKey, err := storageLookupCursor.Seek(address[:]); len(k) >= 20 && bytes.Equal(k[:20], address[:]); k, treeKey, err = storageLookupCursor.Next() {
		if err != nil {
			return nil, err
		}
		badKeys = append(badKeys, common.CopyBytes(treeKey))
	}

	for k, treeKey, err := codeLookupCursor.Seek(address[:]); len(k) >= 20 && bytes.Equal(k[:20], address[:]); k, treeKey, err = codeLookupCursor.Next() {
		if err != nil {
			return nil, err
		}
		badKeys = append(badKeys, common.CopyBytes(treeKey))
	}
	return badKeys, nil
}

func incrementAccount(vTx kv.RwTx, tx kv.Tx, cfg optionsCfg, from, to uint64) error {
	logInterval := time.NewTicker(30 * time.Second)
	logPrefix := "IncrementVerkleAccount"

	collectorLookup := etl.NewCollector(PedersenHashedCodeLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

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
	verkleWriter := NewVerkleTreeWriter(vTx, cfg.tmpdir)
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
			if err := verkleWriter.UpdateAccount(o.versionHash, o.codeSize, o.account); err != nil {
				panic(err)
			}
			if err := verkleWriter.WriteContractCodeChunks(o.codeKeys, o.codeChunks); err != nil {
				panic(err)
			}
			// Build lookup [address + index]
			for i := range o.codeChunks {
				lookupKey := make([]byte, 24)
				copy(lookupKey, o.address[:])
				binary.BigEndian.PutUint32(lookupKey[20:], uint32(i))
				if err := collectorLookup.Collect(lookupKey, o.codeKeys[i]); err != nil {
					panic(err)
				}
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
			badKeys, err := badKeysForAddress(vTx, address)
			if err != nil {
				return err
			}
			jobs <- &regenerateIncrementalPedersenAccountsJob{
				absentInState: true,
				badKeys:       badKeys,
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
				badKeys, err = badKeysForAddress(vTx, address)
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
	if err := collectorLookup.Load(vTx, PedersenHashedCodeLookup, identityFuncForVerkleTree, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}}); err != nil {
		return err
	}
	// Get root
	root, err := ReadVerkleRoot(tx, from)
	if err != nil {
		return err
	}
	_, err = verkleWriter.CommitVerkleTree(root)
	return err
}
