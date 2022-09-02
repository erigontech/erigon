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
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
	"github.com/ledgerwatch/log/v3"
)

/*func retrieveAccountKeys(address common.Address) (versionKey, balanceKey, codeSizeKey, codeHashKey, noncekey [32]byte) {
	// Process the polynomial
	versionkey := vtree.GetTreeKeyVersion(address[:])
	copy(balanceKey[:], versionkey)
	balanceKey[31] = vtree.BalanceLeafKey
	copy(noncekey[:], versionkey)
	noncekey[31] = vtree.NonceLeafKey
	copy(codeSizeKey[:], versionkey)
	codeSizeKey[31] = vtree.CodeSizeLeafKey
	copy(codeHashKey[:], versionkey)
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	return
}*/

func regeneratePedersenAccounts(outTx kv.RwTx, readTx kv.Tx, workersCount uint) error {
	logPrefix := PedersenHashedAccounts
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Accounts")

	collector := etl.NewCollector(PedersenHashedAccounts, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()

	collectorLookup := etl.NewCollector(PedersenHashedAccountsLookup, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	plainStateCursor, err := readTx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer plainStateCursor.Close()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	jobs := make(chan *regeneratePedersenAccountsJob, batchSize)
	out := make(chan *regeneratePedersenAccountsOut, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workersCount); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			pedersenAccountWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		var ok bool
		var o *regeneratePedersenAccountsOut
		for {
			select {
			case o, ok = <-out:
				if !ok {
					return
				}

				if err := collector.Collect(o.versionHash[:], o.encodedAccount); err != nil {
					panic(err)
					return
				}
				if err := collectorLookup.Collect(o.address[:], o.versionHash[:]); err != nil {
					panic(err)
					return
				}
			}
		}
	}()
	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 20 {
			var acc accounts.Account
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}
			jobs <- &regeneratePedersenAccountsJob{
				address: common.BytesToAddress(k),
				account: acc,
			}
			select {
			case <-logEvery.C:
				log.Info("[Pedersen Account Hashing] Current progress in Collection Phase", "key", common.Bytes2Hex(k))
			default:
			}
		}
	}

	close(jobs)
	wg.Wait()
	close(out)

	collector.Load(outTx, PedersenHashedAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	collectorLookup.Load(outTx, PedersenHashedAccountsLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	log.Info("Finished generation of Pedersen Hashed Accounts", "elapsed", time.Since(start))

	return nil
}

func regeneratePedersenStorage(outTx kv.RwTx, readTx kv.Tx) error {
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Storage")

	collector := etl.NewCollector(PedersenHashedStorage, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()

	collectorLookup := etl.NewCollector(PedersenHashedStorageLookup, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	plainStateCursor, err := readTx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer plainStateCursor.Close()

	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()

	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 60 {
			storageKey := vtree.GetTreeKeyStorageSlot(k[:20], new(uint256.Int).SetBytes(k[:28]))
			if err := collector.Collect(storageKey[:], v); err != nil {
				return err
			}
			if err := collectorLookup.Collect(k, storageKey[:]); err != nil {
				return err
			}
			select {
			case <-logInterval.C:
				log.Info("[Pedersen Storage Hashing] Current progress in Collection Phase", "key", common.Bytes2Hex(k))
			default:
			}
		}
	}
	collector.Load(outTx, PedersenHashedStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	collectorLookup.Load(outTx, PedersenHashedStorageLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	log.Info("Finished generation of Pedersen Hashed Storage", "elapsed", time.Since(start))

	return nil
}

func regeneratePedersenCode(outTx kv.RwTx, readTx kv.Tx) error {
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Code")

	collector := etl.NewCollector(PedersenHashedCode, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()

	plainStateCursor, err := readTx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer plainStateCursor.Close()

	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()

	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 20 {
			versionKey, err := outTx.GetOne(PedersenHashedAccountsLookup, k)
			if err != nil {
				return err
			}
			codeSizeKey := make([]byte, 32)
			copy(codeSizeKey[:], versionKey)
			codeSizeKey[31] = vtree.CodeSizeLeafKey

			// Retrieve codeHash
			acc := accounts.NewAccount()
			acc.DecodeForStorage(v)
			codeSizeBytes := make([]byte, 32)
			if !acc.IsEmptyCodeHash() {
				code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
				if err != nil {
					return err
				}
				// Chunkify contract code and build keys for each chunks and insert them in the tree
				chunkedCode, err := vtree.ChunkifyCode(code)
				if err != nil {
					return err
				}
				// Write code chunks
				for i := 0; i < len(chunkedCode); i += 32 {
					collector.Collect(vtree.GetTreeKeyCodeChunk(k, uint256.NewInt(uint64(i)/32)), chunkedCode[i:i+32])
				}

				// Set code size
				binary.LittleEndian.PutUint64(codeSizeBytes, uint64(len(code)))

			}
			collector.Collect(codeSizeKey, codeSizeBytes)
		}
	}
	collector.Load(outTx, PedersenHashedCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	log.Info("Finished generation of Pedersen Hashed Code", "elapsed", time.Since(start))

	return nil
}

func RegeneratePedersenHashstate(outTx kv.RwTx, readTx kv.Tx, workersCount uint) error {
	for _, b := range ExtraBuckets {
		if err := outTx.CreateBucket(b); err != nil {
			return err
		}
	}
	if err := regeneratePedersenAccounts(outTx, readTx, workersCount); err != nil {
		return err
	}
	if err := regeneratePedersenStorage(outTx, readTx); err != nil {
		return err
	}
	if err := regeneratePedersenCode(outTx, readTx); err != nil {
		return err
	}
	return outTx.Commit()
}
