package main

import (
	"bytes"
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
		for o := range out {
			if err := collector.Collect(o.versionHash[:], o.encodedAccount); err != nil {
				panic(err)
			}
			if err := collectorLookup.Collect(o.address[:], o.versionHash[:]); err != nil {
				panic(err)
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
				log.Info("[Pedersen Account Hashing] Current progress in Collection Phase", "address", "0x"+common.Bytes2Hex(k))
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

func regeneratePedersenStorage(outTx kv.RwTx, readTx kv.Tx, workersCount uint) error {
	logPrefix := PedersenHashedStorage
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

	jobs := make(chan *regeneratePedersenStorageJob, batchSize)
	out := make(chan *regeneratePedersenStorageJob, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workersCount); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			pedersenStorageWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			if err := collector.Collect(o.storageVerkleKey[:], o.storageValue); err != nil {
				panic(err)
			}
			if err := collectorLookup.Collect(append(o.address[:], o.storageKey.Bytes()...), o.storageVerkleKey[:]); err != nil {
				panic(err)
			}
		}
	}()

	var address common.Address
	var incarnation uint64
	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 60 {
			if !bytes.Equal(address[:], k[:20]) || binary.BigEndian.Uint64(k[20:28]) != incarnation {
				continue
			}
			jobs <- &regeneratePedersenStorageJob{
				storageKey:   new(uint256.Int).SetBytes(k[28:]),
				storageValue: v,
				address:      address,
			}
			select {
			case <-logInterval.C:
				log.Info("[Pedersen Storage Hashing] Current progress in Collection Phase", "address", "0x"+common.Bytes2Hex(k[:20]))
			default:
			}
		} else if len(k) == 20 {
			acc := accounts.NewAccount()
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}
			incarnation = acc.Incarnation
			address = common.BytesToAddress(k)
		}
	}

	close(jobs)
	wg.Wait()
	close(out)

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

func regeneratePedersenCode(outTx kv.RwTx, readTx kv.Tx, workersCount uint) error {
	logPrefix := PedersenHashedCode
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Code")

	collector := etl.NewCollector(PedersenHashedCode, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()

	collectorLookup := etl.NewCollector(PedersenHashedCodeLookup, "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	plainStateCursor, err := readTx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer plainStateCursor.Close()

	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()

	jobs := make(chan *regeneratePedersenCodeJob, batchSize)
	out := make(chan *regeneratePedersenCodeOut, batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(int(workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workersCount); i++ {
		go func(threadNo int) {
			defer debug.LogPanic()
			defer wg.Done()
			pedersenCodeWorker(ctx, logPrefix, jobs, out)
		}(i)
	}
	defer cancelWorkers()
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			// Write Code Size
			codeSizeBytes := make([]byte, 32)
			binary.LittleEndian.PutUint32(codeSizeBytes, uint32(o.codeSize))

			if err := collector.Collect(o.codeSizeHash[:], codeSizeBytes); err != nil {
				panic(err)
			}
			// Write code chunks
			if o.codeSize == 0 {
				continue
			}
			for i := range o.chunks {
				if err := collector.Collect(o.chunksKeys[i][:], o.chunks[i]); err != nil {
					panic(err)
				}
				// Build lookup [address + index]
				lookupKey := make([]byte, 24)
				copy(lookupKey, o.address[:])
				binary.BigEndian.PutUint32(lookupKey[20:], uint32(i))
				if err := collectorLookup.Collect(lookupKey, o.chunksKeys[i][:]); err != nil {
					panic(err)
				}
			}
		}
	}()

	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) != 20 {
			continue
		}
		versionKey, err := outTx.GetOne(PedersenHashedAccountsLookup, k)
		if err != nil {
			return err
		}
		codeSizeKey := make([]byte, 32)
		copy(codeSizeKey, versionKey)
		codeSizeKey[31] = vtree.CodeSizeLeafKey

		acc := accounts.NewAccount()
		acc.DecodeForStorage(v)

		if acc.IsEmptyCodeHash() {
			jobs <- &regeneratePedersenCodeJob{
				address:      common.BytesToAddress(k),
				codeSizeHash: common.BytesToHash(codeSizeKey),
				code:         nil,
			}
			continue
		}

		code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
		if err != nil {
			return err
		}

		jobs <- &regeneratePedersenCodeJob{
			address:      common.BytesToAddress(k),
			codeSizeHash: common.BytesToHash(codeSizeKey),
			code:         common.CopyBytes(code),
		}
		select {
		case <-logInterval.C:
			log.Info("[Pedersen Code Hashing] Current progress in Collection Phase", "address", "0x"+common.Bytes2Hex(k))
		default:
		}
	}

	close(jobs)
	wg.Wait()
	close(out)
	collector.Load(outTx, PedersenHashedCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	collectorLookup.Load(outTx, PedersenHashedCodeLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
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
	if err := regeneratePedersenStorage(outTx, readTx, workersCount); err != nil {
		return err
	}
	if err := regeneratePedersenCode(outTx, readTx, workersCount); err != nil {
		return err
	}
	return outTx.Commit()
}
