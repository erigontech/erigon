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
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
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

func regeneratePedersenAccounts(outTx kv.RwTx, readTx kv.Tx, cfg optionsCfg, collector *etl.Collector) error {
	logPrefix := PedersenHashedAccounts
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Accounts")

	collectorLookup := etl.NewCollector(PedersenHashedAccountsLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
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
	wg.Add(int(cfg.workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(cfg.workersCount); i++ {
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
			acc := o.account
			versionKey := o.versionHash
			var codeHashKey, nonceKey, balanceKey, codeSizeKey, nonce, balance, codeSize [32]byte
			copy(codeHashKey[:], versionKey[:31])
			copy(nonceKey[:], versionKey[:31])
			copy(balanceKey[:], versionKey[:31])
			copy(codeSizeKey[:], versionKey[:31])
			codeHashKey[31] = vtree.CodeKeccakLeafKey
			nonceKey[31] = vtree.NonceLeafKey
			balanceKey[31] = vtree.BalanceLeafKey
			codeSizeKey[31] = vtree.CodeSizeLeafKey
			// Compute balance value
			bbytes := acc.Balance.ToBig().Bytes()
			if len(bbytes) > 0 {
				for i, b := range bbytes {
					balance[len(bbytes)-i-1] = b
				}
			}
			// compute nonce value
			binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
			// Compute code size value
			binary.LittleEndian.PutUint64(codeSize[:], o.codeSize)

			if cfg.disabledLookups {
				continue
			}

			// Collect all data
			if err := collector.Collect(versionKey[:], []byte{0}); err != nil {
				panic(err)
			}

			if err := collector.Collect(nonceKey[:], nonce[:]); err != nil {
				panic(err)
			}
			if err := collector.Collect(balanceKey[:], balance[:]); err != nil {
				panic(err)
			}
			if err := collector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
				panic(err)
			}
			if err := collector.Collect(codeSizeKey[:], codeSize[:]); err != nil {
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
			codeSize := uint64(0)
			if !acc.IsEmptyCodeHash() {
				code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
				if err != nil {
					return err
				}
				codeSize = uint64(len(code))
			}
			jobs <- &regeneratePedersenAccountsJob{
				address:  common.BytesToAddress(k),
				account:  acc,
				codeSize: codeSize,
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

	collectorLookup.Load(outTx, PedersenHashedAccountsLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	log.Info("Finished generation of Pedersen Hashed Accounts", "elapsed", time.Since(start))

	return nil
}

func regeneratePedersenStorage(outTx kv.RwTx, readTx kv.Tx, cfg optionsCfg, collector *etl.Collector) error {
	logPrefix := PedersenHashedStorage
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Storage")

	collectorLookup := etl.NewCollector(PedersenHashedStorageLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
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
	// Start Goroutine for collection
	go func() {
		defer debug.LogPanic()
		defer cancelWorkers()
		for o := range out {
			if err := collector.Collect(o.storageVerkleKey[:], o.storageValue); err != nil {
				panic(err)
			}
			if cfg.disabledLookups {
				continue
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
			storageValue := new(uint256.Int).SetBytes(v).Bytes32()
			jobs <- &regeneratePedersenStorageJob{
				storageKey:   new(uint256.Int).SetBytes(k[28:]),
				storageValue: storageValue[:],
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

	collectorLookup.Load(outTx, PedersenHashedStorageLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
	log.Info("Finished generation of Pedersen Hashed Storage", "elapsed", time.Since(start))

	return nil
}

func regeneratePedersenCode(outTx kv.RwTx, readTx kv.Tx, cfg optionsCfg, collector *etl.Collector) error {
	logPrefix := PedersenHashedCode
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Code")

	collectorLookup := etl.NewCollector(PedersenHashedCodeLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
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
	wg.Add(int(cfg.workersCount))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(cfg.workersCount); i++ {
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
			// Write code chunks
			if o.codeSize == 0 {
				continue
			}
			for i := range o.chunks {
				if err := collector.Collect(o.chunksKeys[i][:], o.chunks[i]); err != nil {
					panic(err)
				}
				if cfg.disabledLookups {
					continue
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

		acc := accounts.NewAccount()
		acc.DecodeForStorage(v)

		if acc.IsEmptyCodeHash() {
			continue
		}

		code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
		if err != nil {
			return err
		}

		jobs <- &regeneratePedersenCodeJob{
			address: common.BytesToAddress(k),
			code:    common.CopyBytes(code),
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

	if err := collectorLookup.Load(outTx, PedersenHashedCodeLookup, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}}); err != nil {
		return err
	}
	log.Info("Finished generation of Pedersen Hashed Code", "elapsed", time.Since(start))

	return nil
}

func RegeneratePedersenHashstate(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.stateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.stateDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return err
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := initDB(vTx); err != nil {
		return err
	}

	collector := etl.NewCollector("VerkleTrie", cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize*8))
	defer collector.Close()
	if err := regeneratePedersenAccounts(vTx, tx, cfg, collector); err != nil {
		return err
	}
	if err := regeneratePedersenCode(vTx, tx, cfg, collector); err != nil {
		return err
	}

	if err := regeneratePedersenStorage(vTx, tx, cfg, collector); err != nil {
		return err
	}
	return vTx.Commit()
}
