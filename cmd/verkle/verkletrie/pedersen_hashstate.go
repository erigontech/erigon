package verkletrie

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func RegeneratePedersenAccounts(outTx kv.RwTx, readTx kv.Tx, workers uint64, verkleWriter *VerkleTreeWriter) error {
	logPrefix := "PedersenHashedAccounts"
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Accounts")

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
	wg.Add(int(workers))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workers); i++ {
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
			if err := verkleWriter.UpdateAccount(o.versionHash[:], o.codeSize, true, o.account); err != nil {
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
				address:  libcommon.BytesToAddress(k),
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

	log.Info("Finished generation of Pedersen Hashed Accounts", "elapsed", time.Since(start))

	return nil
}

func RegeneratePedersenStorage(outTx kv.RwTx, readTx kv.Tx, workers uint64, verkleWriter *VerkleTreeWriter) error {
	logPrefix := "PedersenHashedStorage"
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Storage")

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

	var address libcommon.Address
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
			address = libcommon.BytesToAddress(k)
		}
	}

	close(jobs)
	wg.Wait()
	close(out)

	log.Info("Finished generation of Pedersen Hashed Storage", "elapsed", time.Since(start))

	return nil
}

func RegeneratePedersenCode(outTx kv.RwTx, readTx kv.Tx, workers uint64, verkleWriter *VerkleTreeWriter) error {
	logPrefix := "PedersenHashedCode"
	start := time.Now()
	log.Info("Started Generation of Pedersen Hashed Code")

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
	wg.Add(int(workers))
	ctx, cancelWorkers := context.WithCancel(context.Background())
	for i := 0; i < int(workers); i++ {
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
			if err := verkleWriter.WriteContractCodeChunks(o.chunksKeys, o.chunks); err != nil {
				panic(err)
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
			address: libcommon.BytesToAddress(k),
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

	log.Info("Finished generation of Pedersen Hashed Code", "elapsed", time.Since(start))

	return nil
}
