package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

// This executor indexes contract creation.
//
// It produces as result the bucket OtsDeployments with:
//
//   - key: blockNum (uint64)
//   - value: address ([20]byte) + incarnation (uint64)
//
// This bucket serves as a starting point to all contract classifiers.
//
// It follows 2 different strategies, the first one "firstSyncStrategy" is optimized
// for traversing the entire DB and create the entire bucket from existing data. But it works
// only the first time.
//
// The next runs use the "continuousStrategy", which for each block traverses the account changeset
// to detect new contract deployments.
func ContractIndexerExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	if startBlock == 0 && isInternalTx {
		return firstSyncContractExecutor(tx, tmpDir, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, ctx, s, logger)
	}
	return incrementalContractIndexer(tx, tmpDir, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, ctx, s, logger)
}

// This strategy must be run ONLY the first time the contract indexer is run. That's because
// this stage traverses PlainContractCode bucket and indexes all existing addresses that contain
// a deployed contract.
//
// That works only the first time, because it is not unwind friendly, however it's more efficient
// than the general strategy of traversing account change sets.
func firstSyncContractExecutor(tx kv.RwTx, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, logger log.Logger) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using concurrent executor", s.LogPrefix()))
	}

	plainCC, err := tx.Cursor(kv.PlainContractCode)
	if err != nil {
		return startBlock, err
	}
	defer plainCC.Close()

	accHistory, err := tx.Cursor(kv.E2AccountsHistory)
	if err != nil {
		return startBlock, err
	}
	defer accHistory.Close()

	contractCount := 0
	reader := state.NewPlainState(tx, startBlock, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	contractCollector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer contractCollector.Close()

	// Iterate through all deployed contracts; identify the block when it was deployed and
	// trace it.
	//
	// We are only interested in the key set (address+incarnation) here.
	stopped := false
	for k, _, err := plainCC.First(); k != nil && !stopped; k, _, err = plainCC.Next() {
		if err != nil {
			return startBlock, err
		}

		addr := libcommon.BytesToAddress(k[:length.Addr])
		incarnation := binary.BigEndian.Uint64(k[length.Addr:])

		blockFound, err := locateBlock(accHistory, addr, incarnation, stopped, bm, reader)
		if err != nil {
			return startBlock, err
		}
		// TODO: review; use blockFound == 0 to signal skip
		if blockFound == 0 {
			continue
		}

		contractCount++

		// Insert contract deployment entry
		newK, newV := contractMatchKeyPair(blockFound, addr, incarnation)
		if err := contractCollector.Collect(newK, newV); err != nil {
			return startBlock, err
		}

		select {
		default:
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s]", s.LogPrefix()), "addr", addr, "incarnation", incarnation, "contractCount", contractCount)
		}
	}

	if stopped {
		return startBlock, libcommon.ErrStopped
	}

	if err := contractCollector.Load(tx, kv.OtsAllContracts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return startBlock, err
	}
	log.Info(fmt.Sprintf("[%s] Contract count", s.LogPrefix()), "count", contractCount)

	if err := fillCounterBucket(tx, startBlock, endBlock, kv.OtsAllContracts, kv.OtsAllContractsCounter, logEvery, ctx, s); err != nil {
		return startBlock, err
	}
	return endBlock, nil
}

func locateBlock(accHistory kv.Cursor, addr libcommon.Address, incarnation uint64, stopped bool, bm *roaring64.Bitmap, reader *state.PlainState) (uint64, error) {
	kh, vh, err := accHistory.Seek(addr.Bytes())
	if err != nil {
		return 0, err
	}
	if !bytes.HasPrefix(kh, addr.Bytes()) {
		return 0, nil
		// TODO: fix contracts appearing on PlainContractCode, but not in history
		// e.g.: k=0x026950001a85dd75b6a5f17b94c35a2feaeb2421ffffffffffffffff addr=0x02694FebD8d726465976BEE5cE0a9Ef5f72b577f
		// return startBlock, nil
	}

	// Given address/incarnation, locate in which block it was deployed
	blockFound := uint64(0)
	for !stopped && blockFound == 0 {
		if _, err := bm.ReadFrom(bytes.NewReader(vh)); err != nil {
			return 0, err
		}

		// default case
		reader.SetBlockNr(bm.Maximum() + 1)
		acc, err := reader.ReadAccountData(addr)
		if err != nil {
			return 0, err
		}

		if acc == nil || acc.Incarnation == 0 {
			// maybe the contract/incarnation isn't deployed yet, but it may have been
			// selfdestroyed, can't say for sure, so linear search the entire shard
			blockFound, err = findBlockInsideShard(reader, bm, addr, incarnation)
			if err != nil {
				return 0, err
			}
			if blockFound != 0 {
				break
			}
		} else if acc.Incarnation >= incarnation {
			// found shard
			break
		}

		kh, vh, err = accHistory.Next()
		if err != nil {
			return 0, err
		}
		if !bytes.HasPrefix(kh, addr.Bytes()) {
			return 0, nil
		}
	}

	// Locate block inside shard
	if blockFound == 0 {
		blockFound, err = findBlockInsideShard(reader, bm, addr, incarnation)
		if err != nil {
			return 0, err
		}
	}

	return blockFound, nil
}

// Encode data into the expected standard format for contract match
// tables.
//
// blockNum uint64 -> address [20]byte + [incarnation uint64 (ONLY if != 1)]
func contractMatchKeyPair(blockNum uint64, addr libcommon.Address, incarnation uint64) ([]byte, []byte) {
	k := hexutility.EncodeTs(blockNum)

	var v []byte
	if incarnation != 1 {
		v = make([]byte, length.Addr+common.IncarnationLength)
		copy(v, addr.Bytes())
		binary.BigEndian.PutUint64(v[length.Addr:], incarnation)
	} else {
		v = make([]byte, length.Addr)
		copy(v, addr.Bytes())
	}

	return k, v
}

// Given a shard of accounts history, locate which block the incarnation was deployed
// by linear searching all touched blocks and comparing the state before/after each one.
//
// TODO: return DB inconsistency error if can't find block inside shard
func findBlockInsideShard(reader *state.PlainState, bm *roaring64.Bitmap, addr libcommon.Address, incarnation uint64) (uint64, error) {
	it := bm.Iterator()
	for it.HasNext() {
		blockNum := it.Next()
		reader.SetBlockNr(blockNum + 1)
		acc, err := reader.ReadAccountData(addr)
		if err != nil {
			return 0, err
		}
		if acc != nil && acc.Incarnation >= incarnation {
			return blockNum, nil
		}
	}

	return 0, nil
}

func incrementalContractIndexer(tx kv.RwTx, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, logger log.Logger) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using incremental executor", s.LogPrefix()))
	}

	acs, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return startBlock, err
	}
	defer acs.Close()

	newContractCount := 0
	reader := state.NewPlainState(tx, startBlock, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
	currentBlockNumber := startBlock
	for ; currentBlockNumber <= endBlock; currentBlockNumber++ {
		key := hexutility.EncodeTs(currentBlockNumber)
		k, v, err := acs.Seek(key)
		if err != nil {
			return startBlock, err
		}

		// Iterate through block changeset, identify those entries whose codehash is empty;
		// empty codehash == previous state is EOA, next state might be a deployed contract
		for ; bytes.Equal(k, key); k, v, err = acs.NextDup() {
			if err != nil {
				return startBlock, err
			}

			addr := libcommon.BytesToAddress(v[:length.Addr])

			reader.SetBlockNr(currentBlockNumber)
			prevBlockAcc, err := reader.ReadAccountData(addr)
			if err != nil {
				return startBlock, err
			}
			// acc == nil: state didn't exist yet; empty codehash: no contract yet
			if prevBlockAcc != nil && !prevBlockAcc.IsEmptyCodeHash() {
				// contract already deployed at this address; ignore
				continue
			}

			reader.SetBlockNr(currentBlockNumber + 1)
			currBlockAcc, err := reader.ReadAccountData(addr)
			if err != nil {
				return startBlock, err
			}
			// acc == nil: was selfdestructed here; !empty codehash: was deployed here
			if currBlockAcc != nil && !currBlockAcc.IsEmptyCodeHash() {
				// detected contract deployment at this address
				newK, newV := contractMatchKeyPair(currentBlockNumber, addr, currBlockAcc.Incarnation)
				if err := tx.Put(kv.OtsAllContracts, newK, newV); err != nil {
					return startBlock, err
				}

				newContractCount++
			}
		}

		select {
		default:
		case <-ctx.Done():
			return startBlock, libcommon.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s]", s.LogPrefix()), "currentBlock", currentBlockNumber, "contractCount", newContractCount)
		}
	}

	if newContractCount > 0 {
		log.Info(fmt.Sprintf("[%s] Found new contracts", s.LogPrefix()), "count", newContractCount)
	}

	if err := fillCounterBucket(tx, startBlock, endBlock, kv.OtsAllContracts, kv.OtsAllContractsCounter, logEvery, ctx, s); err != nil {
		return startBlock, err
	}
	return endBlock, nil
}

func fillCounterBucket(tx kv.RwTx, startBlock, endBlock uint64, bucket string, counterBucket string, logEvery *time.Ticker, ctx context.Context, s *StageState) error {
	source, err := tx.CursorDupSort(bucket)
	if err != nil {
		return err
	}
	defer source.Close()

	counter, err := tx.Cursor(counterBucket)
	if err != nil {
		return err
	}
	defer counter.Close()

	// Determine current total
	lastCounter, _, err := counter.Last()
	if err != nil {
		return err
	}
	currentCount := uint64(0)
	if lastCounter != nil {
		currentCount = binary.BigEndian.Uint64(lastCounter)
	}

	// Traverse [startBlock, endBlock]
	blockKey := hexutility.EncodeTs(startBlock)
	k, _, err := source.Seek(blockKey)
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	currentBlock := binary.BigEndian.Uint64(k)

	for currentBlock <= endBlock {
		// Accumulate found contracts
		n, err := source.CountDuplicates()
		if err != nil {
			return err
		}

		// Add count of contracts AFTER the block
		// k: cummulative count before block
		// v: block
		currentCount += n
		counterKey := hexutility.EncodeTs(currentCount)
		if err := tx.Put(counterBucket, counterKey, k); err != nil {
			return err
		}

		select {
		default:
		case <-ctx.Done():
			return libcommon.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Updating counters", s.LogPrefix()), "currentBlock", currentBlock, "count", currentCount)
		}

		// Next block
		k, _, err = source.NextNoDup()
		if err != nil {
			return err
		}
		if k == nil {
			break
		}
		currentBlock = binary.BigEndian.Uint64(k)
	}

	return nil
}
