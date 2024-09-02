package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

// This is a dual strategy StageExecutor which indexes deployed contracts based on a criteria determined
// by a Prober.
//
// It also works as a filter, taking contracts from sourceBucket -> Prober -> write on targetBucket
// + counterBucket.
//
// During the first sync, it runs the indexer concurrently. After that, during the following syncs,
// it runs it single-threadly, incrementaly.
func NewConcurrentIndexerExecutor(proberFactory ProberFactory, sourceBucket, targetBucket, counterBucket string) StageExecutor {
	return func(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
		if startBlock == 0 && isInternalTx {
			return runExecutorConcurrently(ctx, db, tx, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, s, proberFactory, sourceBucket, targetBucket, counterBucket)
		}
		return runExecutorIncrementally(ctx, tx, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, s, proberFactory, sourceBucket, targetBucket, counterBucket)
	}
}

func runExecutorConcurrently(ctx context.Context, db kv.RoDB, tx kv.RwTx, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, proberFactory ProberFactory, sourceBucket, targetBucket, counterBucket string) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using concurrent executor", s.LogPrefix()))
	}

	source, err := tx.CursorDupSort(sourceBucket)
	if err != nil {
		return startBlock, err
	}
	defer source.Close()

	target, err := tx.CursorDupSort(targetBucket)
	if err != nil {
		return startBlock, err
	}
	defer target.Close()

	counter, err := tx.Cursor(counterBucket)
	if err != nil {
		return startBlock, err
	}
	defer counter.Close()

	// Restore last counter
	kct, vct, err := counter.Last()
	if err != nil {
		return startBlock, err
	}
	lastTotal := uint64(0)
	if kct != nil {
		lastTotal = binary.BigEndian.Uint64(kct)
	}

	// Indexer/counter sanity check
	kidx, _, err := target.Last()
	if err != nil {
		return startBlock, err
	}
	if !bytes.Equal(kidx, vct) {
		return startBlock, fmt.Errorf("bucket doesn't match counterBucket: bucket=%v counter=%v blockNum=%v counterBlockNum=%v", targetBucket, counterBucket, binary.BigEndian.Uint64(kidx), binary.BigEndian.Uint64(vct))
	}

	// Loop over [startBlock, endBlock]
	k, v, err := source.Seek(hexutility.EncodeTs(startBlock))
	if err != nil {
		return startBlock, err
	}

	// No data after startBlock in the source bucket; given endBlock comes from source stage's,
	// assume no data up to endBlock.
	if k == nil {
		return endBlock, nil
	}

	// First available data in the source bucket contains a block after the parent stage's endBlock,
	// which comes from parent stage's current block; this may indicate a bug in the code (db inconsistency)
	// or parent block number was set without proper db cleaning (wrong unwind).
	blockNum := binary.BigEndian.Uint64(k[:length.BlockNum])
	if blockNum > endBlock {
		return startBlock, fmt.Errorf("found data for block %d in sourceBucket %s, but stage endBlock is %d", blockNum, sourceBucket, endBlock)
	}

	// Setup concurrent probers support
	workers := estimate.AlmostAllCPUs()

	// Insert sourceBucket data into this channel and workers will probe them
	proberCh := make(chan *sourceData, workers*3)

	// Workers will post matched data here
	//
	// must be >= proberCh buffer size + n. of workers, otherwise can deadlock
	matchesCh := make(chan *matchedData, workers*3+workers)

	// Initialize N workers whose job is to read from proberCh, probe the address,
	// and in case of match, post results on matchesCh.
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		prober, err := proberFactory()
		if err != nil {
			return startBlock, err
		}
		createExecutor(ctx, db, blockReader, chainConfig, engine, prober, proberCh, matchesCh, &wg)
	}

	totalProbed := 0
	totalMatches := 0
	data := NewSourceData(blockNum, k, v)

L:
	// Outer loop handles sourceBucket DB reading -> proberCh (to be executed concurrently)
	// and matchesCh -> targetBucket DB writes.
	for {
		select {
		case proberCh <- data:
			totalProbed++

			// Compute next input for insertion into input channel
			k, v, err = source.NextDup()
			if err != nil {
				return startBlock, err
			}
			if k == nil {
				// Next block
				k, v, err = source.NextNoDup()
				if err != nil {
					return startBlock, err
				}

				if k == nil {
					break L
				}
			}

			blockNum = binary.BigEndian.Uint64(k[:length.BlockNum])
			if blockNum > endBlock {
				break L
			}

			data = NewSourceData(blockNum, k, v)
		case match := <-matchesCh:
			// Extract next matched data and save into db
			if err := tx.Put(targetBucket, match.k, match.v); err != nil {
				return startBlock, err
			}

			// Save attributes
			if err := AddOrUpdateAttributes(tx, match.addr, match.attrs); err != nil {
				return startBlock, err
			}

			totalMatches++
		case <-ctx.Done():
			return startBlock, common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Probing", s.LogPrefix()), "block", blockNum, "totalMatches", totalMatches, "totalProbed", totalProbed, "inCh", len(proberCh), "outCh", len(matchesCh))
		}
	}

	// Close prober channel and wait for workers to finish
	close(proberCh)
	wg.Wait()

	// Close out channel and drain remaining data saving them into db; we are
	// guaranteed no more probing data will be posted here.
	close(matchesCh)
	for match := range matchesCh {
		if err := tx.Put(targetBucket, match.k, match.v); err != nil {
			return startBlock, err
		}
		totalMatches++
	}

	// Rewind target cursor and write counters [startBlock, endBlock]
	k, _, err = target.Seek(hexutility.EncodeTs(startBlock))
	if err != nil {
		return startBlock, err
	}

	currBlockTotal := lastTotal
	for k != nil {
		blockNum := binary.BigEndian.Uint64(k[:length.BlockNum])
		count, err := target.CountDuplicates()
		if err != nil {
			return startBlock, err
		}
		currBlockTotal += count

		// Persist accumulated counter for current block
		if err := writeCounter(tx, counterBucket, currBlockTotal, blockNum); err != nil {
			return startBlock, err
		}

		// Next block containing matches
		k, _, err = target.NextNoDup()
		if err != nil {
			return startBlock, err
		}
	}

	// Don't print summary if no contracts were analyzed to avoid polluting logs
	if !isShortInterval && totalProbed > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "totalMatches", totalMatches, "totalProbed", totalProbed)
	}

	return endBlock, nil
}

func runExecutorIncrementally(ctx context.Context, tx kv.RwTx, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, proberFactory ProberFactory, sourceBucket, targetBucket, counterBucket string) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using incremental executor", s.LogPrefix()))
	}

	// Open required cursors
	source, err := tx.CursorDupSort(sourceBucket)
	if err != nil {
		return startBlock, err
	}
	defer source.Close()

	target, err := tx.CursorDupSort(targetBucket)
	if err != nil {
		return startBlock, err
	}
	defer target.Close()

	counter, err := tx.Cursor(counterBucket)
	if err != nil {
		return startBlock, err
	}
	defer counter.Close()

	// Restore last counter
	kct, vct, err := counter.Last()
	if err != nil {
		return startBlock, err
	}
	lastTotal := uint64(0)
	if kct != nil {
		lastTotal = binary.BigEndian.Uint64(kct)
	}

	// Indexer/counter sanity check
	kidx, _, err := target.Last()
	if err != nil {
		return startBlock, err
	}
	if !bytes.Equal(kidx, vct) {
		return startBlock, fmt.Errorf("bucket doesn't match counterBucket: bucket=%v counter=%v blockNum=%v counterBlockNum=%v", targetBucket, counterBucket, binary.BigEndian.Uint64(kidx), binary.BigEndian.Uint64(vct))
	}

	prober, err := proberFactory()
	if err != nil {
		return startBlock, err
	}

	getHeader := func(hash common.Hash, n uint64) *types.Header {
		h, err := blockReader.Header(ctx, tx, hash, n)
		if err != nil {
			log.Error("Can't get block hash by number", "number", n, "only-canonical", false)
			return nil
		}
		return h
	}

	ex := executor{
		ctx:         ctx,
		tx:          tx,
		blockReader: blockReader,
		chainConfig: chainConfig,
		getHeader:   getHeader,
		engine:      engine,
		prober:      prober,
	}

	// Loop over [startBlock, endBlock]
	k, v, err := source.Seek(hexutility.EncodeTs(startBlock))
	if err != nil {
		return startBlock, err
	}

	// No new data in the [startBlock, ...] interval in the source bucket; since endBlock comes
	// from sourceBucket's stage, assumes the entire interval is done.
	if k == nil {
		return endBlock, nil
	}

	// First available data in the source bucket contains a block after the parent stage's endBlock,
	// which comes from parent stage's current block; this may indicate a bug in the code (db inconsistency)
	// or parent block number was set without proper db cleaning (wrong unwind).
	blockNum := binary.BigEndian.Uint64(k[:length.BlockNum])
	if blockNum > endBlock {
		return startBlock, fmt.Errorf("found data for block %d in sourceBucket %s, but stage endBlock is %d", blockNum, sourceBucket, endBlock)
	}

	prevBlockTotal := lastTotal
	currBlockTotal := lastTotal

	totalProbed := 0
	for {
		totalProbed++
		addr := common.BytesToAddress(v)
		attrs, err := ex.ResetAndProbe(blockNum, addr, k, v)
		if err != nil {
			// Ignore execution errors on purpose
			log.Warn("ignored error", "err", err)
		}

		// Save match
		if attrs != nil {
			if err := tx.Put(targetBucket, k, v); err != nil {
				return startBlock, err
			}

			// Save attributes
			if err := AddOrUpdateAttributes(tx, addr, attrs); err != nil {
				return startBlock, err
			}

			currBlockTotal++
		}

		// Compute next input for insertion into input channel
		k, v, err = source.NextDup()
		if err != nil {
			return startBlock, err
		}
		if k == nil {
			// Next block
			k, v, err = source.NextNoDup()
			if err != nil {
				return startBlock, err
			}

			// EOF
			if k == nil {
				if currBlockTotal > prevBlockTotal {
					// Cut block counter and save accumulated totals
					if err := writeCounter(tx, counterBucket, currBlockTotal, blockNum); err != nil {
						return startBlock, err
					}
				}
				break
			}
		}

		newBlockNum := binary.BigEndian.Uint64(k[:length.BlockNum])
		if newBlockNum != blockNum {
			if currBlockTotal > prevBlockTotal {
				// Cut block counter and save accumulated totals
				if err := writeCounter(tx, counterBucket, currBlockTotal, blockNum); err != nil {
					return startBlock, err
				}
				prevBlockTotal = currBlockTotal
			}

			if newBlockNum > endBlock {
				break
			}
			blockNum = newBlockNum
		}

		select {
		default:
		case <-ctx.Done():
			return startBlock, common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Probing", s.LogPrefix()), "block", blockNum, "totalMatches", currBlockTotal-lastTotal, "totalProbed", totalProbed)
		}
	}

	// Don't print summary if no contracts were analyzed to avoid polluting logs
	if !isShortInterval && totalProbed > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "totalMatches", currBlockTotal-lastTotal, "totalProbed", totalProbed)
	}

	return endBlock, nil
}

type sourceData struct {
	blockNum uint64
	addr     common.Address
	k        []byte
	v        []byte
}

func NewSourceData(blockNum uint64, k, v []byte) *sourceData {
	addr := common.BytesToAddress(v)
	ck := make([]byte, len(k))
	cv := make([]byte, len(v))
	copy(ck, k)
	copy(cv, v)
	return &sourceData{blockNum, addr, ck, cv}
}

type matchedData struct {
	blockNum uint64
	addr     common.Address
	k        []byte
	v        []byte
	attrs    *roaring64.Bitmap
}

func AddOrUpdateAttributes(tx kv.RwTx, addr common.Address, attrs *roaring64.Bitmap) error {
	if attrs == nil {
		log.Warn("attrs bitmap shouldn't be nil")
	}

	addrKey := addr.Bytes()
	a, err := tx.GetOne(kv.OtsAddrAttributes, addrKey)
	if err != nil {
		return err
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	if a != nil {
		if _, err := bm.ReadFrom(bytes.NewReader(a)); err != nil {
			return err
		}
	}
	bm.Or(attrs)

	bm.RunOptimize()
	b, err := bm.ToBytes()
	if err != nil {
		return err
	}
	if err := tx.Put(kv.OtsAddrAttributes, addrKey, b); err != nil {
		return err
	}

	return nil
}

func RemoveAttributes(tx kv.RwTx, addr common.Address, attrs *roaring64.Bitmap) error {
	if attrs == nil {
		log.Warn("attrs bitmap shouldn't be nil")
	}

	addrKey := addr.Bytes()
	a, err := tx.GetOne(kv.OtsAddrAttributes, addrKey)
	if err != nil {
		return err
	}
	if a == nil {
		return nil
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	if _, err := bm.ReadFrom(bytes.NewReader(a)); err != nil {
		return err
	}
	bm.AndNot(attrs)

	// If this attribute removal leads to no attributes set, remove the record entirely
	if bm.IsEmpty() {
		return tx.Delete(kv.OtsAddrAttributes, addrKey)
	}

	// Optimize and update
	bm.RunOptimize()
	b, err := bm.ToBytes()
	if err != nil {
		return err
	}
	if err := tx.Put(kv.OtsAddrAttributes, addrKey, b); err != nil {
		return err
	}

	return nil
}

func writeCounter(tx kv.RwTx, counterBucket string, counter, blockNum uint64) error {
	k := hexutility.EncodeTs(counter)
	v := hexutility.EncodeTs(blockNum)
	return tx.Put(counterBucket, k, v)
}

type executor struct {
	ctx         context.Context
	tx          kv.Tx
	blockReader services.FullBlockReader
	chainConfig *chain.Config
	getHeader   func(common.Hash, uint64) *types.Header
	engine      consensus.Engine
	prober      Prober
}

func createExecutor(ctx context.Context, db kv.RoDB, blockReader services.FullBlockReader, chainConfig *chain.Config, engine consensus.Engine, prober Prober, proberCh <-chan *sourceData, matchesCh chan<- *matchedData, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		tx, err := db.BeginRo(ctx)
		if err != nil {
			// TODO: handle error
			return
		}
		defer tx.Rollback()

		getHeader := func(hash common.Hash, n uint64) *types.Header {
			h, err := blockReader.Header(ctx, tx, hash, n)
			if err != nil {
				log.Error("Can't get block hash by number", "number", n, "only-canonical", false)
				return nil
			}
			return h
		}

		ex := executor{
			ctx:         ctx,
			tx:          tx,
			blockReader: blockReader,
			chainConfig: chainConfig,
			getHeader:   getHeader,
			engine:      engine,
			prober:      prober,
		}

		for {
			// wait for input
			source, ok := <-proberCh
			if !ok {
				break
			}

			attrs, err := ex.ResetAndProbe(source.blockNum, source.addr, source.k, source.v)
			if err != nil {
				// Ignore execution errors on purpose
				log.Warn("ignored error", "err", err)
			}
			if attrs != nil {
				matchesCh <- &matchedData{source.blockNum, source.addr, source.k, source.v, attrs}
			}
		}
	}()
}

func (ex *executor) ResetAndProbe(blockNumber uint64, addr common.Address, k, v []byte) (*roaring64.Bitmap, error) {
	header, err := ex.blockReader.HeaderByNumber(ex.ctx, ex.tx, blockNumber)
	if err != nil {
		return nil, err
	}
	if header == nil {
		// TODO: corrupted?
		log.Warn("couldn't find header", "blockNum", blockNumber)
		return nil, nil
	}

	stateReader := state.NewPlainState(ex.tx, blockNumber+1, systemcontracts.SystemContractCodeLookup[ex.chainConfig.ChainName])
	defer stateReader.Dispose()

	ibs := state.New(stateReader)
	blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, ex.getHeader), ex.engine, nil /* author */)
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, ex.chainConfig, vm.Config{NoBaseFee: true})

	return ex.prober.Probe(ex.ctx, evm, header, ex.chainConfig, ibs, blockNumber, addr, k, v)
}
