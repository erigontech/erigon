package state

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
)

type blockKeys struct {
	blockNum    uint64
	accountKeys []string
	storageKeys []string
}

type keyBatch struct {
	blocks    []blockKeys
	fromBlock uint64
	toBlock   uint64 // inclusive
	sizeBytes uint64 // approx memory: sum of len(key)+8 per entry
	err       error
}

type batchFetcher struct {
	db           kv.TemporalRwDB
	txNumsReader rawdbv3.TxNumsReader
	batchBlocks  int
	batches      chan keyBatch // capacity 1
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	exhaust      chan struct{} // closed by Exhaust()
}

func newBatchFetcher(ctx context.Context, db kv.TemporalRwDB, txNumsReader rawdbv3.TxNumsReader, batchBlocks int, startBlock, endBlock uint64) *batchFetcher {
	ctx, cancel := context.WithCancel(ctx)
	f := &batchFetcher{
		db:           db,
		txNumsReader: txNumsReader,
		batchBlocks:  batchBlocks,
		batches:      make(chan keyBatch, 1),
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
		exhaust:      make(chan struct{}),
	}
	go f.run(startBlock, endBlock)
	return f
}

func (f *batchFetcher) run(startBlock, endBlock uint64) {
	defer close(f.done)

	for currentBlock := startBlock; currentBlock <= endBlock; {
		batchEnd := currentBlock + uint64(f.batchBlocks) - 1
		if batchEnd > endBlock {
			batchEnd = endBlock
		}

		select {
		case <-f.exhaust:
			return
		case <-f.ctx.Done():
			return
		default:
		}

		batch := f.fetchBatch(currentBlock, batchEnd)
		select {
		case f.batches <- batch:
		case <-f.ctx.Done():
			return
		}
		if batch.err != nil {
			return
		}
		currentBlock = batchEnd + 1
	}
}

func (f *batchFetcher) fetchBatch(fromBlock, toBlock uint64) keyBatch {
	roTx, err := f.db.BeginTemporalRo(f.ctx)
	if err != nil {
		return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
	}
	defer roTx.Rollback()

	numBlocks := int(toBlock - fromBlock + 1)
	maxTxNums := make([]uint64, numBlocks)
	for i := 0; i < numBlocks; i++ {
		maxTxNums[i], err = f.txNumsReader.Max(f.ctx, roTx, fromBlock+uint64(i))
		if err != nil {
			return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
		}
	}

	batchFromTxNum, err := f.txNumsReader.Min(f.ctx, roTx, fromBlock)
	if err != nil {
		return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
	}
	batchToTxNum := maxTxNums[numBlocks-1] + 1 // exclusive end

	blocks := make([]blockKeys, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks[i].blockNum = fromBlock + uint64(i)
	}

	type fetchedKey struct {
		txNum     uint64
		key       string
		isStorage bool
	}
	var entries []fetchedKey

	accIt, err := roTx.Debug().HistoryKeyTxNumRange(kv.AccountsDomain, int(batchFromTxNum), int(batchToTxNum), order.Asc, -1)
	if err != nil {
		return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
	}
	for accIt.HasNext() {
		k, txNum, err := accIt.Next()
		if err != nil {
			accIt.Close()
			return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
		}
		entries = append(entries, fetchedKey{txNum: txNum, key: string(k)})
	}
	accIt.Close()

	storIt, err := roTx.Debug().HistoryKeyTxNumRange(kv.StorageDomain, int(batchFromTxNum), int(batchToTxNum), order.Asc, -1)
	if err != nil {
		return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
	}
	for storIt.HasNext() {
		k, txNum, err := storIt.Next()
		if err != nil {
			storIt.Close()
			return keyBatch{fromBlock: fromBlock, toBlock: toBlock, err: err}
		}
		entries = append(entries, fetchedKey{txNum: txNum, key: string(k), isStorage: true})
	}
	storIt.Close()

	sort.Slice(entries, func(i, j int) bool { return entries[i].txNum < entries[j].txNum })

	var sizeBytes uint64
	blockIdx := 0
	for _, e := range entries {
		for blockIdx < numBlocks && maxTxNums[blockIdx] < e.txNum {
			blockIdx++
		}
		if blockIdx >= numBlocks {
			return keyBatch{fromBlock: fromBlock, toBlock: toBlock,
				err: fmt.Errorf("fetchBatch: txNum %d beyond block range [%d, %d] (maxTxNum %d)", e.txNum, fromBlock, toBlock, maxTxNums[numBlocks-1])}
		}
		if e.isStorage {
			blocks[blockIdx].storageKeys = append(blocks[blockIdx].storageKeys, e.key)
		} else {
			blocks[blockIdx].accountKeys = append(blocks[blockIdx].accountKeys, e.key)
		}
		sizeBytes += uint64(len(e.key)) + 8
	}

	return keyBatch{
		blocks:    blocks,
		fromBlock: fromBlock,
		toBlock:   toBlock,
		sizeBytes: sizeBytes,
	}
}

func (f *batchFetcher) Receive() (keyBatch, bool) {
	select {
	case batch := <-f.batches:
		return batch, true
	case <-f.done:
		select {
		case batch := <-f.batches:
			return batch, true
		default:
			return keyBatch{}, false
		}
	case <-f.ctx.Done():
		return keyBatch{err: f.ctx.Err()}, false
	}
}

func (f *batchFetcher) Exhaust() {
	select {
	case <-f.exhaust:
	default:
		close(f.exhaust)
	}
}

func (f *batchFetcher) Stop() {
	f.cancel()
	// fetchBatch may be in long file I/O that doesn't check context;
	// bound the wait so shutdown isn't blocked indefinitely
	t := time.NewTimer(5 * time.Second)
	select {
	case <-f.done:
		t.Stop()
	case <-t.C:
	}
	for {
		select {
		case <-f.batches:
		default:
			return
		}
	}
}
