package state

import (
	"context"
	"sort"

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

	var sizeBytes uint64

	// Account keys
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
		idx := sort.Search(numBlocks, func(i int) bool { return maxTxNums[i] >= txNum })
		if idx < numBlocks {
			blocks[idx].accountKeys = append(blocks[idx].accountKeys, string(k))
			sizeBytes += uint64(len(k)) + 8
		}
	}
	accIt.Close()

	// Storage keys
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
		idx := sort.Search(numBlocks, func(i int) bool { return maxTxNums[i] >= txNum })
		if idx < numBlocks {
			blocks[idx].storageKeys = append(blocks[idx].storageKeys, string(k))
			sizeBytes += uint64(len(k)) + 8
		}
	}
	storIt.Close()

	return keyBatch{
		blocks:    blocks,
		fromBlock: fromBlock,
		toBlock:   toBlock,
		sizeBytes: sizeBytes,
	}
}

func (f *batchFetcher) Receive() keyBatch {
	select {
	case batch := <-f.batches:
		return batch
	case <-f.ctx.Done():
		return keyBatch{err: f.ctx.Err()}
	}
}

func (f *batchFetcher) Stop() {
	f.cancel()
	<-f.done
	// drain any remaining batch
	for {
		select {
		case <-f.batches:
		default:
			return
		}
	}
}
