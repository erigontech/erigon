package state

import (
	"context"
	"math"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

// keyPrefetcher fetches changed keys for upcoming blocks in parallel goroutines
// while the main loop processes the current block.
type prefetchWork struct {
	blockNum           uint64
	fromTxNum, toTxNum uint64
}

type prefetchResult struct {
	blockNum    uint64
	accountKeys []string
	storageKeys []string
	err         error
}

type keyPrefetcher struct {
	db      kv.TemporalRwDB
	work    chan prefetchWork
	results chan prefetchResult

	eg     *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
}

func newKeyPrefetcher(ctx context.Context, db kv.TemporalRwDB, workers, lookahead int) *keyPrefetcher {
	ctx, cancel := context.WithCancel(ctx)
	p := &keyPrefetcher{
		db:      db,
		work:    make(chan prefetchWork, lookahead),
		results: make(chan prefetchResult, lookahead),
		ctx:     ctx,
		cancel:  cancel,
	}
	p.eg = &errgroup.Group{}
	for i := 0; i < workers; i++ {
		p.eg.Go(p.worker)
	}
	return p
}

func (p *keyPrefetcher) Submit(blockNum, fromTxNum, toTxNum uint64) {
	select {
	case p.work <- prefetchWork{blockNum: blockNum, fromTxNum: fromTxNum, toTxNum: toTxNum}:
	case <-p.ctx.Done():
	}
}

func (p *keyPrefetcher) Receive() prefetchResult {
	select {
	case r := <-p.results:
		return r
	case <-p.ctx.Done():
		return prefetchResult{err: p.ctx.Err()}
	}
}

func (p *keyPrefetcher) Stop() {
	p.cancel()
	// drain work channel so workers can exit
	for {
		select {
		case <-p.work:
		default:
			goto drained
		}
	}
drained:
	_ = p.eg.Wait()
	// drain any remaining results
	for {
		select {
		case <-p.results:
		default:
			return
		}
	}
}

func (p *keyPrefetcher) worker() error {
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case w, ok := <-p.work:
			if !ok {
				return nil
			}
			p.results <- p.fetchBlock(w)
		}
	}
}

func (p *keyPrefetcher) fetchBlock(w prefetchWork) prefetchResult {
	roTx, err := p.db.BeginTemporalRo(p.ctx)
	if err != nil {
		return prefetchResult{blockNum: w.blockNum, err: err}
	}
	defer roTx.Rollback()

	var accountKeys, storageKeys []string

	accIt, err := roTx.Debug().HistoryKeyRange(kv.AccountsDomain, int(w.fromTxNum), int(w.toTxNum+1), order.Asc, math.MaxInt64)
	if err != nil {
		return prefetchResult{blockNum: w.blockNum, err: err}
	}
	for accIt.HasNext() {
		k, _, err := accIt.Next()
		if err != nil {
			accIt.Close()
			return prefetchResult{blockNum: w.blockNum, err: err}
		}
		accountKeys = append(accountKeys, string(k))
	}
	accIt.Close()

	storIt, err := roTx.Debug().HistoryKeyRange(kv.StorageDomain, int(w.fromTxNum), int(w.toTxNum+1), order.Asc, math.MaxInt64)
	if err != nil {
		return prefetchResult{blockNum: w.blockNum, err: err}
	}
	for storIt.HasNext() {
		k, _, err := storIt.Next()
		if err != nil {
			storIt.Close()
			return prefetchResult{blockNum: w.blockNum, err: err}
		}
		storageKeys = append(storageKeys, string(k))
	}
	storIt.Close()

	return prefetchResult{blockNum: w.blockNum, accountKeys: accountKeys, storageKeys: storageKeys}
}
