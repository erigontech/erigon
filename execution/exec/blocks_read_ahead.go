package exec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/balcache"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type BlockReadAheader struct {
	// keeps some caches for block themselves
	headers *lru.Cache[common.Hash, *types.Header]
	bodies  *lru.Cache[common.Hash, *types.Body]
	senders *lru.Cache[common.Hash, []byte] // just do raw senders

	// this is for warming state
	warming atomic.Bool // only one warmBody can run at a time
	warmWg  sync.WaitGroup

	// stateCache is the process-global state cache that SharedDomains.GetLatest
	// consults on the EVM hot path. When set, warmBody routes its prefetches
	// through a cache-populating getter so the same hashmap the EVM probes is
	// pre-warmed. Without it, prefetches only warm OS page cache + RoTx
	// cursors — disconnected from the cache layer the EVM actually reads.
	// Mirrors reth's CachedReads / ExecutionCache "same hashmap" property.
	stateCache *cache.StateCache
}

func NewBlockReadAheader() *BlockReadAheader {
	headers, err := lru.New[common.Hash, *types.Header](4)
	if err != nil {
		panic(err)
	}
	bodies, err := lru.New[common.Hash, *types.Body](4)
	if err != nil {
		panic(err)
	}
	senders, err := lru.New[common.Hash, []byte](4)
	if err != nil {
		panic(err)
	}
	return &BlockReadAheader{
		headers: headers,
		bodies:  bodies,
		senders: senders,
	}
}

// SetStateCache wires the process-global state cache so warmBody's
// prefetches land in the same hashmap that SharedDomains.GetLatest probes
// on the EVM hot path. Without this, prefetches warm OS page cache only —
// the EVM still pays the file accessor stack on its first per-address read.
// Idempotent; safe to call before the first AddHeaderAndBody.
func (bra *BlockReadAheader) SetStateCache(sc *cache.StateCache) {
	bra.stateCache = sc
}

// cachePopulatingGetter wraps a kv.TemporalGetter and writes successful
// reads through to a cache.StateCache as a side effect. Used by warmBody
// to make read-ahead prefetches populate the same in-process cache layer
// that SharedDomains.GetLatest consults — eliminating the file-accessor
// stack cost on the EVM's first touch of any prefetched address.
//
// For the CodeDomain, when the code bytes come back together with the
// owning account's codeHash (decoded from a preceding AccountsDomain read
// in the same loop iteration), the wrapper also populates the L2b
// ethHash→bytes + size-cache layers via PutCodeWithHash. The codeHash
// hint is provided per-iteration via withCodeHashHint().
type cachePopulatingGetter struct {
	g            kv.TemporalGetter
	sc           *cache.StateCache
	codeHashHint []byte // valid only for the next CodeDomain read; cleared after use
}

func (cpg *cachePopulatingGetter) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	v, step, err := cpg.g.GetLatest(name, k)
	if err == nil && cpg.sc != nil {
		if name == kv.CodeDomain && len(cpg.codeHashHint) > 0 {
			cpg.sc.PutCodeWithHash(k, v, cpg.codeHashHint)
			cpg.codeHashHint = nil
		} else {
			// Cache including nil/empty results: a probe returning no
			// bytes is a valid negative answer (missing account, empty
			// storage slot, no code) and caching it lets repeated probes
			// skip the file accessor stack. Mirrors revm's CacheAccount
			// { account: None, status: LoadedNotExisting } pattern.
			cpg.sc.Put(name, k, v)
		}
	}
	return v, step, err
}

func (cpg *cachePopulatingGetter) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return cpg.g.HasPrefix(name, prefix)
}

func (cpg *cachePopulatingGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return cpg.g.StepsInFiles(entitySet...)
}

// withCodeHashHint stashes the codeHash so the next CodeDomain read routes
// through PutCodeWithHash (populating L2b + size cache) instead of a bare
// addr-keyed Put. Caller MUST follow this with a single GetLatest(CodeDomain, …)
// for the matching addr; the hint clears on use.
func (cpg *cachePopulatingGetter) withCodeHashHint(ethHash []byte) {
	cpg.codeHashHint = ethHash
}

func (bra *BlockReadAheader) AddHeaderAndBody(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body) {
	blockHash := header.Hash()
	bra.headers.Add(blockHash, header)
	bra.bodies.Add(blockHash, body)
	if db != nil && ctx != nil {
		// Only allow one warmBody to run at a time
		if !bra.warming.CompareAndSwap(false, true) {
			return
		}
		bra.warmWg.Add(1)
		go func() {
			defer bra.warmWg.Done()
			bra.warmBody(ctx, db, header, body, 8) // use 8 workers for warming
		}()
	}
}

// WaitForWarmup blocks until any in-flight warmBody goroutine finishes or
// the context is cancelled. Call before closing the database to avoid
// waitTxsAllDoneOnClose hangs.
func (bra *BlockReadAheader) WaitForWarmup(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		bra.warmWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (bra *BlockReadAheader) AddSenders(senders []byte, blockHash common.Hash) {
	if _, ok := bra.bodies.Get(blockHash); !ok {
		return
	}
	bra.senders.Add(blockHash, common.Copy(senders))
}

// warmBody warms state for all transactions in a body using multiple workers.
// It reads: To accounts, To account code, To account storage from access lists,
// and block-level access lists. Each worker creates its own transaction.
// Only one warmBody can run at a time - concurrent calls are no-ops.
func (bra *BlockReadAheader) warmBody(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body, workers int) {
	defer bra.warming.Store(false)

	if !dbg.ReadAhead {
		return
	}

	if workers <= 0 {
		workers = 1
	}

	var wg errgroup.Group

	// BAL source = the in-memory balcache (populated by
	// EngineServer.HandleNewPayload on receipt). The chaindata
	// kv.BlockAccessList table no longer exists; cache miss is a clean
	// signal that BAL prefetch is not available for this block — fall
	// through to per-transaction warming below.
	var bal types.BlockAccessList
	if header != nil {
		if data, ok := balcache.CachedBlockAccessList(header.Hash()); ok && len(data) > 0 {
			decoded, err := types.DecodeBlockAccessListBytes(data)
			if err != nil {
				log.Warn("[warmBody] failed to decode BAL", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
			} else {
				bal = decoded
			}
		}
	}

	balLen := len(bal)
	if balLen > 0 {
		balWorkers := min(workers, balLen)

		// Pre-divide work: each worker gets a dedicated range of BAL entries
		entriesPerWorker := (balLen + balWorkers - 1) / balWorkers

		for w := 0; w < balWorkers; w++ {
			start := w * entriesPerWorker
			end := min(start+entriesPerWorker, balLen)
			if start >= balLen {
				break
			}

			// Capture loop variables for closure
			workerStart, workerEnd, workerID := start, end, w
			wg.Go(func() error {
				startTime := time.Now()
				tx, err := db.BeginRo(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()

				ttx, ok := tx.(kv.TemporalTx)
				if !ok {
					return nil
				}
				var getter kv.TemporalGetter = ttx
				var cpg *cachePopulatingGetter
				if bra.stateCache != nil {
					cpg = &cachePopulatingGetter{g: ttx, sc: bra.stateCache}
					getter = cpg
				}
				stateReader := state.NewReaderV3(getter)

				for idx := workerStart; idx < workerEnd; idx++ {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					acctChanges := bal[idx]
					acct, _ := stateReader.ReadAccountData(acctChanges.Address)
					// Warm code if account has code or if there are code changes.
					// When we already know the codeHash from the account read, hint
					// the cache-populating getter so the code bytes land in the L2b
					// (ethHash → bytes) + size layers — not just the addr-keyed L1.
					if acct != nil && !acct.CodeHash.IsEmpty() {
						if cpg != nil {
							h := acct.CodeHash.Value()
							cpg.withCodeHashHint(h[:])
						}
						stateReader.ReadAccountCode(acctChanges.Address)
					} else if len(acctChanges.CodeChanges) > 0 {
						stateReader.ReadAccountCode(acctChanges.Address)
					}
					for _, slotChanges := range acctChanges.StorageChanges {
						stateReader.ReadAccountStorage(acctChanges.Address, slotChanges.Slot)
					}
					for _, slot := range acctChanges.StorageReads {
						stateReader.ReadAccountStorage(acctChanges.Address, slot)
					}
				}
				log.Debug("[warmBody] BAL worker finished", "worker", workerID, "entries", workerEnd-workerStart, "elapsed", time.Since(startTime))
				return nil
			})
		}
		wg.Wait()
		return
	}
	// Fallback: per-transaction warming when no BAL
	txns := body.Transactions
	if len(txns) == 0 {
		return
	}

	txnLen := len(txns)
	if workers > txnLen {
		workers = txnLen
	}

	// Pre-divide work: each worker gets a dedicated range of transactions
	txnsPerWorker := (txnLen + workers - 1) / workers

	for w := 0; w < workers; w++ {
		start := w * txnsPerWorker
		end := min(start+txnsPerWorker, txnLen)
		if start >= txnLen {
			break
		}

		// Capture loop variables for closure
		workerStart, workerEnd, workerID := start, end, w
		wg.Go(func() error {
			startTime := time.Now()
			tx, err := db.BeginRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			ttx, ok := tx.(kv.TemporalTx)
			if !ok {
				return nil
			}
			var getter kv.TemporalGetter = ttx
			var cpg *cachePopulatingGetter
			if bra.stateCache != nil {
				cpg = &cachePopulatingGetter{g: ttx, sc: bra.stateCache}
				getter = cpg
			}
			stateReader := state.NewReaderV3(getter)

			for txIdx := workerStart; txIdx < workerEnd; txIdx++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				txn := txns[txIdx]

				// Warm To account and its code if it has one
				if toAddr := txn.GetTo(); toAddr != nil {
					to := accounts.InternAddress(*toAddr)
					if acct, _ := stateReader.ReadAccountData(to); acct != nil && !acct.CodeHash.IsEmpty() {
						if cpg != nil {
							h := acct.CodeHash.Value()
							cpg.withCodeHashHint(h[:])
						}
						stateReader.ReadAccountCode(to)
					}
				}

				// Warm transaction access list accounts and their code
				for _, entry := range txn.GetAccessList() {
					addr := accounts.InternAddress(entry.Address)
					if acct, _ := stateReader.ReadAccountData(addr); acct != nil && !acct.CodeHash.IsEmpty() {
						if cpg != nil {
							h := acct.CodeHash.Value()
							cpg.withCodeHashHint(h[:])
						}
						stateReader.ReadAccountCode(addr)
					}
					for _, slot := range entry.StorageKeys {
						stateReader.ReadAccountStorage(addr, accounts.InternKey(slot))
					}
				}
			}
			log.Debug("[warmBody] TX worker finished", "worker", workerID, "txns", workerEnd-workerStart, "elapsed", time.Since(startTime))
			return nil
		})
	}

	wg.Wait()
}

func (bra *BlockReadAheader) ReadBodyWithTransactions(blockHash common.Hash) (*types.Body, bool) {
	return bra.bodies.Get(blockHash)
}

func (bra *BlockReadAheader) ReadBlockWithSenders(blockHash common.Hash) (*types.Block, bool) {
	header, ok := bra.headers.Get(blockHash)
	if header == nil || !ok {
		return nil, false
	}
	body, ok := bra.bodies.Get(blockHash)
	if body == nil || !ok {
		return nil, false
	}
	senders, ok := bra.senders.Get(blockHash)
	if len(senders) == 0 || !ok {
		return nil, false
	}
	sendersAddresses := make([]common.Address, 0, len(senders)/length.Addr)
	for i := 0; i < len(senders); i += length.Addr {
		sendersAddresses = append(sendersAddresses, common.BytesToAddress(senders[i:i+length.Addr]))
	}
	body.SendersToTxs(sendersAddresses)
	return types.NewBlockFromStorage(header.Hash(), header, body.Transactions, body.Uncles, body.Withdrawals), true
}

func BlocksReadAhead(ctx context.Context, workers int, db kv.RoDB, engine rules.Engine, blockReader services.FullBlockReader) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 500
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for workerNum := 0; workerNum < workers; workerNum++ {
		g.Go(func() (err error) {
			var bn uint64
			var ok bool
			var tx kv.Tx
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()

			for i := 0; ; i++ {
				select {
				case bn, ok = <-readAhead:
					if !ok {
						return
					}
				case <-gCtx.Done():
					return gCtx.Err()
				}

				if i%100 == 0 {
					if tx != nil {
						tx.Rollback()
					}
					tx, err = db.BeginRo(ctx)
					if err != nil {
						return err
					}
				}

				if err := blocksReadAheadFunc(gCtx, tx, bn+readAheadBlocks, engine, blockReader); err != nil {
					return err
				}
			}
		})
	}
	return readAhead, func() {
		close(readAhead)
		_ = g.Wait()
	}
}
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, blockNum uint64, engine rules.Engine, blockReader services.FullBlockReader) error {
	block, err := blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache

	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}

	stateReader := state.NewReaderV3(ttx)
	senders := block.Body().SendersFromTxs()

	for _, sender := range senders {
		a, _ := stateReader.ReadAccountData(accounts.InternAddress(sender))
		if a == nil {
			continue
		}

		//Code domain using .bt index - means no false-positives
		if code, _ := stateReader.ReadAccountCode(accounts.InternAddress(sender)); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}

	for _, txn := range block.Transactions() {
		toaddr := txn.GetTo()
		if toaddr != nil {
			to := accounts.InternAddress(*toaddr)
			a, _ := stateReader.ReadAccountData(to)
			if a == nil {
				continue
			}
			//if account != nil && !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
			//	reader.Code(*tx.To(), common.BytesToHash(account.CodeHash))
			//}
			if code, _ := stateReader.ReadAccountCode(to); len(code) > 0 {
				_, _ = code[0], code[len(code)-1]
			}

			for _, list := range txn.GetAccessList() {
				stateReader.ReadAccountData(accounts.InternAddress(list.Address))
				if len(list.StorageKeys) > 0 {
					for _, slot := range list.StorageKeys {
						stateReader.ReadAccountStorage(accounts.InternAddress(list.Address), accounts.InternKey(slot))
					}
				}
			}
			//TODO: exec txn and pre-fetch commitment keys. see also: `func (p *statePrefetcher) Prefetch` in geth
		}

	}
	_, _ = stateReader.ReadAccountData(accounts.InternAddress(block.Coinbase()))

	return nil
}
