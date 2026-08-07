package exec

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
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
// For the CodeDomain the wrapper also populates the codeHashToCode
// (codeHash→bytes) + size-cache layers via PutCodeWithHashIfAbsent, keyed by
// the code's own keccak hash so every cached pair is self-consistent.
type cachePopulatingGetter struct {
	g                 kv.TemporalGetter
	sc                *cache.StateCache
	progress          func(kv.Domain) uint64 // domain progress source for stamping negative fills
	stepSize          uint64                 // for the read txNum upper bound (last txNum of the read's step)
	lastCodeAddr      common.Address
	lastCodeHash      common.Hash
	lastCodeHashKnown bool
}

func newCachePopulatingGetter(tx kv.TemporalTx, sc *cache.StateCache) *cachePopulatingGetter {
	debug := tx.Debug()
	return &cachePopulatingGetter{g: tx, sc: sc, progress: debug.DomainProgress, stepSize: debug.StepSize()}
}

func (cpg *cachePopulatingGetter) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	v, step, err := cpg.g.GetLatest(name, k)
	if name == kv.AccountsDomain {
		cpg.lastCodeHashKnown = false
		if err == nil && len(k) == len(cpg.lastCodeAddr) {
			if codeHash := accounts.DeserialiseV3CodeHash(v); len(codeHash) == len(cpg.lastCodeHash) {
				cpg.lastCodeAddr = common.BytesToAddress(k)
				copy(cpg.lastCodeHash[:], codeHash)
				cpg.lastCodeHashKnown = true
			}
		}
	}
	if err == nil && cpg.sc != nil {
		// If-absent writes only: this runs in a fire-and-forget goroutine over a
		// committed snapshot, so an unconditional Put racing an FCU flush's
		// cache-apply could replace the flushed value with the pre-flush one.
		if name == kv.CodeDomain && len(v) > 0 {
			// Key the content cache by the code's OWN hash, never a separately
			// read account codeHash: under parallel/speculative exec that hash
			// can be skewed or cross-account, and a (hash, code) pair that
			// doesn't satisfy keccak(code)==hash poisons every account sharing
			// the hash. keccak(v) makes each entry self-consistent.
			cpg.sc.PutCodeWithHashIfAbsent(k, v, crypto.Keccak256(v), (uint64(step)+1)*cpg.stepSize-1)
		} else {
			// Cache including nil/empty results: a probe returning no
			// bytes is a valid negative answer (missing account, empty
			// storage slot; empty code lands here too but CodeCache drops
			// zero-length puts) and caching it lets repeated probes
			// skip the file accessor stack. Mirrors revm's CacheAccount
			// { account: None, status: LoadedNotExisting } pattern.
			// Stamp with an upper bound on the value's write txNum (last txNum
			// of the step it came from) so unwind invalidation is correct. A
			// negative carries no step — stamp it with the domain's progress
			// at observation time so any unwind drops it (as the SD read-fill
			// does).
			readTxNum := (uint64(step)+1)*cpg.stepSize - 1
			if len(v) == 0 && name != kv.CodeDomain && cpg.sc.GetCache(name) != nil {
				readTxNum = cpg.progress(name)
			}
			cpg.sc.PutIfAbsent(name, k, v, readTxNum)
		}
	}
	return v, step, err
}

func (cpg *cachePopulatingGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	// A warmup worker calls ReadAccountData immediately before ReadAccountCode.
	// The account read provides the code hash, which lets the code read probe the
	// code cache before falling back to the database. This avoids repeated database
	// reads for accounts sharing identical code.
	if cpg.sc != nil && cpg.lastCodeHashKnown && bytes.Equal(addr, cpg.lastCodeAddr[:]) {
		if code, ok := cpg.sc.GetCodeByHash(cpg.lastCodeHash[:]); ok {
			return code, true, nil
		}
	}
	code, _, err := cpg.GetLatest(kv.CodeDomain, addr)
	if err != nil {
		return nil, false, err
	}
	return code, len(code) > 0, nil
}

func (cpg *cachePopulatingGetter) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return cpg.g.HasPrefix(name, prefix)
}

func (cpg *cachePopulatingGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return cpg.g.StepsInFiles(entitySet...)
}

func (bra *BlockReadAheader) AddHeaderAndBody(ctx context.Context, db kv.RoDB, tx kv.Getter, header *types.Header, body *types.Body) {
	blockHash := header.Hash()
	bra.headers.Add(blockHash, header)
	bra.bodies.Add(blockHash, body)
	if db != nil && ctx != nil {
		// Only allow one warmBody to run at a time
		if !bra.warming.CompareAndSwap(false, true) {
			return
		}
		var bal types.BlockAccessList
		balBytes, err := tx.GetOne(kv.BlockAccessList, dbutils.BlockBodyKey(header.Number.Uint64(), blockHash))
		if err != nil {
			log.Warn("[warmBody] failed to read BAL", "blockNum", header.Number.Uint64(), "blockHash", blockHash, "err", err)
		} else if len(balBytes) > 0 {
			bal, err = types.DecodeBlockAccessListBytes(balBytes)
			if err != nil {
				log.Warn("[warmBody] failed to decode BAL", "blockNum", header.Number.Uint64(), "blockHash", blockHash, "err", err)
			}
		}
		bra.warmWg.Go(func() {
			bra.warmBody(ctx, db, body, bal, dbg.ReadAheadWorkers)
		})
		bra.waitForWarmupIfConfigured(ctx)
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

func (bra *BlockReadAheader) waitForWarmupIfConfigured(ctx context.Context) {
	if dbg.ReadAheadWait {
		bra.WaitForWarmup(ctx)
	}
}

func (bra *BlockReadAheader) AddSenders(senders []byte, blockHash common.Hash) {
	if _, ok := bra.bodies.Get(blockHash); !ok {
		return
	}
	bra.senders.Add(blockHash, bytes.Clone(senders))
}

type balWarmupTaskKind uint8

const (
	balWarmAccount balWarmupTaskKind = iota
	balWarmStorageChanges
	balWarmStorageReads
	balWarmBALCode
	balWarmTxnDestination
)

type balCodeWarmupMode uint8

const (
	balCodeWarmupNone balCodeWarmupMode = iota
	balCodeWarmupTxnDestinations
	balCodeWarmupAll
)

type balWarmupTask struct {
	accountIndex int
	kind         balWarmupTaskKind
	slotIndex    int
	address      accounts.Address
}

func balCodeWarmupModeForFlags(warmBALCode, warmTxCode bool) balCodeWarmupMode {
	if warmBALCode {
		return balCodeWarmupAll
	}
	if warmTxCode {
		return balCodeWarmupTxnDestinations
	}
	return balCodeWarmupNone
}

func makeBALWarmupPlan(bal types.BlockAccessList, txns types.Transactions, codeMode balCodeWarmupMode, workers int) ([]balWarmupTask, int) {
	taskCount := len(bal)
	for _, account := range bal {
		taskCount += len(account.StorageChanges) + len(account.StorageReads)
	}
	tasks := make([]balWarmupTask, 0, taskCount+len(bal)+len(txns))
	for accountIndex, account := range bal {
		tasks = append(tasks, balWarmupTask{accountIndex: accountIndex, kind: balWarmAccount})
		for slotIndex := range account.StorageChanges {
			tasks = append(tasks, balWarmupTask{accountIndex: accountIndex, kind: balWarmStorageChanges, slotIndex: slotIndex})
		}
		for slotIndex := range account.StorageReads {
			tasks = append(tasks, balWarmupTask{accountIndex: accountIndex, kind: balWarmStorageReads, slotIndex: slotIndex})
		}
	}
	switch codeMode {
	case balCodeWarmupAll:
		for accountIndex := range bal {
			tasks = append(tasks, balWarmupTask{accountIndex: accountIndex, kind: balWarmBALCode})
		}
	case balCodeWarmupTxnDestinations:
		txnDestinations := make(map[accounts.Address]struct{}, len(txns))
		for _, txn := range txns {
			to := txn.GetTo()
			if to == nil {
				continue
			}
			address := accounts.InternAddress(*to)
			if _, ok := txnDestinations[address]; ok {
				continue
			}
			txnDestinations[address] = struct{}{}
			tasks = append(tasks, balWarmupTask{kind: balWarmTxnDestination, address: address})
		}
	}
	return tasks, min(workers, len(tasks))
}

func (bra *BlockReadAheader) warmBody(ctx context.Context, db kv.RoDB, body *types.Body, bal types.BlockAccessList, workers int) {
	defer bra.warming.Store(false)
	if !dbg.ReadAhead {
		return
	}
	if workers <= 0 {
		workers = 1
	}
	if len(bal) > 0 {
		codeMode := balCodeWarmupModeForFlags(dbg.ReadAheadBALCode, dbg.ReadAheadTxCode)
		bra.warmBAL(ctx, db, bal, body.Transactions, codeMode, workers)
		return
	}
	bra.warmTxns(ctx, db, body.Transactions, workers)
}

func (bra *BlockReadAheader) warmBAL(ctx context.Context, db kv.RoDB, bal types.BlockAccessList, txns types.Transactions, codeMode balCodeWarmupMode, workers int) {
	tasks, balWorkers := makeBALWarmupPlan(bal, txns, codeMode, workers)
	var nextTask atomic.Uint64
	var wg errgroup.Group
	for w := range balWorkers {
		workerID := w
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
			if bra.stateCache != nil {
				getter = newCachePopulatingGetter(ttx, bra.stateCache)
			}
			stateReader := state.NewReaderV3(getter)
			tasksProcessed := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				taskIndex := int(nextTask.Add(1) - 1)
				if taskIndex >= len(tasks) {
					break
				}
				task := tasks[taskIndex]
				switch task.kind {
				case balWarmAccount:
					acctChanges := bal[task.accountIndex]
					stateReader.ReadAccountData(acctChanges.Address)
				case balWarmBALCode:
					acctChanges := bal[task.accountIndex]
					acct, _ := stateReader.ReadAccountData(acctChanges.Address)
					if (acct != nil && !acct.CodeHash.IsEmpty()) || len(acctChanges.CodeChanges) > 0 {
						stateReader.ReadAccountCode(acctChanges.Address)
					}
				case balWarmTxnDestination:
					acct, _ := stateReader.ReadAccountData(task.address)
					if acct != nil && !acct.CodeHash.IsEmpty() {
						stateReader.ReadAccountCode(task.address)
					}
				case balWarmStorageChanges:
					acctChanges := bal[task.accountIndex]
					slot := acctChanges.StorageChanges[task.slotIndex].Slot
					stateReader.ReadAccountStorage(acctChanges.Address, slot)
				case balWarmStorageReads:
					acctChanges := bal[task.accountIndex]
					slot := acctChanges.StorageReads[task.slotIndex]
					stateReader.ReadAccountStorage(acctChanges.Address, slot)
				}
				tasksProcessed++
			}
			log.Debug("[warmBAL] worker finished", "worker", workerID, "tasks", tasksProcessed, "elapsed", time.Since(startTime))
			return nil
		})
	}
	wg.Wait()
}

func (bra *BlockReadAheader) warmTxns(ctx context.Context, db kv.RoDB, txns types.Transactions, workers int) {
	if len(txns) == 0 {
		return
	}
	txnLen := len(txns)
	if workers > txnLen {
		workers = txnLen
	}
	// Pre-divide work: each worker gets a dedicated range of transactions
	txnsPerWorker := (txnLen + workers - 1) / workers
	var wg errgroup.Group
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
			if bra.stateCache != nil {
				getter = newCachePopulatingGetter(ttx, bra.stateCache)
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
						stateReader.ReadAccountCode(to)
					}
				}
				// Warm transaction access list accounts and their code
				for _, entry := range txn.GetAccessList() {
					addr := accounts.InternAddress(entry.Address)
					if acct, _ := stateReader.ReadAccountData(addr); acct != nil && !acct.CodeHash.IsEmpty() {
						stateReader.ReadAccountCode(addr)
					}
					for _, slot := range entry.StorageKeys {
						stateReader.ReadAccountStorage(addr, accounts.InternKey(slot))
					}
				}
			}
			log.Debug("[warmTxns] worker finished", "worker", workerID, "txns", workerEnd-workerStart, "elapsed", time.Since(startTime))
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

func BlocksReadAhead(ctx context.Context, workers int, db kv.RoDB, engine rules.Engine, blockReader dbservices.FullBlockReader) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 500
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for range workers {
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
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, blockNum uint64, engine rules.Engine, blockReader dbservices.FullBlockReader) error {
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
