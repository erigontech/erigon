package exec

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
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
	bals    *lru.Cache[common.Hash, []byte]

	// The single permit belongs either to one warmup or to the code suspending
	// warmup across an unwind. Warmups never wait for it: read-ahead is
	// best-effort, and queued work would be stale by the time an unwind ends.
	warmupGate *semaphore.Weighted

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
	bals, err := lru.New[common.Hash, []byte](4)
	if err != nil {
		panic(err)
	}
	return &BlockReadAheader{
		headers:    headers,
		bodies:     bodies,
		senders:    senders,
		bals:       bals,
		warmupGate: semaphore.NewWeighted(1),
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

// cachePopulatingGetter wraps a kv.TemporalGetter and fills a StateCache
// ReadView as a side effect. Used by warmBody to make read-ahead prefetches
// populate the same in-process cache layer that SharedDomains.GetLatest
// consults — eliminating the file-accessor stack cost on the EVM's first
// touch of any prefetched address.
//
// Code reads also populate the content-addressed and size-cache layers.
type cachePopulatingGetter struct {
	execctxapi.StateGetter
	view     cache.ReadView
	stepSize uint64 // for the read txNum upper bound (last txNum of the read's step)
}

func readAheadGetter(ttx kv.TemporalTx, sc *cache.StateCache) execctxapi.StateGetter {
	getter := execctx.NewTemporalTxStateGetter(ttx)
	if sc == nil {
		return getter
	}
	debug := ttx.Debug()
	stateVersion, err := rawdb.GetStateVersion(ttx)
	if err != nil {
		return getter
	}
	frontier := cache.FrontierWithStateVersion(debug, stateVersion)
	return &cachePopulatingGetter{StateGetter: getter, view: sc.View(frontier), stepSize: debug.StepSize()}
}

func (cpg *cachePopulatingGetter) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	v, step, err := cpg.StateGetter.GetLatest(name, k)
	if err == nil {
		readTxNum := (uint64(step)+1)*cpg.stepSize - 1
		cpg.view.Fill(name, k, v, readTxNum)
		if name == kv.AccountsDomain {
			var codeHash common.Hash
			copy(codeHash[:], accounts.DeserialiseV3CodeHash(v))
			cpg.view.SeedAddrCodeHash(k, codeHash, readTxNum)
		}
	}
	return v, step, err
}

func (cpg *cachePopulatingGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	if code, ok := cpg.view.GetCodeByAddressHash(addr); ok {
		return code, true, nil
	}
	code, _, err := cpg.GetLatest(kv.CodeDomain, addr)
	if err != nil {
		return nil, false, err
	}
	return code, len(code) > 0, nil
}

func (cpg *cachePopulatingGetter) GetCodeSize(addr []byte, txNum uint64) (int, bool, error) {
	code, found, err := cpg.GetCode(addr, txNum)
	return len(code), found, err
}

func (bra *BlockReadAheader) AddHeaderAndBody(ctx context.Context, db kv.RoDB, tx kv.Getter, header *types.Header, body *types.Body) {
	if header == nil || body == nil {
		return
	}
	blockHash := header.Hash()
	bra.headers.Add(blockHash, header)
	bra.bodies.Add(blockHash, body)
	if db == nil || tx == nil || ctx == nil || !dbg.ReadAhead {
		return
	}
	if !bra.warmupGate.TryAcquire(1) {
		return
	}
	var balBytes []byte
	if header.HasNonEmptyBAL() {
		var ok bool
		balBytes, ok = bra.bals.Get(blockHash)
		if !ok {
			var err error
			balBytes, err = rawdb.ReadBlockAccessListBytes(tx, blockHash, header.Number.Uint64())
			balBytes = bytes.Clone(balBytes)
			if err != nil {
				log.Warn("[warmBody] failed to read BAL", "blockNum", header.Number.Uint64(), "blockHash", blockHash, "err", err)
			}
		}
	}
	go func() {
		defer bra.warmupGate.Release(1)
		bra.warmBody(ctx, db, header, body, balBytes, dbg.ReadAheadWorkers)
	}()
}

func (bra *BlockReadAheader) startWarmup(warm func()) bool {
	if !bra.warmupGate.TryAcquire(1) {
		return false
	}
	go func() {
		defer bra.warmupGate.Release(1)
		warm()
	}()
	return true
}

// SuspendWarmup waits for active state-cache warmup and prevents another
// warmup from starting until the returned function is called. Keep it
// suspended while staged unwind state is being read or published. If ctx is
// cancelled first, no suspension remains pending.
func (bra *BlockReadAheader) SuspendWarmup(ctx context.Context) (func(), error) {
	if err := bra.warmupGate.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return sync.OnceFunc(func() { bra.warmupGate.Release(1) }), nil
}

// WaitForWarmup waits until neither a warmup nor a suspension owns the permit,
// or until the context is cancelled. Call it before closing the database to
// avoid waitTxsAllDoneOnClose hangs.
func (bra *BlockReadAheader) WaitForWarmup(ctx context.Context) {
	if err := bra.warmupGate.Acquire(ctx, 1); err == nil {
		bra.warmupGate.Release(1)
	}
}

func (bra *BlockReadAheader) AddSenders(senders []byte, blockHash common.Hash) {
	if _, ok := bra.bodies.Get(blockHash); !ok {
		return
	}
	bra.senders.Add(blockHash, bytes.Clone(senders))
}

func (bra *BlockReadAheader) AddBlockAccessList(blockHash common.Hash, bal []byte) {
	if len(bal) == 0 {
		return
	}
	bra.bals.Add(blockHash, bal)
}

const balWarmupStorageChunkSize = 64

type balCodeWarmupMode uint8

const (
	balCodeWarmupNone balCodeWarmupMode = iota
	balCodeWarmupTxnDestinations
	balCodeWarmupAll
)

type balWarmupTask struct {
	accountIndex uint32
	slotFrom     uint32
	slotTo       uint32
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

func makeBALWarmupPlan(bal types.BlockAccessList, workers int) ([]balWarmupTask, int) {
	taskCount := 0
	for _, account := range bal {
		slots := len(account.StorageChanges) + len(account.StorageReads)
		taskCount += max(1, (slots+balWarmupStorageChunkSize-1)/balWarmupStorageChunkSize)
	}
	tasks := make([]balWarmupTask, 0, taskCount)
	for accountIndex, account := range bal {
		slots := len(account.StorageChanges) + len(account.StorageReads)
		if slots == 0 {
			tasks = append(tasks, balWarmupTask{accountIndex: uint32(accountIndex)})
			continue
		}
		for slotFrom := 0; slotFrom < slots; slotFrom += balWarmupStorageChunkSize {
			tasks = append(tasks, balWarmupTask{accountIndex: uint32(accountIndex), slotFrom: uint32(slotFrom), slotTo: uint32(min(slotFrom+balWarmupStorageChunkSize, slots))})
		}
	}
	return tasks, min(workers, len(tasks))
}

func uniqueTransactionDestinations(txns types.Transactions) map[accounts.Address]struct{} {
	destinations := make(map[accounts.Address]struct{}, len(txns))
	for _, txn := range txns {
		to := txn.GetTo()
		if to == nil {
			continue
		}
		address := accounts.InternAddress(*to)
		destinations[address] = struct{}{}
	}
	return destinations
}

func warmBALStateTask(stateReader *state.ReaderV3, account *types.AccountChanges, task balWarmupTask, codeMode balCodeWarmupMode, txCodeDestinations map[accounts.Address]struct{}) error {
	var accountData *accounts.Account
	if task.slotFrom == 0 {
		var err error
		accountData, err = stateReader.ReadAccountData(account.Address)
		if err != nil {
			return err
		}
	}
	storageChanges := uint32(len(account.StorageChanges))
	for slotIndex := task.slotFrom; slotIndex < task.slotTo; slotIndex++ {
		var slot accounts.StorageKey
		if slotIndex < storageChanges {
			slot = account.StorageChanges[slotIndex].Slot
		} else {
			slot = account.StorageReads[slotIndex-storageChanges]
		}
		if _, _, err := stateReader.ReadAccountStorage(account.Address, slot); err != nil {
			return err
		}
	}
	if task.slotFrom != 0 || codeMode == balCodeWarmupNone {
		return nil
	}
	warmCode := false
	if codeMode == balCodeWarmupAll {
		warmCode = len(account.CodeChanges) > 0 || (accountData != nil && !accountData.CodeHash.IsEmpty())
	} else if _, ok := txCodeDestinations[account.Address]; ok {
		warmCode = accountData != nil && !accountData.CodeHash.IsEmpty()
	}
	if warmCode {
		_, err := stateReader.ReadAccountCode(account.Address)
		return err
	}
	return nil
}

// warmBody warms state for all transactions in a body using multiple workers.
// It reads: To accounts, To account code, To account storage from access lists,
// and block-level access lists. Each worker creates its own transaction.
// AddHeaderAndBody permits only one warmBody at a time; concurrent requests
// skip warming.
func (bra *BlockReadAheader) warmBody(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body, balBytes []byte, workers int) {
	if !dbg.ReadAhead {
		return
	}
	if workers <= 0 {
		workers = 1
	}
	var bal types.BlockAccessList
	if len(balBytes) > 0 {
		var err error
		bal, err = types.DecodeBlockAccessListBytes(balBytes)
		if err != nil {
			log.Warn("[warmBody] failed to decode BAL", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
		}
	}
	if len(bal) > 0 {
		codeMode := balCodeWarmupModeForFlags(dbg.ReadAheadBALCode, dbg.ReadAheadTxCode)
		if err := bra.warmBAL(ctx, db, bal, body.Transactions, codeMode, workers); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("[warmBAL] failed", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
		}
		return
	}
	if err := bra.warmTxns(ctx, db, body.Transactions, workers); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("[warmTxns] failed", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
	}
}

func (bra *BlockReadAheader) warmBAL(ctx context.Context, db kv.RoDB, bal types.BlockAccessList, txns types.Transactions, codeMode balCodeWarmupMode, workers int) error {
	var txCodeDestinations map[accounts.Address]struct{}
	if codeMode == balCodeWarmupTxnDestinations {
		txCodeDestinations = uniqueTransactionDestinations(txns)
	}
	tasks, balWorkers := makeBALWarmupPlan(bal, workers)
	return bra.warmBALState(ctx, db, bal, tasks, codeMode, txCodeDestinations, balWorkers)
}

func (bra *BlockReadAheader) warmBALState(ctx context.Context, db kv.RoDB, bal types.BlockAccessList, tasks []balWarmupTask, codeMode balCodeWarmupMode, txCodeDestinations map[accounts.Address]struct{}, workers int) error {
	var nextTask atomic.Uint64
	wg, workerCtx := errgroup.WithContext(ctx)
	for w := range workers {
		wg.Go(func() error {
			startTime := time.Now()
			tx, err := db.BeginRo(workerCtx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			ttx, ok := tx.(kv.TemporalTx)
			if !ok {
				return errors.New("BAL warmup requires a temporal read transaction")
			}
			stateReader := state.NewReaderV3(readAheadGetter(ttx, bra.stateCache))
			tasksProcessed := 0
			for {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				default:
				}
				taskIndex := int(nextTask.Add(1) - 1)
				if taskIndex >= len(tasks) {
					break
				}
				task := tasks[taskIndex]
				account := bal[task.accountIndex]
				if err := warmBALStateTask(stateReader, account, task, codeMode, txCodeDestinations); err != nil {
					log.Warn("[warmBAL] state task failed", "worker", w, "account", account.Address, "err", err)
				}
				tasksProcessed++
			}
			log.Debug("[warmBAL] state worker finished", "worker", w, "tasks", tasksProcessed, "elapsed", time.Since(startTime))
			return nil
		})
	}
	return wg.Wait()
}

func (bra *BlockReadAheader) warmTxns(ctx context.Context, db kv.RoDB, txns types.Transactions, workers int) error {
	if len(txns) == 0 {
		return nil
	}
	txnLen := len(txns)
	if workers > txnLen {
		workers = txnLen
	}
	// Pre-divide work: each worker gets a dedicated range of transactions
	txnsPerWorker := (txnLen + workers - 1) / workers
	wg, workerCtx := errgroup.WithContext(ctx)
	for w := 0; w < workers; w++ {
		start := w * txnsPerWorker
		end := min(start+txnsPerWorker, txnLen)
		if start >= txnLen {
			break
		}
		wg.Go(func() error {
			startTime := time.Now()
			tx, err := db.BeginRo(workerCtx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			ttx, ok := tx.(kv.TemporalTx)
			if !ok {
				return errors.New("transaction warmup requires a temporal read transaction")
			}
			stateReader := state.NewReaderV3(readAheadGetter(ttx, bra.stateCache))
			for txIdx := start; txIdx < end; txIdx++ {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				default:
				}
				txn := txns[txIdx]
				// Warm To account and its code if it has one
				if toAddr := txn.GetTo(); toAddr != nil {
					to := accounts.InternAddress(*toAddr)
					acct, err := stateReader.ReadAccountData(to)
					if err != nil {
						log.Warn("[warmTxns] account read failed", "worker", w, "tx", txIdx, "address", to, "err", err)
					} else if acct != nil && !acct.CodeHash.IsEmpty() {
						if _, err := stateReader.ReadAccountCode(to); err != nil {
							log.Warn("[warmTxns] code read failed", "worker", w, "tx", txIdx, "address", to, "err", err)
						}
					}
				}
				// Warm transaction access list accounts and their code
				for _, entry := range txn.GetAccessList() {
					addr := accounts.InternAddress(entry.Address)
					acct, err := stateReader.ReadAccountData(addr)
					if err != nil {
						log.Warn("[warmTxns] access-list account read failed", "worker", w, "tx", txIdx, "address", addr, "err", err)
					} else if acct != nil && !acct.CodeHash.IsEmpty() {
						if _, err := stateReader.ReadAccountCode(addr); err != nil {
							log.Warn("[warmTxns] access-list code read failed", "worker", w, "tx", txIdx, "address", addr, "err", err)
						}
					}
					for _, slot := range entry.StorageKeys {
						if _, _, err := stateReader.ReadAccountStorage(addr, accounts.InternKey(slot)); err != nil {
							log.Warn("[warmTxns] access-list storage read failed", "worker", w, "tx", txIdx, "address", addr, "slot", slot, "err", err)
						}
					}
				}
			}
			log.Debug("[warmTxns] worker finished", "worker", w, "txns", end-start, "elapsed", time.Since(startTime))
			return nil
		})
	}
	return wg.Wait()
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
	bal, _ := bra.bals.Get(blockHash)
	return types.NewBlockFromStorage(header.Hash(), header, body.Transactions, body.Uncles, body.Withdrawals, bal), true
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

	stateReader := state.NewReaderV3(execctx.NewTemporalTxStateGetter(ttx))
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
