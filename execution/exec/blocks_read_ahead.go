package exec

import (
	"context"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type blockReadAheader struct {
	// keeps some caches for block themselves
	headers *lru.Cache[common.Hash, *types.Header]
	bodies  *lru.Cache[common.Hash, *types.Body]
	senders *lru.Cache[common.Hash, []byte] // just do raw senders

	// this is for warming state
	warming atomic.Bool // only one warmBody can run at a time
}

func newBlockReadAheader() *blockReadAheader {
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
	return &blockReadAheader{
		headers: headers,
		bodies:  bodies,
		senders: senders,
	}
}

var globalReadAheader = newBlockReadAheader()

func (bra *blockReadAheader) AddHeaderAndBody(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body) {
	blockHash := header.Hash()
	bra.headers.Add(blockHash, header)
	bra.bodies.Add(blockHash, body)
	if db != nil && ctx != nil {
		// Only allow one warmBody to run at a time
		if !bra.warming.CompareAndSwap(false, true) {
			return
		}
		go bra.warmBody(ctx, db, header, body, 8) // use 8 workers for warming
	}
}

func (bra *blockReadAheader) AddSenders(senders []byte, blockHash common.Hash) {
	if _, ok := bra.bodies.Get(blockHash); !ok {
		return
	}
	bra.senders.Add(blockHash, common.Copy(senders))
}

func AddHeaderAndBodyToGlobalReadAheader(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body) {
	globalReadAheader.AddHeaderAndBody(ctx, db, header, body)
}

func AddSendersToGlobalReadAheader(senders []byte, blockHash common.Hash) {
	globalReadAheader.AddSenders(senders, blockHash)
}

// warmBody warms state for all transactions in a body using multiple workers.
// It reads: To accounts, To account code, To account storage from access lists,
// and block-level access lists. Each worker creates its own transaction.
// Only one warmBody can run at a time - concurrent calls are no-ops.
func (bra *blockReadAheader) warmBody(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body, workers int) {
	defer bra.warming.Store(false)

	if workers <= 0 {
		workers = 1
	}

	var wg errgroup.Group

	// If BAL exists in DB, use BAL warming (more complete)
	var bal types.BlockAccessList
	if header != nil && db != nil {
		tx, err := db.BeginRo(ctx)
		if err != nil {
			log.Warn("[warmBody] failed to open tx for BAL", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
		} else {
			data, err := tx.GetOne(kv.BlockAccessList, dbutils.BlockBodyKey(header.Number.Uint64(), header.Hash()))
			if err != nil {
				log.Warn("[warmBody] failed to read BAL", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
			} else if len(data) > 0 {
				bal, err = types.DecodeBlockAccessListBytes(data)
				if err != nil {
					log.Warn("[warmBody] failed to decode BAL", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "err", err)
				}
			}
			tx.Rollback()
		}
	}

	balLen := len(bal)
	if balLen > 0 {
		balWorkers := workers
		if balWorkers > balLen {
			balWorkers = balLen
		}

		// Pre-divide work: each worker gets a dedicated range of BAL entries
		entriesPerWorker := (balLen + balWorkers - 1) / balWorkers

		for w := 0; w < balWorkers; w++ {
			start := w * entriesPerWorker
			end := start + entriesPerWorker
			if end > balLen {
				end = balLen
			}
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
				stateReader := state.NewReaderV3(ttx)

				for idx := workerStart; idx < workerEnd; idx++ {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					acctChanges := bal[idx]
					acct, _ := stateReader.ReadAccountData(acctChanges.Address)
					// Warm code if account has code or if there are code changes
					if (acct != nil && !acct.CodeHash.IsEmpty()) || len(acctChanges.CodeChanges) > 0 {
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
		end := start + txnsPerWorker
		if end > txnLen {
			end = txnLen
		}
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
			stateReader := state.NewReaderV3(ttx)

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
			log.Debug("[warmBody] TX worker finished", "worker", workerID, "txns", workerEnd-workerStart, "elapsed", time.Since(startTime))
			return nil
		})
	}

	wg.Wait()
}

func WarmBodyFromGlobalReadAheader(ctx context.Context, db kv.RoDB, header *types.Header, body *types.Body, workers int) {
	globalReadAheader.warmBody(ctx, db, header, body, workers)
}

func ReadBodyWithTransactionsFromGlobalReadAheader(blockHash common.Hash) (*types.Body, bool) {
	return globalReadAheader.bodies.Get(blockHash)
}

func ReadBlockWithSendersFromGlobalReadAheader(blockHash common.Hash) (*types.Block, bool) {
	header, ok := globalReadAheader.headers.Get(blockHash)
	if header == nil || !ok {
		return nil, false
	}
	body, ok := globalReadAheader.bodies.Get(blockHash)
	if body == nil || !ok {
		return nil, false
	}
	senders, ok := globalReadAheader.senders.Get(blockHash)
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
	const readAheadBlocks = 100
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
