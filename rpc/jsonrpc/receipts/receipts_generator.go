package receipts

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/rpc/rpchelper"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/aa"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc/transactions"
)

type Generator struct {
	stateCache    kvcache.Cache
	receiptsCache *lru.Cache[common.Hash, types.Receipts]
	receiptCache  *lru.Cache[common.Hash, *types.Receipt]

	// blockExecMutex ensuring that only 1 block with given hash
	// executed at a time - all parallel requests for same hash will wait for results
	// "Requesting near-chain-tip block receipts" - is very common RPC request, means we're facing many similar parallel requests
	blockExecMutex *loaderMutex[common.Hash]
	txnExecMutex   *loaderMutex[common.Hash] // only 1 txn with current hash executed at a time - same parallel requests are waiting for results

	receiptsCacheTrace bool
	receiptCacheTrace  bool
	evmTimeout         time.Duration

	blockReader services.FullBlockReader
	txNumReader rawdbv3.TxNumsReader
	engine      rules.EngineReader

	commitmentReplay *rpchelper.CommitmentReplay
}

type ReceiptEnv struct {
	ibs        *state.IntraBlockState
	gasUsed    *protocol.GasUsed
	gp         *protocol.GasPool
	noopWriter *state.NoopWriter
	getHeader  func(hash common.Hash, number uint64) (*types.Header, error)
	header     *types.Header
}

var (
	receiptsCacheLimit = dbg.EnvInt("R_LRU", 1024) //ethmainnet: 1K receipts is ~200mb RAM
	receiptsCacheTrace = dbg.EnvBool("R_LRU_TRACE", false)
)

func NewGenerator(dirs datadir.Dirs, blockReader services.FullBlockReader, engine rules.EngineReader, stateCache kvcache.Cache, evmTimeout time.Duration) *Generator {
	receiptsCache, err := lru.New[common.Hash, types.Receipts](receiptsCacheLimit) //TODO: is handling both of them a good idea though...?
	if err != nil {
		panic(err)
	}

	receiptCache, err := lru.New[common.Hash, *types.Receipt](receiptsCacheLimit * 100) // think they should be connected in some of that way
	if err != nil {
		panic(err)
	}

	txNumReader := blockReader.TxnumReader()

	return &Generator{
		stateCache:         stateCache,
		receiptsCache:      receiptsCache,
		blockReader:        blockReader,
		txNumReader:        txNumReader,
		engine:             engine,
		receiptsCacheTrace: receiptsCacheTrace,
		receiptCacheTrace:  receiptsCacheTrace,
		receiptCache:       receiptCache,
		evmTimeout:         evmTimeout,

		blockExecMutex: &loaderMutex[common.Hash]{},
		txnExecMutex:   &loaderMutex[common.Hash]{},

		commitmentReplay: rpchelper.NewCommitmentReplay(dirs, txNumReader, log.Root()),
	}
}

func (g *Generator) LogStats() {
	if g == nil || !g.receiptsCacheTrace {
		return
	}
}

func (g *Generator) GetCachedReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, bool) {
	return g.receiptsCache.Get(blockHash)
}

func (g *Generator) GetCachedReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, bool) {
	return g.receiptCache.Get(hash)
}

var rpcDisableRCache = dbg.EnvBool("RPC_DISABLE_RCACHE", false)
var rpcDisableRLRU = dbg.EnvBool("RPC_DISABLE_RLRU", false)

func (g *Generator) PrepareEnv(ctx context.Context, header *types.Header, cfg *chain.Config, tx kv.TemporalTx, txIndex int) (*ReceiptEnv, error) {
	txNumsReader := g.blockReader.TxnumReader()
	ibs, _, _, _, _, err := transactions.ComputeBlockContext(ctx, g.engine, header, cfg, g.blockReader, g.stateCache, txNumsReader, tx, txIndex)

	if err != nil {
		return nil, fmt.Errorf("ReceiptsGen: PrepareEnv: bn=%d, %w", header.Number.Uint64(), err)
	}

	gasUsed := new(protocol.GasUsed)
	gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(cfg.GetMaxBlobGasPerBlock(header.Time, 0))

	noopWriter := state.NewNoopWriter()

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		h, e := g.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h, e
	}
	return &ReceiptEnv{
		ibs:        ibs,
		gasUsed:    gasUsed,
		gp:         gp,
		noopWriter: noopWriter,
		getHeader:  getHeader,
		header:     header,
	}, nil
}

func (g *Generator) addToCacheReceipts(header *types.Header, receipts types.Receipts) {
	if rpcDisableRLRU {
		return
	}
	//g.receiptsCache.Add(header.Hash(), receipts.Copy()) // .Copy() helps pprof to attribute memory to cache - instead of evm (where it was allocated). but 5% perf
	g.receiptsCache.Add(header.Hash(), receipts)
}

func (g *Generator) addToCacheReceipt(hash common.Hash, receipt *types.Receipt) {
	if rpcDisableRLRU {
		return
	}
	//g.receiptCache.Add(hash, receipt.Copy()) // .Copy() helps pprof to attribute memory to cache - instead of evm (where it was allocated). but 5% perf
	g.receiptCache.Add(hash, receipt)
}

type PostStateInfo struct {
	Txns              types.Transactions
	CommitmentHistory bool
}

func (g *Generator) GetReceipt(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, header *types.Header, txn types.Transaction, index int, txNum uint64, postState *PostStateInfo) (_ *types.Receipt, err error) {
	blockHash := header.Hash()
	blockNum := header.Number.Uint64()
	txnHash := txn.Hash()

	calculatePostState := postState != nil

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	var receiptFromDB, receipt *types.Receipt
	var firstLogIndex, logIdxAfterTx uint32
	var cumGasUsed uint64

	defer func() {
		if dbg.Enabled(ctx) {
			log.Info("[dbg] ReceiptGenerator.GetReceipt",
				"txNum", txNum,
				"txHash", txnHash.String(),
				"blockNum", blockNum,
				"firstLogIndex", firstLogIndex,
				"logIdxAfterTx", logIdxAfterTx,
				"nil receipt in db", receiptFromDB == nil,
				"err", err)
		}
	}()

	if receipts, ok := g.receiptsCache.Get(blockHash); ok && len(receipts) > index {
		return receipts[index], nil
	}

	mu := g.txnExecMutex.lock(txnHash)
	defer g.txnExecMutex.unlock(mu, txnHash)
	if receipt, ok := g.receiptCache.Get(txnHash); ok {
		if receipt.BlockHash == blockHash && // elegant way to handle reorgs
			calculatePostState == (len(receipt.PostState) != 0) { // verify if the expected postState matches the actual postState on cache. Otherwise re-calculate it
			return receipt, nil
		}

		g.receiptCache.Remove(txnHash) // remove old receipt with same hash, but different blockHash OR different postState
	}

	// Now the snapshot have not the `postState` field. Therefore, for pre-Byzantium blocks,
	// we must skip persistent receipts and re-calculate
	// If/when receipts are saved to the DB *with* the `postState` field,
	// this check on `calculatePostState` should be removed to utilize the stored receipt.
	if !rpcDisableRCache && !calculatePostState {
		var ok bool
		var err error
		receiptFromDB, ok, err = rawdb.ReadReceiptCacheV2(tx, rawdb.RCacheV2Query{
			TxNum:     txNum,
			BlockNum:  blockNum,
			BlockHash: blockHash,
			TxnHash:   txnHash,
		})
		if err != nil {
			return nil, err
		}
		if ok && receiptFromDB != nil && !dbg.AssertEnabled {
			g.addToCacheReceipt(txnHash, receiptFromDB)
			return receiptFromDB, nil
		}
	}

	var evm *vm.EVM
	var genEnv *ReceiptEnv

	err = rpchelper.CheckBlockExecuted(tx, blockNum)
	if err != nil {
		return nil, err
	}

	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(tx, txNum+1)
	if err != nil {
		return nil, err
	}

	if txn.Type() == types.AccountAbstractionTxType {
		genEnv, err = g.PrepareEnv(ctx, header, cfg, tx, index)
		if err != nil {
			return nil, err
		}

		aaTxn := txn.(*types.AccountAbstractionTransaction)
		blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, accounts.NilAddress, cfg)
		evm = vm.NewEVM(blockContext, evmtypes.TxContext{}, genEnv.ibs, cfg, vm.Config{})
		paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, genEnv.ibs, genEnv.gp, header, evm, cfg)
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(ctx, g.evmTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			evm.Cancel()
		}()

		status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, paymasterContext, validationGasUsed, genEnv.gp, evm, header, genEnv.ibs)
		if err != nil {
			return nil, err
		}

		logs := genEnv.ibs.GetLogs(genEnv.ibs.TxnIndex(), txn.Hash(), header.Number.Uint64(), header.Hash())
		receipt = aa.CreateAAReceipt(txn.Hash(), status, gasUsed, header.GasUsed, header.Number.Uint64(), uint64(genEnv.ibs.TxnIndex()), logs)
	} else {
		var sharedDomains *execctx.SharedDomains
		defer func() {
			if sharedDomains != nil {
				sharedDomains.Close()
			}
		}()

		var stateWriter state.StateWriter

		if calculatePostState && postState.CommitmentHistory {
			sharedDomains, err = execctx.NewSharedDomains(ctx, tx, log.Root())
			if err != nil {
				return nil, err
			}
			sharedDomains.GetCommitmentContext().SetDeferBranchUpdates(false)

			genEnv, err = g.PrepareEnv(ctx, header, cfg, tx, 0)
			if err != nil {
				return nil, err
			}

			minTxNum, err := g.txNumReader.Min(ctx, tx, blockNum)
			if err != nil {
				return nil, err
			}

			// commitment is indexed by txNum of the first tx (system-tx) of the block
			sharedDomains.GetCommitmentContext().SetHistoryStateReader(tx, minTxNum)
			latestTxNum, _, err := sharedDomains.SeekCommitment(ctx, tx)
			if err != nil {
				return nil, err
			}
			stateWriter = state.NewWriter(sharedDomains.AsPutDel(tx), nil, latestTxNum)

			evm = protocol.CreateEVM(cfg, protocol.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, accounts.NilAddress, genEnv.ibs, genEnv.header, vm.Config{})
			ctx, cancel := context.WithTimeout(ctx, g.evmTimeout)
			defer cancel()
			go func() {
				<-ctx.Done()
				evm.Cancel()
			}()

			// re-run previous txs of the blocks
			for txnIndex := 0; txnIndex < index; txnIndex++ {
				currTxn := postState.Txns[txnIndex]

				genEnv.ibs.SetTxContext(blockNum, txnIndex)
				_, err := protocol.ApplyTransactionWithEVM(cfg, g.engine, genEnv.gp, genEnv.ibs, stateWriter, genEnv.header, currTxn, genEnv.gasUsed, vm.Config{}, evm)
				if err != nil {
					return nil, fmt.Errorf("ReceiptGen.GetReceipts: bn=%d, txnIdx=%d, %w", blockNum, txnIndex, err)
				}
				if evm.Cancelled() {
					return nil, fmt.Errorf("execution aborted (timeout = %v)", g.evmTimeout)
				}

				if err := genEnv.ibs.CommitBlock(evm.ChainRules(), stateWriter); err != nil {
					return nil, fmt.Errorf("CommitBlock failed: %w", err)
				}
			}

			genEnv.ibs.SetTxContext(blockNum, index)
		} else {
			genEnv, err = g.PrepareEnv(ctx, header, cfg, tx, index)
			if err != nil {
				return nil, err
			}
			stateWriter = genEnv.noopWriter
		}

		evm = protocol.CreateEVM(cfg, protocol.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, accounts.NilAddress, genEnv.ibs, genEnv.header, vm.Config{})
		ctx, cancel := context.WithTimeout(ctx, g.evmTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			evm.Cancel()
		}()

		receipt, err = protocol.ApplyTransactionWithEVM(cfg, g.engine, genEnv.gp, genEnv.ibs, stateWriter, genEnv.header, txn, genEnv.gasUsed, vm.Config{}, evm)
		if err != nil {
			return nil, fmt.Errorf("ReceiptGen.GetReceipt: bn=%d, txnIdx=%d, %w", blockNum, index, err)
		}

		if calculatePostState {
			if err := genEnv.ibs.CommitBlock(evm.ChainRules(), stateWriter); err != nil {
				return nil, fmt.Errorf("CommitBlock failed: %w", err)
			}

			// calculate state root after tx identified by txNum (txNim+1)
			var stateRoot []byte
			if postState.CommitmentHistory {
				sharedDomains.GetCommitmentContext().SetHistoryStateReader(tx, txNum+1)
				stateRoot, err = sharedDomains.ComputeCommitment(ctx, tx, false, blockNum, sharedDomains.TxNum(), "getReceipt", nil)
				if err != nil {
					return nil, err
				}
			} else { // use state history to compute commitment
				stateRoot, err = g.computeCommitmentFromStateHistory(ctx, tx, blockNum, txNum)
				if err != nil {
					return nil, err
				}
			}
			receipt.PostState = stateRoot
		}
	}

	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", g.evmTimeout)
	}

	if rawtemporaldb.ReceiptStoresFirstLogIdx(tx) {
		firstLogIndex = logIdxAfterTx
	} else {
		firstLogIndex = logIdxAfterTx - uint32(len(receipt.Logs))
	}
	receipt.BlockHash = blockHash
	receipt.CumulativeGasUsed = cumGasUsed
	receipt.TransactionIndex = uint(index)
	receipt.FirstLogIndexWithinBlock = firstLogIndex

	for i := range receipt.Logs {
		receipt.Logs[i].TxIndex = uint(index)
		receipt.Logs[i].Index = uint(firstLogIndex + uint32(i))
	}

	g.addToCacheReceipt(txnHash, receipt)

	if dbg.AssertEnabled && receiptFromDB != nil {
		g.assertEqualReceipts(receipt, receiptFromDB)
	}
	return receipt, nil
}

func (g *Generator) GetReceipts(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, block *types.Block) (_ types.Receipts, err error) {
	blockHash := block.Hash()
	blockNum := block.NumberU64()

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	var receiptsFromDB types.Receipts
	receipts := make(types.Receipts, len(block.Transactions()))
	defer func() {
		if dbg.Enabled(ctx) {
			log.Info("[dbg] ReceiptGenerator.GetReceipts",
				"blockNum", blockNum,
				"nil receipts in db", receiptsFromDB == nil)
		}
	}()

	mu := g.blockExecMutex.lock(blockHash) // parallel requests of same blockNum will executed only once
	defer g.blockExecMutex.unlock(mu, blockHash)
	if receipts, ok := g.receiptsCache.Get(blockHash); ok {
		return receipts, nil
	}

	err = rpchelper.CheckBlockExecuted(tx, blockNum)
	if err != nil {
		return nil, err
	}

	// Check if we have commitment history: this is required to know if state root will be computed or left zero for historical state.
	var commitmentHistory bool
	commitmentHistory, _, err = rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}
	calculatePostState := (commitmentHistory || g.blockReader.FrozenBlocks() == 0) && !cfg.IsByzantium(blockNum)

	// Now the snapshot have not the `postState` field. Therefore, for pre-Byzantium blocks,
	// we must skip persistent receipts and re-calculate
	// If/when receipts are saved to the DB *with* the `postState` field,
	// this check on `calculatePostState` should be removed to utilize the stored receipt.
	if !rpcDisableRCache && !calculatePostState {
		receiptsFromDB, err = rawdb.ReadReceiptsCacheV2(tx, block, g.txNumReader)
		if err != nil {
			return nil, err
		}
		if len(receiptsFromDB) > 0 && !dbg.AssertEnabled {
			g.addToCacheReceipts(block.HeaderNoCopy(), receiptsFromDB)
			return receiptsFromDB, nil
		}
	}

	var genEnv *ReceiptEnv
	genEnv, err = g.PrepareEnv(ctx, block.HeaderNoCopy(), cfg, tx, 0)
	if err != nil {
		return nil, err
	}
	//genEnv.ibs.SetTrace(true)
	vmCfg := vm.Config{}
	hashFn := protocol.GetHashFn(genEnv.header, genEnv.getHeader)
	ctx, cancel := context.WithTimeout(ctx, g.evmTimeout)
	defer cancel()

	var sharedDomains *execctx.SharedDomains
	defer func() {
		if sharedDomains != nil {
			sharedDomains.Close()
		}
	}()

	minTxNum, err := g.txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	var stateWriter state.StateWriter
	if calculatePostState && commitmentHistory {
		sharedDomains, err = execctx.NewSharedDomains(ctx, tx, log.Root())
		if err != nil {
			return nil, err
		}
		sharedDomains.GetCommitmentContext().SetDeferBranchUpdates(false)
		// commitment are indexed by txNum of the first tx (system-tx) of the block
		sharedDomains.GetCommitmentContext().SetHistoryStateReader(tx, minTxNum)
		latestTxNum, _, err := sharedDomains.SeekCommitment(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateWriter = state.NewWriter(sharedDomains.AsPutDel(tx), nil, latestTxNum)
	} else {
		stateWriter = genEnv.noopWriter
	}

	for i, txn := range block.Transactions() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		evm := protocol.CreateEVM(cfg, hashFn, g.engine, accounts.NilAddress, genEnv.ibs, genEnv.header, vmCfg)
		go func() {
			<-ctx.Done()
			evm.Cancel()
		}()

		genEnv.ibs.SetTxContext(blockNum, i)
		receipt, err := protocol.ApplyTransactionWithEVM(cfg, g.engine, genEnv.gp, genEnv.ibs, stateWriter, genEnv.header, txn, genEnv.gasUsed, vmCfg, evm)
		if err != nil {
			return nil, fmt.Errorf("ReceiptGen.GetReceipts: bn=%d, txnIdx=%d, %w", block.NumberU64(), i, err)
		}
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted (timeout = %v)", g.evmTimeout)
		}

		if calculatePostState {
			txNum := minTxNum + 1 + uint64(i)

			if err := genEnv.ibs.CommitBlock(evm.ChainRules(), stateWriter); err != nil {
				return nil, fmt.Errorf("CommitBlock failed: %w", err)
			}

			// calculate state root after tx identified by txNum (txNim+1)
			var stateRoot []byte
			if commitmentHistory {
				sharedDomains.GetCommitmentContext().SetHistoryStateReader(tx, txNum+1)
				stateRoot, err = sharedDomains.ComputeCommitment(ctx, tx, false, blockNum, sharedDomains.TxNum(), "getReceipts", nil)
				if err != nil {
					return nil, err
				}
			} else { // use state history to compute commitment
				stateRoot, err = g.computeCommitmentFromStateHistory(ctx, tx, blockNum, txNum)
				if err != nil {
					return nil, err
				}
			}
			receipt.PostState = stateRoot
		}

		receipt.BlockHash = blockHash
		if len(receipt.Logs) > 0 {
			receipt.FirstLogIndexWithinBlock = uint32(receipt.Logs[0].Index)
		} else if i > 0 {
			receipt.FirstLogIndexWithinBlock = receipts[i-1].FirstLogIndexWithinBlock + uint32(len(receipts[i-1].Logs))
		}
		receipts[i] = receipt

		if dbg.AssertEnabled && receiptsFromDB != nil && i < len(receiptsFromDB) {
			g.assertEqualReceipts(receipt, receiptsFromDB[i])
		}
	}

	// When assertions are enabled, receipts are *always* computed (i.e. receipt cache V2 is skipped)
	// Hence, we need commitment history to correctly compute the `root` field for pre-Byzantium receipts
	if dbg.AssertEnabled && (commitmentHistory || cfg.IsByzantium(blockNum)) {
		computedReceiptsRoot := types.DeriveSha(receipts)
		blockReceiptsRoot := block.Header().ReceiptHash
		if computedReceiptsRoot != blockReceiptsRoot {
			panic(fmt.Sprintf("assert: computedReceiptsRoot=%s, blockReceiptsRoot=%s", computedReceiptsRoot.Hex(), blockReceiptsRoot.Hex()))
		}
	}

	g.addToCacheReceipts(block.HeaderNoCopy(), receipts)
	return receipts, nil
}

func (g *Generator) assertEqualReceipts(fromExecution, fromDB *types.Receipt) {
	toJson := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		return string(b)
	}

	generated := fromExecution.Copy()
	if generated.TransactionIndex != fromDB.TransactionIndex {
		panic(fmt.Sprintf("assert: txn index mismatch: %d, %d", generated.TransactionIndex, fromDB.TransactionIndex))
	}
	if generated.FirstLogIndexWithinBlock != fromDB.FirstLogIndexWithinBlock {
		panic(fmt.Sprintf("assert: first log index mismatch: %d, %d", generated.FirstLogIndexWithinBlock, fromDB.FirstLogIndexWithinBlock))
	}
	if len(generated.Logs) != len(fromDB.Logs) {
		panic(fmt.Sprintf("assert: logs length mismatch: generated=%d, fromDB=%d, bn=%d, txnIdx=%d",
			len(generated.Logs), len(fromDB.Logs), generated.BlockNumber.Uint64(), generated.TransactionIndex))
	}

	for i := range generated.Logs {
		a := toJson(generated.Logs[i])
		b := toJson(fromDB.Logs[i])
		if a != b {
			panic(fmt.Sprintf("assert: %v, bn=%d, txnIdx=%d", cmp.Diff(a, b), generated.BlockNumber.Uint64(), generated.TransactionIndex))
		}
	}
	fromDB.Logs, generated.Logs = nil, nil
	fromDB.Bloom, generated.Bloom = types.Bloom{}, types.Bloom{}
	a := toJson(generated)
	b := toJson(fromDB)
	if a != b {
		panic(fmt.Sprintf("assert: %v, bn=%d, txnIdx=%d", cmp.Diff(a, b), generated.BlockNumber.Uint64(), generated.TransactionIndex))
	}
}

func (g *Generator) GetReceiptsGasUsed(ctx context.Context, tx kv.TemporalTx, block *types.Block, txNumsReader rawdbv3.TxNumsReader) (types.Receipts, error) {
	if receipts, ok := g.receiptsCache.Get(block.Hash()); ok {
		return receipts, nil
	}

	startTxNum, err := txNumsReader.Min(ctx, tx, block.NumberU64())
	if err != nil {
		return nil, err
	}

	receipts := make(types.Receipts, len(block.Transactions()))

	var prevCumGasUsed uint64
	currentTxNum := startTxNum + 1
	for i := range block.Transactions() {
		receipt := &types.Receipt{}
		cumGasUsed, _, _, err := rawtemporaldb.ReceiptAsOf(tx, currentTxNum+1)
		if err != nil {
			return nil, fmt.Errorf("ReceiptGen.GetReceiptsGasUsed: at tx %d (block %d, index %d): %w",
				currentTxNum, block.NumberU64(), i, err)
		}

		receipt.GasUsed = cumGasUsed - prevCumGasUsed
		receipts[i] = receipt

		prevCumGasUsed = cumGasUsed
		currentTxNum++
	}

	return receipts, nil
}

type loaderMutex[K comparable] struct {
	sync.Map
}

func (m *loaderMutex[K]) lock(key K) *sync.Mutex {
	value, _ := m.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu
}

func (m *loaderMutex[K]) unlock(mu *sync.Mutex, key K) {
	mu.Unlock()
	m.Delete(key)
}

func (g *Generator) computeCommitmentFromStateHistory(ctx context.Context, tx kv.TemporalTx, blockNum uint64, txNum uint64) ([]byte, error) {
	receiptComputeCommitment := func(ctx context.Context, ttx kv.TemporalTx, tsd *execctx.SharedDomains) ([]byte, error) {
		tsd.GetCommitmentCtx().SetStateReader(rpchelper.NewCommitmentReplayStateReader(ttx, tx, tsd, txNum+1))
		minTxNum, err := g.txNumReader.Min(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		log.Debug("Touch historical keys", "fromTxNum", minTxNum, "toTxNum", txNum+1)
		_, _, err = tsd.TouchChangedKeysFromHistory(tx, minTxNum, txNum+1)
		if err != nil {
			return nil, err
		}
		root, err := tsd.ComputeCommitment(ctx, ttx, false, blockNum, txNum, "commitment-from-history", nil)
		if err != nil {
			return nil, err
		}
		log.Debug("Historical state", "blockNum", blockNum, "txNum", txNum, "root", common.Bytes2Hex(root))
		return root, nil
	}
	if dbg.AssertEnabled && blockNum == 0 {
		panic("assert: blockNum=0 in computeCommitmentFromStateHistory")
	}
	baseBlockNum := blockNum - 1
	return g.commitmentReplay.ComputeCustomCommitmentFromStateHistory(ctx, tx, baseBlockNum, receiptComputeCommitment)
}
