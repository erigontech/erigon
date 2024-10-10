package jsonrpc

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

type OverlayAPI interface {
	GetLogs(ctx context.Context, crit filters.FilterCriteria, stateOverride *ethapi.StateOverrides) ([]*types.Log, error)
	CallConstructor(ctx context.Context, address common.Address, code *hexutility.Bytes) (*CreationCode, error)
}

// OverlayAPIImpl is implementation of the OverlayAPIImpl interface based on remote Db access
type OverlayAPIImpl struct {
	*BaseAPI
	db                        kv.RoDB
	GasCap                    uint64
	OverlayGetLogsTimeout     time.Duration
	OverlayReplayBlockTimeout time.Duration
	OtsAPI                    OtterscanAPI
}

type CreationCode struct {
	Code *hexutility.Bytes `json:"code"`
}

type blockReplayTask struct {
	idx         int
	BlockNumber int64
}

type blockReplayResult struct {
	BlockNumber int64        `json:"block_number"`
	Logs        []*types.Log `json:"logs,omitempty"`
	Error       string       `json:"error,omitempty"`
}

// NewOverlayAPI returns OverlayAPIImpl instance
func NewOverlayAPI(base *BaseAPI, db kv.RoDB, gascap uint64, overlayGetLogsTimeout time.Duration, overlayReplayBlockTimeout time.Duration, otsApi OtterscanAPI) *OverlayAPIImpl {
	return &OverlayAPIImpl{
		BaseAPI:                   base,
		db:                        db,
		GasCap:                    gascap,
		OverlayGetLogsTimeout:     overlayGetLogsTimeout,
		OverlayReplayBlockTimeout: overlayReplayBlockTimeout,
		OtsAPI:                    otsApi,
	}
}

func (api *OverlayAPIImpl) CallConstructor(ctx context.Context, address common.Address, code *hexutility.Bytes) (*CreationCode, error) {
	var (
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
		baseFee            uint256.Int
	)

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	defer func(start time.Time) { log.Trace("CallConstructor finished", "runtime", time.Since(start)) }(time.Now())

	creationData, err := api.OtsAPI.GetContractCreator(ctx, address)
	if err != nil {
		return nil, err
	}

	blockNum, ok, err := api.txnLookup(ctx, tx, creationData.Tx)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("contract construction tx not found")
	}

	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		return nil, err
	}

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	// -1 is a default value for transaction index.
	// If it's -1, we will try to replay every single transaction in that block
	transactionIndex := -1
	for idx, transaction := range block.Transactions() {
		if transaction.Hash() == creationData.Tx {
			transactionIndex = idx
			break
		}
	}

	if transactionIndex == -1 {
		return nil, fmt.Errorf("could not find tx hash")
	}

	replayTransactions = block.Transactions()[:transactionIndex]

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
	if err != nil {
		return nil, err
	}

	statedb := state.New(stateReader)

	parent := block.Header()

	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNum, block.Hash())
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := api._blockReader.CanonicalHash(ctx, tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	if parent.BaseFee != nil {
		baseFee.SetFromBig(parent.BaseFee)
	}

	blockCtx = evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     getHash,
		Coinbase:    parent.Coinbase,
		BlockNumber: parent.Number.Uint64(),
		Time:        parent.Time,
		Difficulty:  new(big.Int).Set(parent.Difficulty),
		GasLimit:    parent.GasLimit,
		BaseFee:     &baseFee,
	}

	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, statedb, chainConfig, vm.Config{Debug: false})
	signer := types.MakeSigner(chainConfig, blockNum, block.Time())
	rules := chainConfig.Rules(blockNum, blockCtx.Time)

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		statedb.SetTxContext(txn.Hash(), block.Hash(), idx)
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			return nil, err
		}
		txCtx = core.NewEVMTxContext(msg)
		evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
		// Execute the transaction message
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		_ = statedb.FinalizeTx(rules, state.NewNoopWriter())

	}

	creationTx := block.Transactions()[transactionIndex]
	statedb.SetTxContext(creationTx.Hash(), block.Hash(), transactionIndex)

	// CREATE2: keep original message so we match the existing contract address, code will be replaced later
	msg, err := creationTx.AsMessage(*signer, block.BaseFee(), rules)
	if err != nil {
		return nil, err
	}

	contractAddr := crypto.CreateAddress(msg.From(), msg.Nonce())
	if creationTx.GetTo() == nil && contractAddr == address {
		// CREATE: adapt message with new code so it's replaced instantly
		msg = types.NewMessage(msg.From(), msg.To(), msg.Nonce(), msg.Value(), api.GasCap, msg.GasPrice(), msg.FeeCap(), msg.Tip(), *code, msg.AccessList(), msg.CheckNonce(), msg.IsFree(), msg.MaxFeePerBlobGas())
	} else {
		msg.ChangeGas(api.GasCap, api.GasCap)
	}
	txCtx = core.NewEVMTxContext(msg)
	ct := OverlayCreateTracer{contractAddress: address, code: *code, gasCap: api.GasCap}
	evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: true, Tracer: &ct})

	// Execute the transaction message
	_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
	if ct.err != nil {
		return nil, err
	}

	resultCode := &CreationCode{}
	if ct.resultCode != nil && len(ct.resultCode) > 0 {
		c := hexutility.Bytes(ct.resultCode)
		resultCode.Code = &c
		return resultCode, nil
	} else {
		// err from core.ApplyMessage()
		if err != nil {
			return nil, err
		}
		code := evm.IntraBlockState().GetCode(address)
		if len(code) > 0 {
			c := hexutility.Bytes(code)
			resultCode.Code = &c
			return resultCode, nil
		}
	}

	_ = statedb.FinalizeTx(rules, state.NewNoopWriter())

	return nil, nil
}

func (api *OverlayAPIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria, stateOverride *ethapi.StateOverrides) ([]*types.Log, error) {
	timeout := api.OverlayGetLogsTimeout
	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	defer func(start time.Time) { log.Trace("Executing overlay_getLogs finished", "runtime", time.Since(start)) }(time.Now())

	begin, end, err := getBeginEnd(ctx, tx, api, crit)
	if err != nil {
		return nil, err
	}

	numBlocks := end - begin + 1
	var (
		results = make([]*blockReplayResult, numBlocks)
		pend    sync.WaitGroup
	)

	threads := runtime.NumCPU()
	if threads > int(numBlocks) {
		threads = int(numBlocks)
	}
	jobs := make(chan *blockReplayTask, threads)
	for th := 0; th < threads; th++ {
		pend.Add(1)
		go func() {
			defer pend.Done()
			tx, err := api.db.BeginRo(ctx)
			if err != nil {
				log.Error("Error", "error", err.Error())
				return
			}
			defer tx.Rollback()
			for task := range jobs {
				blockNumber := task.BlockNumber
				if err := ctx.Err(); err != nil {
					results[task.idx] = &blockReplayResult{BlockNumber: task.BlockNumber, Error: err.Error()}
					continue
				}

				// try to recompute the state
				stateReader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber-1)), 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
				if err != nil {
					results[task.idx] = &blockReplayResult{BlockNumber: task.BlockNumber, Error: err.Error()}
					continue
				}
				statedb := state.New(stateReader)

				if stateOverride != nil {
					err = stateOverride.Override(statedb)
					if err != nil {
						results[task.idx] = &blockReplayResult{BlockNumber: task.BlockNumber, Error: err.Error()}
						continue
					}
				}
				blockLogs, err := api.replayBlock(ctx, uint64(blockNumber), statedb, chainConfig, tx)
				if err != nil {
					results[task.idx] = &blockReplayResult{BlockNumber: task.BlockNumber, Error: err.Error()}
					continue
				}
				log.Debug("[GetLogs]", "len(blockLogs)", len(blockLogs))
				logs := filterLogs(blockLogs, crit.Addresses, crit.Topics)
				log.Debug("[GetLogs]", "len(logs)", len(logs))

				results[task.idx] = &blockReplayResult{BlockNumber: task.BlockNumber, Logs: logs}
			}
		}()
	}

	hasOverrides := false
	allBlocks := roaring64.New()
	for overlayAddress := range *stateOverride {
		hasOverrides = true
		fromB, err := bitmapdb.Get64(tx, kv.CallFromIndex, overlayAddress.Bytes(), begin, end+1)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		toB, err := bitmapdb.Get64(tx, kv.CallToIndex, overlayAddress.Bytes(), begin, end+1)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		allBlocks.Or(fromB)
		allBlocks.Or(toB)
	}

	var failed error
	idx := 0
blockLoop:
	for blockNumber := begin; blockNumber <= end; blockNumber++ {
		if hasOverrides && !allBlocks.Contains(blockNumber) {
			log.Debug("skipping untouched blocked", "blockNumber", blockNumber)
			continue
		}

		// Send the trace task over for execution
		task := &blockReplayTask{idx: idx, BlockNumber: int64(blockNumber)}
		select {
		case <-ctx.Done():
			failed = ctx.Err()
			break blockLoop
		case jobs <- task:
		}
		idx++
	}
	close(jobs)
	pend.Wait()

	// If execution failed in between, abort
	if failed != nil {
		log.Error("[GetLogs]", "failed", failed)
		return nil, failed
	}

	logs := []*types.Log{}
	for idx := range results {
		res := results[idx]
		if res == nil {
			continue
		}
		logs = append(logs, res.Logs...)
	}
	return logs, nil
}

func filterLogs(logs types.Logs, addresses []common.Address, topics [][]common.Hash) []*types.Log {
	addrMap := make(map[common.Address]struct{}, len(addresses))
	for _, v := range addresses {
		addrMap[v] = struct{}{}
	}
	return logs.Filter(addrMap, topics)
}

func (api *OverlayAPIImpl) replayBlock(ctx context.Context, blockNum uint64, statedb *state.IntraBlockState, chainConfig *chain.Config, tx kv.Tx) ([]*types.Log, error) {
	log.Debug("[replayBlock] begin", "block", blockNum)
	var (
		hash               common.Hash
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
		baseFee            uint256.Int
	)

	blockLogs := []*types.Log{}
	overrideBlockHash = make(map[uint64]common.Hash)

	blockNumber := rpc.BlockNumber(blockNum)
	blockNum, hash, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHash{BlockNumber: &blockNumber}, tx, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil || block == nil {
		return nil, err
	}

	replayTransactions = block.Transactions()
	log.Debug("[replayBlock] replayTx", "length", len(replayTransactions))

	parent := block.Header()

	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := api._blockReader.CanonicalHash(ctx, tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	if parent.BaseFee != nil {
		baseFee.SetFromBig(parent.BaseFee)
	}

	var excessBlobGas uint64 = 0
	blockCtx = evmtypes.BlockContext{
		CanTransfer:   core.CanTransfer,
		Transfer:      core.Transfer,
		GetHash:       getHash,
		Coinbase:      parent.Coinbase,
		BlockNumber:   parent.Number.Uint64(),
		Time:          parent.Time,
		Difficulty:    new(big.Int).Set(parent.Difficulty),
		GasLimit:      parent.GasLimit,
		BaseFee:       &baseFee,
		ExcessBlobGas: &excessBlobGas,
	}

	signer := types.MakeSigner(chainConfig, blockNum, blockCtx.Time)
	rules := chainConfig.Rules(blockNum, blockCtx.Time)

	timeout := api.OverlayReplayBlockTimeout
	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)
	vmConfig := vm.Config{Debug: false}
	evm = vm.NewEVM(blockCtx, evmtypes.TxContext{}, statedb, chainConfig, vmConfig)
	receipts, err := api.getReceipts(ctx, tx, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, err
	}

	// try to replay all transactions in this block
	for idx, txn := range replayTransactions {
		log.Debug("[replayBlock] replaying transaction", "idx", idx, "transactionHash", txn.Hash())

		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		msg.ChangeGas(api.GasCap, api.GasCap)

		receipt := receipts[uint64(idx)]
		log.Debug("[replayBlock]", "receipt.TransactionIndex", receipt.TransactionIndex, "receipt.TxHash", receipt.TxHash, "receipt.Status", receipt.Status)
		// check if this tx has failed in the original context
		if receipt.Status == types.ReceiptStatusFailed {
			log.Debug("[replayBlock] skipping transaction because it has status=failed", "transactionHash", txn.Hash())

			contractCreation := msg.To() == nil
			if !contractCreation {
				// bump the nonce of the sender
				sender := vm.AccountRef(msg.From())
				statedb.SetNonce(msg.From(), statedb.GetNonce(sender.Address())+1)
				continue
			}
		}

		statedb.SetTxContext(txn.Hash(), block.Hash(), idx)
		txCtx = core.NewEVMTxContext(msg)
		evm.TxContext = txCtx

		// Execute the transaction message
		res, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		err = statedb.FinalizeTx(rules, state.NewNoopWriter())
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		// If the timer caused an abort, return an appropriate error message
		if evm.Cancelled() {
			log.Error("EVM cancelled")
			return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
		}

		if res.Failed() {
			log.Debug("[replayBlock] res result for transaction", "transactionHash", txn.Hash(), "failed", res.Failed(), "revert", res.Revert(), "error", res.Err)
			log.Debug("[replayBlock] discarding txLogs because tx has status=failed", "transactionHash", txn.Hash())
		} else {
			// append logs only if tx has not reverted
			txLogs := statedb.GetLogs(txn.Hash())
			log.Debug("[replayBlock]", "len(txLogs)", len(txLogs), "transactionHash", txn.Hash())
			blockLogs = append(blockLogs, txLogs...)
		}
	}
	return blockLogs, nil
}

func getBeginEnd(ctx context.Context, tx kv.Tx, api *OverlayAPIImpl, crit filters.FilterCriteria) (uint64, uint64, error) {
	var begin, end uint64
	if crit.BlockHash != nil {
		block, err := api.blockByHashWithSenders(ctx, tx, *crit.BlockHash)
		if err != nil {
			return 0, 0, err
		}

		if block == nil {
			return 0, 0, fmt.Errorf("block not found: %x", *crit.BlockHash)
		}

		num := block.NumberU64()
		begin = num
		end = num
	} else {
		// Convert the RPC block numbers into internal representations
		latest, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, nil)
		if err != nil {
			return 0, 0, err
		}

		begin = latest
		if crit.FromBlock != nil {
			fromBlock := crit.FromBlock.Int64()
			if fromBlock > 0 {
				begin = uint64(fromBlock)
			} else {
				blockNum := rpc.BlockNumber(fromBlock)
				begin, _, _, err = rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(blockNum), tx, api.filters)
				if err != nil {
					return 0, 0, err
				}
			}

		}
		end = latest
		if crit.ToBlock != nil {
			toBlock := crit.ToBlock.Int64()
			if toBlock > 0 {
				end = uint64(toBlock)
			} else {
				blockNum := rpc.BlockNumber(toBlock)
				end, _, _, err = rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(blockNum), tx, api.filters)
				if err != nil {
					return 0, 0, err
				}
			}
		}
	}

	if end < begin {
		return 0, 0, fmt.Errorf("end (%d) < begin (%d)", end, begin)
	}
	if end > roaring.MaxUint32 {
		latest, err := rpchelper.GetLatestFinishedBlockNumber(tx)
		if err != nil {
			return 0, 0, err
		}
		if begin > latest {
			return 0, 0, fmt.Errorf("begin (%d) > latest (%d)", begin, latest)
		}
		end = latest
	}
	return begin, end, nil
}
