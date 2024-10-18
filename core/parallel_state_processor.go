package core

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/blockstm"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/crypto"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/turbo/services"
	"golang.org/x/exp/slices"

	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
)

type ParallelEVMConfig struct {
	Enable               bool
	SpeculativeProcesses int
}

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type ParallelStateProcessor struct {
	config *chain.Config    // Chain configuration options
	engine consensus.Engine // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewParallelStateProcessor(config *chain.Config, engine consensus.Engine) *ParallelStateProcessor {
	return &ParallelStateProcessor{
		config: config,
		engine: engine,
	}
}

type ExecutionTask struct {
	config *chain.Config

	gasLimit                   uint64
	blockHash                  libcommon.Hash
	tx                         types.Transaction
	block                      *types.Block
	index                      int
	statedb                    *state.IntraBlockState // State database that stores the modified values after tx execution.
	finalStateDB               *state.IntraBlockState // The final statedb.
	header                     *types.Header
	evmConfig                  *vm.Config
	result                     *evmtypes.ExecutionResult
	shouldDelayFeeCal          *bool
	shouldRerunWithoutFeeDelay bool
	sender                     libcommon.Address
	totalUsedGas               *uint64
	receipts                   *types.Receipts
	allLogs                    *[]*types.Log
	stateWriter                state.StateWriter
	excessDataGas              *big.Int

	// length of dependencies          -> 2 + k (k = a whole number)
	// first 2 element in dependencies -> transaction index, and flag representing if delay is allowed or not
	//                                       (0 -> delay is not allowed, 1 -> delay is allowed)
	// next k elements in dependencies -> transaction indexes on which transaction i is dependent on
	dependencies []int

	blockContext  evmtypes.BlockContext
	evm           *vm.EVM
	engine        consensus.Engine
	msg           *types.Message
	db            kv.RwDB
	dbtx          kv.Tx
	rules         *chain.Rules
	blockHashFunc func(n uint64) libcommon.Hash
	getHeader     func(hash libcommon.Hash, number uint64) *types.Header
	chainReader   consensus.ChainHeaderReader
	blockReader   services.FullBlockReader
	ctx           context.Context
}

func (task *ExecutionTask) Execute(mvh *blockstm.MVHashMap, incarnation int, logger log.Logger) (err error) {
	if task.dbtx == nil {
		task.dbtx, err = task.db.BeginRo(task.ctx)
		if err != nil {
			err = blockstm.ErrExecAbortError{OriginError: err}
			return err
		}
	}

	task.statedb = state.NewWithMVHashmap(state.NewPlainStateReader(task.dbtx), mvh)
	task.statedb.SetTxContext(task.index)

	task.statedb.SetBlockSTMIncarnation(incarnation)

	if !task.evmConfig.ReadOnly {
		if err := InitializeBlockExecution(task.engine, task.chainReader, task.block.Header(), task.config, task.statedb, logger, nil); err != nil {
			return err
		}
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := task.blockReader.Header(context.Background(), task.dbtx, hash, number)
		return h
	}

	getHashFn := GetHashFn(task.block.Header(), getHeader)

	blockContext := NewEVMBlockContext(task.header, getHashFn, task.engine, nil, task.config)

	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, task.statedb, task.config, *task.evmConfig)

	task.evm = evm

	txContext := NewEVMTxContext(task.msg)
	if task.evmConfig.TraceJumpDest {
		txContext.TxHash = task.tx.Hash()
	}

	evm.Reset(txContext, task.statedb)

	defer func() {
		if r := recover(); r != nil {
			// Recover from dependency panic and retry the execution.
			log.Debug("Recovered from EVM failure.", "Error:", r)

			err = blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex()}

			return
		}
	}()

	// Apply the transaction to the current state (included in the env).
	if *task.shouldDelayFeeCal {
		task.result, err = ApplyMessageNoFeeBurnOrTip(evm, task.msg, new(GasPool).AddGas(task.gasLimit), true, false)

		if task.result == nil || err != nil {
			return blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		}

		reads := task.statedb.MVReadMap()

		if _, ok := reads[blockstm.NewSubpathKey(blockContext.Coinbase, state.BalancePath)]; ok {
			log.Debug("Coinbase is in MVReadMap", "address", blockContext.Coinbase)

			task.shouldRerunWithoutFeeDelay = true
		}

		if _, ok := reads[blockstm.NewSubpathKey(task.result.BurntContractAddress, state.BalancePath)]; ok {
			log.Debug("BurntContractAddress is in MVReadMap", "address", task.result.BurntContractAddress)

			task.shouldRerunWithoutFeeDelay = true
		}
	} else {
		task.result, err = ApplyMessage(evm, task.msg, new(GasPool).AddGas(task.gasLimit), true, false)
	}

	if task.statedb.HadInvalidRead() || err != nil {
		err = blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		return
	}

	task.statedb.FinalizeTx(evm.ChainRules(), task.stateWriter)
	return
}

func (task *ExecutionTask) MVReadList() []blockstm.ReadDescriptor {
	return task.statedb.MVReadList()
}

func (task *ExecutionTask) MVWriteList() []blockstm.WriteDescriptor {
	return task.statedb.MVWriteList()
}

func (task *ExecutionTask) MVFullWriteList() []blockstm.WriteDescriptor {
	return task.statedb.MVFullWriteList()
}

func (task *ExecutionTask) Sender() libcommon.Address {
	return task.sender
}

func (task *ExecutionTask) Hash() libcommon.Hash {
	return task.tx.Hash()
}

func (task *ExecutionTask) Dependencies() []int {
	return task.dependencies
}

func (task *ExecutionTask) Settle() {
	task.finalStateDB.SetTxContext(task.index)

	task.finalStateDB.ApplyMVWriteSet(task.statedb.MVFullWriteList())

	txHash := task.tx.Hash()
	blockNumber := task.block.NumberU64()

	for _, l := range task.statedb.GetLogs(task.index, txHash, blockNumber, task.blockHash) {
		task.finalStateDB.AddLog(l)
	}

	if *task.shouldDelayFeeCal {
		if task.config.IsLondon(task.block.NumberU64()) {
			task.finalStateDB.AddBalance(task.result.BurntContractAddress, task.result.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		task.finalStateDB.AddBalance(task.blockContext.Coinbase, task.result.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if task.engine != nil {
			if postApplyMessageFunc := task.engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				result := *task.result
				result.CoinbaseInitBalance = task.finalStateDB.GetBalance(task.blockContext.Coinbase).Clone()

				postApplyMessageFunc(
					task.finalStateDB,
					task.msg.From(),
					task.blockContext.Coinbase,
					&result,
				)
			}
		}
	}

	// Update the state with pending changes.
	var root []byte

	if task.config.IsByzantium(task.block.NumberU64()) {
		task.finalStateDB.FinalizeTx(task.rules, task.stateWriter)
	}

	*task.totalUsedGas += task.result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: task.tx.Type(), PostState: root, CumulativeGasUsed: *task.totalUsedGas}
	if task.result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	receipt.TxHash = txHash
	receipt.GasUsed = task.result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if task.msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(task.msg.From(), task.tx.GetNonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = task.finalStateDB.GetLogs(task.index, txHash, blockNumber, task.blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = task.blockHash
	receipt.BlockNumber = task.block.Number()
	receipt.TransactionIndex = uint(task.index)

	*task.receipts = append(*task.receipts, receipt)
	*task.allLogs = append(*task.allLogs, receipt.Logs...)
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
// nolint:gocognit
func ParallelExecuteBlockEphemerally(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainReader,
	getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
	getHeader func(hash libcommon.Hash, number uint64) *types.Header,
	db kv.RwDB,
	blockReader services.FullBlockReader,
	logger log.Logger,
) (*EphemeralExecResult, error) {

	defer blockExecutionTimer.ObserveDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock())

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, logger, nil); err != nil {
			return nil, err
		}
	}

	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	noop := state.NewNoopWriter()

	shouldDelayFeeCal := true
	tasks := make([]blockstm.ExecTask, 0, len(block.Transactions()))

	var logs []*types.Log

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, nil, chainConfig)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, *vmConfig)

	rules := vmenv.ChainRules()

	ctx := context.Background()

	vmConfig.SkipAnalysis = SkipAnalysis(chainConfig, header.Number.Uint64())

	for i, tx := range block.Transactions() {
		msg, err := tx.AsMessage(*types.MakeSigner(chainConfig, header.Number.Uint64(), header.Time), header.BaseFee, rules)
		if err != nil {
			return nil, err
		}
		msg.SetCheckNonce(!vmConfig.StatelessExec)

		if msg.FeeCap().IsZero() && engine != nil {
			// Only zero-gas transactions may be service ones
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return SysCallContract(contract, data, vmenv.ChainConfig(), ibs, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		evmConfig := vmConfig.Copy()
		evmConfig.Debug = false

		if evmConfig.Debug {
			tracer, err := getTracer(i, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}

			evmConfig.Tracer = tracer
		}

		task := &ExecutionTask{
			config:            chainConfig,
			gasLimit:          block.GasLimit(),
			blockHash:         block.Hash(),
			block:             block,
			tx:                tx,
			index:             i,
			finalStateDB:      ibs,
			header:            header,
			evmConfig:         evmConfig,
			shouldDelayFeeCal: &shouldDelayFeeCal,
			totalUsedGas:      usedGas,
			receipts:          &receipts,
			allLogs:           &logs,
			dependencies:      nil,
			blockContext:      blockContext,
			stateWriter:       noop,
			// excessDataGas:     excessDataGas,
			db:            db,
			sender:        msg.From(),
			msg:           &msg,
			rules:         rules,
			blockHashFunc: blockHashFunc,
			engine:        engine,
			getHeader:     getHeader,
			chainReader:   chainReader,
			blockReader:   blockReader,
			ctx:           ctx,
		}

		tasks = append(tasks, task)

		defer func(t *ExecutionTask) {
			if t.dbtx != nil {
				t.dbtx.Rollback()
			}
		}(task)
	}

	var err error

	_, err = blockstm.ExecuteParallel(context.Background(), tasks, false, false, logger)

	if err != nil {
		return ExecuteBlockEphemerally(chainConfig, vmConfig, blockHashFunc, engine, block, stateReader, stateWriter, chainReader, getTracer, logger)
	}

	for _, task := range tasks {
		task := task.(*ExecutionTask)
		if task.shouldRerunWithoutFeeDelay {
			return ExecuteBlockEphemerally(chainConfig, vmConfig, blockHashFunc, engine, block, stateReader, stateWriter, chainReader, getTracer, logger)
		}
	}

	for _, task := range tasks {
		includedTxs = append(includedTxs, task.(*ExecutionTask).tx)
		task.(*ExecutionTask).Settle()
	}

	if err != nil {
		log.Error("blockstm error executing block", "err", err)
		return nil, err
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		for i, l := range logs {
			log.Info("Log", "index", i, "address", l.Address, "topics", l.Topics, "data", fmt.Sprintf("%x", l.Data))
		}

		for i, r := range tasks {
			log.Info("Receipt", "index", i, "incarnation", r.(*ExecutionTask).statedb.Version().Incarnation, "usedGas", r.(*ExecutionTask).result.UsedGas)
		}

		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	if header.BlobGasUsed != nil && *usedBlobGas != *header.BlobGasUsed {
		return nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", *usedBlobGas, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), block.Requests(), chainReader, false, logger); err != nil {
			return nil, err
		}
	}

	blockLogs := ibs.Logs()
	stateSyncReceipt := &types.Receipt{}
	if chainConfig.Consensus == chain.BorConsensus && len(blockLogs) > 0 {
		slices.SortStableFunc(blockLogs, func(i, j *types.Log) int { return int(i.Index) - int(j.Index) })

		if len(blockLogs) > len(logs) {
			stateSyncReceipt.Logs = blockLogs[len(logs):] // get state-sync logs from `state.Logs()`

			// fill the state sync with the correct information
			bortypes.DeriveFieldsForBorReceipt(stateSyncReceipt, block.Hash(), block.NumberU64(), receipts)
			stateSyncReceipt.Status = types.ReceiptStatusSuccessful
		}
	}

	execRs := &EphemeralExecResult{
		TxRoot:           types.DeriveSha(includedTxs),
		ReceiptRoot:      receiptSha,
		Bloom:            bloom,
		LogsHash:         rlpHash(blockLogs),
		Receipts:         receipts,
		Difficulty:       (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:          math.HexOrDecimal64(*usedGas),
		Rejected:         rejectedTxs,
		StateSyncReceipt: stateSyncReceipt,
	}

	return execRs, nil
}

func GetDeps(txDependency [][]uint64) (map[int][]int, map[int]bool) {
	deps := make(map[int][]int)
	delayMap := make(map[int]bool)

	for i := 0; i <= len(txDependency)-1; i++ {
		idx := int(txDependency[i][0])
		shouldDelay := txDependency[i][1] == 1

		delayMap[idx] = shouldDelay

		deps[idx] = []int{}

		for j := 2; j <= len(txDependency[i])-1; j++ {
			deps[idx] = append(deps[idx], int(txDependency[i][j]))
		}
	}

	return deps, delayMap
}
