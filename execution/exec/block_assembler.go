package exec

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/aa"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"

	"github.com/erigontech/erigon/db/services"
)

type AssemblerCfg struct {
	ChainConfig     *chain.Config
	Engine          rules.Engine
	BlockReader     services.FullBlockReader
	ExperimentalBAL bool
}

type AssembledBlock struct {
	PayloadId        uint64
	ParentHeaderTime uint64
	Header           *types.Header
	Uncles           []*types.Header
	Txns             types.Transactions
	Receipts         types.Receipts
	Withdrawals      []*types.Withdrawal
	Requests         types.FlatRequests
	BlockAccessList  types.BlockAccessList

	headerRlpSize         *int
	withdrawalsRlpSize    *int
	unclesRlpSize         *int
	txnsRlpSize           int
	txnsRlpSizeCalculated int
}

func (mb *AssembledBlock) AddTxn(txn types.Transaction) {
	mb.Txns = append(mb.Txns, txn)
	s := txn.EncodingSize()
	s += rlp.ListPrefixLen(s)
	mb.txnsRlpSize += s
	mb.txnsRlpSizeCalculated++
}

func (mb *AssembledBlock) AvailableRlpSpace(chainConfig *chain.Config, withAdditional ...types.Transaction) int {
	if mb.headerRlpSize == nil {
		s := mb.Header.EncodingSize()
		s += rlp.ListPrefixLen(s)
		mb.headerRlpSize = &s
	}
	if mb.withdrawalsRlpSize == nil {
		var s int
		if mb.Withdrawals != nil {
			s = types.EncodingSizeGenericList(mb.Withdrawals)
			s += rlp.ListPrefixLen(s)
		}
		mb.withdrawalsRlpSize = &s
	}
	if mb.unclesRlpSize == nil {
		s := types.EncodingSizeGenericList(mb.Uncles)
		s += rlp.ListPrefixLen(s)
		mb.unclesRlpSize = &s
	}

	blockSize := *mb.headerRlpSize
	blockSize += *mb.unclesRlpSize
	blockSize += *mb.withdrawalsRlpSize
	blockSize += mb.TxnsRlpSize(withAdditional...)
	blockSize += rlp.ListPrefixLen(blockSize)
	maxSize := chainConfig.GetMaxRlpBlockSize(mb.Header.Time)
	return maxSize - blockSize
}

func (mb *AssembledBlock) TxnsRlpSize(withAdditional ...types.Transaction) int {
	if len(mb.Txns) != mb.txnsRlpSizeCalculated {
		panic("mismatch between mb.Txns and mb.txnsRlpSizeCalculated - did you forget to use mb.AddTxn()?")
	}
	s := mb.txnsRlpSize
	s += types.EncodingSizeGenericList(withAdditional) // what size would be if we add additional txns
	s += rlp.ListPrefixLen(s)
	return s
}

type BlockAssembler struct {
	*AssembledBlock
	cfg         AssemblerCfg
	balIO       *state.VersionedIO
	stateWriter state.StateWriter // optional: if set, domain writes go here instead of NoopWriter
	gasUsed     protocol.GasUsed  // EIP-8037: cumulative per-dimension gas across AddTransactions calls
}

func (ba *BlockAssembler) CumulativeGasUsed() protocol.GasUsed { return ba.gasUsed }

func NewBlockAssembler(cfg AssemblerCfg, block *AssembledBlock) *BlockAssembler {
	var balIO *state.VersionedIO

	if cfg.ChainConfig.IsAmsterdam(block.Header.Time) || cfg.ExperimentalBAL {
		balIO = &state.VersionedIO{}
	}
	return &BlockAssembler{
		AssembledBlock: block,
		cfg:            cfg,
		balIO:          balIO,
	}
}

func (ba *BlockAssembler) HasBAL() bool {
	return ba.balIO != nil
}

// SetStateWriter sets a real state writer for domain writes during block assembly.
// When set, state changes are written to the backing store (e.g. SharedDomains)
// instead of being discarded via NoopWriter. This enables computing state root
// directly from the assembled block without re-executing via ExecV3.
func (ba *BlockAssembler) SetStateWriter(w state.StateWriter) {
	ba.stateWriter = w
}

func (ba *BlockAssembler) writer() state.StateWriter {
	if ba.stateWriter != nil {
		return ba.stateWriter
	}
	return state.NewNoopWriter()
}

func (ba *BlockAssembler) BalIO() *state.VersionedIO {
	return ba.balIO
}

func (ba *BlockAssembler) Initialize(ibs *state.IntraBlockState, tx kv.TemporalTx, logger log.Logger) error {
	// Use NoopWriter for FinalizeTx during initialization. Intermediate state
	// writes to SharedDomains would become stale if later system calls (e.g.
	// EIP-7002 dequeue) revert storage slots back to their original values,
	// because CommitBlock's blockOriginStorage==dirtyStorage check skips the
	// undo write. All final state is correctly written by CommitBlock in
	// AssembleBlock using the real writer.
	if err := protocol.InitializeBlockExecution(ba.cfg.Engine,
		NewChainReader(ba.cfg.ChainConfig, tx, ba.cfg.BlockReader, logger), ba.Header, ba.cfg.ChainConfig, ibs, state.NewNoopWriter(), logger, nil); err != nil {
		return err
	}
	if ba.HasBAL() {
		ba.balIO = ba.balIO.Merge(ibs.TxIO())
		ibs.ResetVersionedIO()
	}
	return nil
}

func (ba *BlockAssembler) AddTransactions(
	ctx context.Context,
	getHeader func(hash common.Hash, number uint64) (*types.Header, error),
	txns types.Transactions,
	coinbase accounts.Address,
	vmConfig *vm.Config,
	ibs *state.IntraBlockState,
	interrupt *atomic.Bool,
	logPrefix string,
	logger log.Logger) (types.Logs, bool, error) {

	txnIdx := ibs.TxnIndex() + 1
	header := ba.AssembledBlock.Header
	// EIP-8037: initialize the pool from cumulative regular gas, not the
	// bottleneck (max of regular, state) stored in header.GasUsed. This
	// gives compute-heavy transactions access to the full regular gas
	// budget even when state gas dominates the bottleneck. State gas is
	// enforced in applyTransaction before FinalizeTx.
	gasPool := new(protocol.GasPool).AddGas(header.GasLimit - ba.gasUsed.BlockRegular)
	if header.BlobGasUsed != nil {
		gasPool.AddBlobGas(ba.cfg.ChainConfig.GetMaxBlobGasPerBlock(header.Time) - *header.BlobGasUsed)
	}
	signer := types.MakeSigner(ba.cfg.ChainConfig, header.Number.Uint64(), header.Time)

	var coalescedLogs types.Logs
	// Use NoopWriter for FinalizeTx after each user transaction. See
	// Initialize comment for rationale — intermediate writes to
	// SharedDomains become stale when system calls revert storage slots.
	// CommitBlock in AssembleBlock writes all final state correctly.
	writer := state.NewNoopWriter()
	recordTxIO := func(balIO *state.VersionedIO) {
		if balIO != nil {
			ba.balIO = ba.balIO.Merge(ibs.TxIO())
		}
		ibs.ResetVersionedIO()
	}
	clearTxIO := func(balIO *state.VersionedIO) {
		if balIO == nil {
			return
		}
		ibs.AccessedAddresses()
		ibs.ResetVersionedIO()
	}

	gasUsed := &ba.gasUsed

	var commitTx = func(txn types.Transaction, coinbase accounts.Address, vmConfig *vm.Config, chainConfig *chain.Config, ibs *state.IntraBlockState, current *AssembledBlock) ([]*types.Log, error) {
		ibs.SetTxContext(current.Header.Number.Uint64(), txnIdx)
		gasSnap := gasPool.Gas()
		blobGasSnap := gasPool.BlobGas()
		snap := ibs.PushSnapshot()
		defer ibs.PopSnapshot(snap)

		if txn.Type() == types.AccountAbstractionTxType {
			aaTxn := txn.(*types.AccountAbstractionTransaction)
			blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, getHeader), ba.cfg.Engine, coinbase, chainConfig)
			evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, *vmConfig)
			paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, ibs, gasPool, header, evm, chainConfig)
			if err != nil {
				ibs.RevertToSnapshot(snap, err)
				gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap)
				return nil, err
			}

			status, aaGasUsed, err := aa.ExecuteAATransaction(aaTxn, paymasterContext, validationGasUsed, gasPool, evm, header, ibs)
			if err != nil {
				ibs.RevertToSnapshot(snap, err)
				gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap)
				return nil, err
			}

			// EIP-8037: AA txns don't go through protocol.ApplyTransaction, so
			// update cumulative gas manually. We attribute aaGasUsed entirely to
			// regular gas (AA has no state-gas dimension yet).
			gasUsed.BlockRegular += aaGasUsed
			gasUsed.Blob += txn.GetBlobGas()
			protocol.SetGasUsed(header, gasUsed)
			logs := ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), header.Number.Uint64(), header.Hash())
			receipt := aa.CreateAAReceipt(txn.Hash(), status, aaGasUsed, header.GasUsed, header.Number.Uint64(), uint64(ibs.TxnIndex()), logs)

			current.AddTxn(txn)
			current.Receipts = append(current.Receipts, receipt)
			return receipt.Logs, nil
		}

		// Snapshot cumulative gas so we can restore on tx failure.
		gasSnapshot := *gasUsed

		receipt, err := protocol.ApplyTransaction(chainConfig, protocol.GetHashFn(header, getHeader),
			ba.cfg.Engine, coinbase, gasPool, ibs, writer, header, txn, gasUsed, *vmConfig)
		if err != nil {
			// Restore cumulative gas to pre-tx values.
			*gasUsed = gasSnapshot
			ibs.RevertToSnapshot(snap, err)
			gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap)
			return nil, err
		}
		protocol.SetGasUsed(header, gasUsed)

		current.AddTxn(txn)
		current.Receipts = append(current.Receipts, receipt)
		return receipt.Logs, nil
	}

	var stopped *time.Ticker
	defer func() {
		if stopped != nil {
			stopped.Stop()
		}
	}()

	done := false

LOOP:
	for _, txn := range txns {
		// see if we need to stop now
		if stopped != nil {
			select {
			case <-stopped.C:
				done = true
				break LOOP
			default:
			}
		}

		if err := common.Stopped(ctx.Done()); err != nil {
			return nil, true, err
		}

		if interrupt != nil && interrupt.Load() && stopped == nil {
			logger.Debug("Transaction adding was requested to stop", "payload", ba.PayloadId)
			// ensure we run for at least 500ms after the request to stop comes in from GetPayload
			stopped = time.NewTicker(500 * time.Millisecond)
		}
		// If we don't have enough gas for any further transactions then we're done.
		if gasPool.Gas() < params.TxGas {
			logger.Debug(fmt.Sprintf("[%s] Not enough gas for further transactions", logPrefix), "have", gasPool, "want", params.TxGas)
			done = true
			break
		}

		rlpSpacePostTxn := ba.AvailableRlpSpace(ba.cfg.ChainConfig, txn)
		if rlpSpacePostTxn < 0 {
			rlpSpacePreTxn := ba.AvailableRlpSpace(ba.cfg.ChainConfig)
			logger.Debug(
				fmt.Sprintf("[%s] Skipping transaction since it does not fit in available rlp space", logPrefix),
				"hash", txn.Hash(),
				"pre", rlpSpacePreTxn,
				"post", rlpSpacePostTxn,
			)
			continue
		}

		// We use the eip155 signer regardless of the env hf.
		from, err := txn.Sender(*signer)
		if err != nil {
			logger.Warn(fmt.Sprintf("[%s] Could not recover transaction sender", logPrefix), "hash", txn.Hash(), "err", err)
			continue
		}

		// Check whether the txn is replay protected. If we're not in the EIP155 (Spurious Dragon) hf
		// phase, start ignoring the sender until we do.
		if txn.Protected() && !ba.cfg.ChainConfig.IsSpuriousDragon(header.Number.Uint64()) {
			logger.Debug(fmt.Sprintf("[%s] Ignoring replay protected transaction", logPrefix), "hash", txn.Hash(), "eip155", ba.cfg.ChainConfig.SpuriousDragonBlock)
			continue
		}

		// Start executing the transaction
		logs, err := commitTx(txn, coinbase, vmConfig, ba.cfg.ChainConfig, ibs, ba.AssembledBlock)
		if err == nil {
			recordTxIO(ba.balIO)
		} else {
			clearTxIO(ba.balIO)
		}
		if errors.Is(err, protocol.ErrGasLimitReached) {
			logger.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash(), "sender", from)
		} else if errors.Is(err, protocol.ErrNonceTooLow) {
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "err", err)
		} else if errors.Is(err, protocol.ErrNonceTooHigh) {
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
		} else if err == nil {
			logger.Trace(fmt.Sprintf("[%s] Added transaction", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "payload", ba.PayloadId)
			coalescedLogs = append(coalescedLogs, logs...)
			txnIdx++
		} else {
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction", logPrefix), "hash", txn.Hash(), "sender", from, "err", err)
		}
	}

	return coalescedLogs, done, nil
}

func (ba *BlockAssembler) AssembleBlock(stateReader state.StateReader, ibs *state.IntraBlockState, tx kv.TemporalTx, logger log.Logger) (block *types.Block, err error) {
	chainReader := NewChainReader(ba.cfg.ChainConfig, tx, ba.cfg.BlockReader, logger)

	if err := ba.cfg.Engine.Prepare(chainReader, ba.Header, ibs); err != nil {
		return nil, err
	}

	if ba.HasBAL() {
		// Set block-end tx context so that system-call I/O (withdrawals, EIP-7002,
		// EIP-7251, etc.) is recorded at TxIndex = len(userTxns), matching the
		// validator path (exec3_parallel.go: ibs.SetTxContext(finalVersion.BlockNum, finalVersion.TxIndex)).
		ibs.SetTxContext(ba.Header.Number.Uint64(), len(ba.Txns))
		ibs.ResetVersionedIO()
	}
	block, ba.Requests, err = protocol.FinalizeBlockExecution(ba.cfg.Engine, stateReader, ba.Header, ba.Txns, ba.Uncles,
		ba.writer(), ba.cfg.ChainConfig, ibs, ba.Receipts, ba.Withdrawals, chainReader, true, logger, nil)

	if err != nil {
		return nil, fmt.Errorf("cannot finalize block execution: %s", err)
	}

	// Note: NewBlock (called by FinalizeBlockExecution) copies the header,
	// so we must modify the block's header directly, not ba.Header.
	header := block.HeaderNoCopy()
	if ba.HasBAL() {
		// Record finalize system call I/O (EIP-7002, EIP-7251, etc.)
		ba.balIO = ba.balIO.Merge(ibs.TxIO())
		ibs.ResetVersionedIO()
		ba.BlockAccessList = ba.balIO.AsBlockAccessList()
		// Only embed the BAL hash in the header for Amsterdam+ chains.
		// For pre-Amsterdam chains with ExperimentalBAL, the BAL is computed
		// and validated but NOT included in the block header, because the
		// header RLP encoding is positional and skipping intermediate nil
		// fields (BlobGasUsed, ExcessBlobGas, etc.) would cause a
		// marshaling mismatch on decode.
		if ba.cfg.ChainConfig.IsAmsterdam(header.Time) {
			balHash := ba.BlockAccessList.Hash()
			header.BlockAccessListHash = &balHash
		}
	}

	return block, nil
}
