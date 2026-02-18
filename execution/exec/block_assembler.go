package exec

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
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
)

type AssemblerCfg struct {
	ChainConfig     *chain.Config
	Engine          rules.Engine
	BlockReader     services.FullBlockReader
	ExperimentalBAL bool
}

type assembledBlock struct {
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

func (mb *assembledBlock) AddTxn(txn types.Transaction) {
	mb.Txns = append(mb.Txns, txn)
	s := txn.EncodingSize()
	s += rlp.ListPrefixLen(s)
	mb.txnsRlpSize += s
	mb.txnsRlpSizeCalculated++
}

func (mb *assembledBlock) AvailableRlpSpace(chainConfig *chain.Config, withAdditional ...types.Transaction) int {
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

func (mb *assembledBlock) TxnsRlpSize(withAdditional ...types.Transaction) int {
	if len(mb.Txns) != mb.txnsRlpSizeCalculated {
		panic("mismatch between mb.Txns and mb.txnsRlpSizeCalculated - did you forget to use mb.AddTxn()?")
	}
	s := mb.txnsRlpSize
	s += types.EncodingSizeGenericList(withAdditional) // what size would be if we add additional txns
	s += rlp.ListPrefixLen(s)
	return s
}

type BlockAssembler struct {
	*assembledBlock
	cfg   AssemblerCfg
	balIO *state.VersionedIO
}

func NewBlockAssembler(cfg AssemblerCfg, payloadId, parentTime uint64, header *types.Header, uncles []*types.Header, withdrawals []*types.Withdrawal) *BlockAssembler {
	var balIO *state.VersionedIO

	if cfg.ChainConfig.IsAmsterdam(header.Time) || cfg.ExperimentalBAL {
		balIO = &state.VersionedIO{}
	}
	return &BlockAssembler{
		assembledBlock: &assembledBlock{
			PayloadId:        payloadId,
			ParentHeaderTime: parentTime,
			Header:           header,
			Uncles:           uncles,
			Withdrawals:      withdrawals,
		},
		cfg:   cfg,
		balIO: balIO,
	}
}

func (ba *BlockAssembler) HasBAL() bool {
	return ba.balIO != nil
}

func (ba *BlockAssembler) Initialize(ibs *state.IntraBlockState, tx kv.TemporalTx, logger log.Logger) {
	protocol.InitializeBlockExecution(ba.cfg.Engine,
		NewChainReader(ba.cfg.ChainConfig, tx, ba.cfg.BlockReader, logger), ba.Header, ba.cfg.ChainConfig, ibs, &state.NoopWriter{}, logger, nil)
	if ba.HasBAL() {
		ba.balIO = ba.balIO.Merge(ibs.TxIO())
		//ba.systemReads = ba.systemReads.Merge(ibs.VersionedReads())
		//ba.systemWrites = ba.systemWrites.Merge(ibs.VersionedWrites(false))
		//ba.systemAccess = ba.systemAccess.Merge(ibs.AccessedAddresses())
		ibs.ResetVersionedIO()
	}
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

	txnIdx := ibs.TxnIndex()
	header := ba.assembledBlock.Header
	gasPool := new(protocol.GasPool).AddGas(header.GasLimit - header.GasUsed)
	if header.BlobGasUsed != nil {
		gasPool.AddBlobGas(ba.cfg.ChainConfig.GetMaxBlobGasPerBlock(header.Time) - *header.BlobGasUsed)
	}
	signer := types.MakeSigner(ba.cfg.ChainConfig, header.Number.Uint64(), header.Time)

	var coalescedLogs types.Logs
	noop := state.NewNoopWriter()
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

	var miningCommitTx = func(txn types.Transaction, coinbase accounts.Address, vmConfig *vm.Config, chainConfig *chain.Config, ibs *state.IntraBlockState, current *assembledBlock) ([]*types.Log, error) {
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
				gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
				return nil, err
			}

			status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, paymasterContext, validationGasUsed, gasPool, evm, header, ibs)
			if err != nil {
				ibs.RevertToSnapshot(snap, err)
				gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
				return nil, err
			}

			header.GasUsed += gasUsed
			logs := ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), header.Number.Uint64(), header.Hash())
			receipt := aa.CreateAAReceipt(txn.Hash(), status, gasUsed, header.GasUsed, header.Number.Uint64(), uint64(ibs.TxnIndex()), logs)

			current.AddTxn(txn)
			current.Receipts = append(current.Receipts, receipt)
			return receipt.Logs, nil
		}

		gasUsed := protocol.NewGasUsed(header, current.Receipts.CumulativeGasUsed())
		receipt, err := protocol.ApplyTransaction(chainConfig, protocol.GetHashFn(header, getHeader),
			ba.cfg.Engine, coinbase, gasPool, ibs, noop, header, txn, gasUsed, *vmConfig)
		if err != nil {
			ibs.RevertToSnapshot(snap, err)
			gasPool = new(protocol.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
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
		// If we don't have enough gas for any further transactions then we're done
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
		logs, err := miningCommitTx(txn, coinbase, vmConfig, ba.cfg.ChainConfig, ibs, ba.assembledBlock)
		if err == nil {
			recordTxIO(ba.balIO)
		} else {
			clearTxIO(ba.balIO)
		}
		if errors.Is(err, protocol.ErrGasLimitReached) {
			// Skip the env out-of-gas transaction
			logger.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash(), "sender", from)
		} else if errors.Is(err, protocol.ErrNonceTooLow) {
			// New head notification data race between the transaction pool and miner, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "err", err)
		} else if errors.Is(err, protocol.ErrNonceTooHigh) {
			// Reorg notification data race between the transaction pool and miner, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
		} else if err == nil {
			// Everything ok, collect the logs and proceed to the next transaction
			logger.Trace(fmt.Sprintf("[%s] Added transaction", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "payload", ba.PayloadId)
			coalescedLogs = append(coalescedLogs, logs...)
			txnIdx++
		} else {
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction", logPrefix), "hash", txn.Hash(), "sender", from, "err", err)
		}
	}

	/*
		// Notify resubmit loop to decrease resubmitting interval if env interval is larger
		// than the user-specified one.
		if interrupt != nil {
			w.resubmitAdjustCh <- &intervalAdjust{inc: false}
		}
	*/
	return coalescedLogs, done, nil
}

func (ba BlockAssembler) AssembleBlock(stateReader state.StateReader, ibs *state.IntraBlockState, tx kv.TemporalTx, logger log.Logger) (block *types.Block, err error) {
	chainReader := NewChainReader(ba.cfg.ChainConfig, tx, ba.cfg.BlockReader, logger)

	if err := ba.cfg.Engine.Prepare(chainReader, ba.Header, ibs); err != nil {
		return nil, err
	}

	if ba.HasBAL() {
		ibs.ResetVersionedIO()
	}
	block, ba.Requests, err = protocol.FinalizeBlockExecution(ba.cfg.Engine, stateReader, ba.Header, ba.Txns, ba.Uncles,
		&state.NoopWriter{}, ba.cfg.ChainConfig, ibs, ba.Receipts, ba.Withdrawals, chainReader, true, logger, nil)

	if err != nil {
		return nil, fmt.Errorf("cannot finalize block execution: %s", err)
	}

	if ba.HasBAL() {
		ba.BlockAccessList = ba.balIO.AsBlockAccessList()
		balHash := ba.BlockAccessList.Hash()
		ba.Header.BlockAccessListHash = &balHash
	}

	return block, nil
}

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
	logger      log.Logger
}

func NewChainReader(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader, logger log.Logger) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader, logger}
}

func (cr ChainReaderImpl) Config() *chain.Config        { return cr.config }
func (cr ChainReaderImpl) CurrentHeader() *types.Header { panic("") }
func (cr ChainReaderImpl) CurrentFinalizedHeader() *types.Header {
	hash := rawdb.ReadForkchoiceFinalized(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}
	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) CurrentSafeHeader() *types.Header {
	hash := rawdb.ReadForkchoiceSafe(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}

	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)
}
func (cr ChainReaderImpl) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
		return h
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReaderImpl) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr ChainReaderImpl) FrozenBlocks() uint64 { return cr.blockReader.FrozenBlocks() }
func (cr ChainReaderImpl) FrozenBorBlocks(align bool) uint64 {
	return cr.blockReader.FrozenBorBlocks(align)
}
func (cr ChainReaderImpl) GetBlock(hash common.Hash, number uint64) *types.Block {
	b, _, _ := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b
}
func (cr ChainReaderImpl) HasBlock(hash common.Hash, number uint64) bool {
	b, _ := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return b != nil
}
