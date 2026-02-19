// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package builderstages

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"golang.org/x/net/context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/aa"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/txnprovider"
)

type BuilderExecCfg struct {
	builderState BuilderState
	notifier     stagedsync.ChainEventNotifier
	chainConfig  *chain.Config
	engine       rules.Engine
	blockReader  services.FullBlockReader
	vmConfig     *vm.Config
	tmpdir       string
	interrupt    *atomic.Bool
	payloadId    uint64
	txnProvider  txnprovider.TxnProvider
}

func StageBuilderExecCfg(
	builderState BuilderState,
	notifier stagedsync.ChainEventNotifier,
	chainConfig *chain.Config,
	engine rules.Engine,
	vmConfig *vm.Config,
	tmpdir string,
	interrupt *atomic.Bool,
	payloadId uint64,
	txnProvider txnprovider.TxnProvider,
	blockReader services.FullBlockReader,
) BuilderExecCfg {
	return BuilderExecCfg{
		builderState: builderState,
		notifier:     notifier,
		chainConfig:  chainConfig,
		engine:       engine,
		blockReader:  blockReader,
		vmConfig:     vmConfig,
		tmpdir:       tmpdir,
		interrupt:    interrupt,
		payloadId:    payloadId,
		txnProvider:  txnProvider,
	}
}

// SpawnBuilderExecStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnBuilderExecStage(ctx context.Context, s *stagedsync.StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, cfg BuilderExecCfg, sendersCfg stagedsync.SendersCfg, execCfg stagedsync.ExecuteBlockCfg, logger log.Logger, u stagedsync.Unwinder) (err error) {
	cfg.vmConfig.NoReceipts = false
	chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
	logPrefix := s.LogPrefix()
	current := cfg.builderState.BuiltBlock
	needBAL := execCfg.ChainConfig().IsAmsterdam(current.Header.Time) || execCfg.IsExperimentalBAL()

	// Create an ephemeral SharedDomains directly on the read tx.
	// Writes accumulate in simSd's in-memory TemporalMemBatch;
	// ComputeCommitment reads from there. Nothing is flushed to the DB.
	simSd, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return err
	}
	defer simSd.Close()

	stateReader := state.NewReaderV3(simSd.AsGetter(tx))
	ibs := state.New(stateReader)
	defer ibs.Release(false)
	ibs.SetTxContext(current.Header.Number.Uint64(), -1)
	var balIO *state.VersionedIO
	var systemReads state.ReadSet
	var systemWrites state.VersionedWrites
	var systemAccess map[accounts.Address]struct{}
	if needBAL {
		ibs.SetVersionMap(state.NewVersionMap(nil))
		balIO = &state.VersionedIO{}
	}
	execCfg = execCfg.WithAuthor(accounts.InternAddress(cfg.builderState.BuilderConfig.Etherbase))

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		if execCfg.BlockReader() == nil {
			return nil, fmt.Errorf("block reader is nil")
		}
		return execCfg.BlockReader().Header(ctx, tx, hash, number)
	}

	chainReader := stagedsync.NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)

	// txNum sequencing: block-begin (+1), each user tx (+1), block-end (+1)
	txNum := sd.TxNum()
	stateWriter := state.NewWriter(simSd.AsPutDel(tx), nil, txNum)

	// Block-begin: system calls (beacon root store, etc.)
	simSd.SetTxNum(txNum)
	protocol.InitializeBlockExecution(cfg.engine, chainReader, current.Header, cfg.chainConfig, ibs, stateWriter, logger, nil)
	if needBAL {
		systemReads = stagedsync.MergeReadSets(systemReads, ibs.VersionedReads())
		systemWrites = stagedsync.MergeVersionedWrites(systemWrites, ibs.VersionedWrites(false))
		systemAccess = stagedsync.MergeAccessedAddresses(systemAccess, ibs.AccessedAddresses())
		ibs.ResetVersionedIO()
	}
	txNum++ // consumed by block-begin

	coinbase := accounts.InternAddress(cfg.builderState.BuilderConfig.Etherbase)

	yielded := mapset.NewSet[[32]byte]()
	simStateWriter := state.NewWriter(simSd.AsPutDel(tx), nil, txNum)
	simStateReader := state.NewReaderV3(simSd.AsGetter(tx))

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	interrupt := cfg.interrupt
	const amount = 50
	for {
		txns, err := getNextTransactions(ctx, cfg, chainID, current.Header, amount, executionAt, yielded, simStateReader, simStateWriter, logger)
		if err != nil {
			return err
		}

		if len(txns) > 0 {
			logs, stop, err := addTransactionsToBlock(ctx, logPrefix, current, cfg.chainConfig, cfg.vmConfig, getHeader, cfg.engine, txns, coinbase, ibs, stateWriter, &txNum, simSd, balIO, interrupt, cfg.payloadId, logger)
			if err != nil {
				return err
			}
			NotifyPendingLogs(logPrefix, cfg.notifier, logs, logger)
			if stop {
				break
			}
		}

		// if we yielded less than the count we wanted, assume the txpool has run dry now
		if len(txns) < amount {
			if interrupt != nil && !interrupt.Load() {
				// if we are in interrupt mode, then keep on poking the txpool until we get interrupted
				// since there may be new txns that can arrive
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
		}
	}

	metrics.UpdateBlockProducerProductionDelay(current.ParentHeaderTime, current.Header.Number.Uint64(), logger)

	logger.Debug("SpawnBuilderExecStage", "block", current.Header.Number, "txn", current.Txns.Len(), "payload", cfg.payloadId)
	if current.Uncles == nil {
		current.Uncles = []*types.Header{}
	}
	if current.Txns == nil {
		current.Txns = []types.Transaction{}
	}
	if current.Receipts == nil {
		current.Receipts = types.Receipts{}
	}

	if err := cfg.engine.Prepare(chainReader, current.Header, ibs); err != nil {
		return err
	}

	// Block-end: finalization (system calls, rewards, etc.)
	stateWriter.SetTxNum(txNum)
	simSd.SetTxNum(txNum)
	var block *types.Block
	if needBAL {
		ibs.ResetVersionedIO()
	}
	block, current.Requests, err = protocol.FinalizeBlockExecution(cfg.engine, stateReader, current.Header, current.Txns, current.Uncles, stateWriter, cfg.chainConfig, ibs, current.Receipts, current.Withdrawals, chainReader, true, logger, nil)
	if err != nil {
		return fmt.Errorf("cannot finalize block execution: %s", err)
	}
	txNum++ // consumed by block-end

	header := block.HeaderNoCopy()

	if execCfg.ChainConfig().IsPrague(header.Time) {
		hash := common.Hash{}
		if len(current.Requests) > 0 {
			hash = *current.Requests.Hash()
		}
		header.RequestsHash = &hash
	}

	blockHeight := block.NumberU64()
	if needBAL {
		systemReads = stagedsync.MergeReadSets(systemReads, ibs.VersionedReads())
		systemWrites = stagedsync.MergeVersionedWrites(systemWrites, ibs.VersionedWrites(false))
		systemAccess = stagedsync.MergeAccessedAddresses(systemAccess, ibs.AccessedAddresses())
		ibs.ResetVersionedIO()

		systemVersion := state.Version{BlockNum: blockHeight, TxIndex: -1}
		balIO.RecordReads(systemVersion, systemReads)
		balIO.RecordWrites(systemVersion, systemWrites)
		balIO.RecordAccesses(systemVersion, systemAccess)
		current.BlockAccessList = stagedsync.CreateBAL(blockHeight, balIO, execCfg.DirsDataDir())
		hash := current.BlockAccessList.Hash()
		header.BlockAccessListHash = &hash
	} else {
		if execCfg.ChainConfig().IsAmsterdam(current.Header.Time) {
			header.BlockAccessListHash = &empty.BlockAccessListHash
		}
	}

	// Compute state root from the ephemeral simSd — no ExecV3 re-execution needed.
	simSd.SetTxNum(txNum)
	rh, err := simSd.ComputeCommitment(ctx, tx, true, blockHeight, txNum, s.LogPrefix(), nil)
	if err != nil {
		return fmt.Errorf("compute commitment failed: %w", err)
	}
	current.Header.Root = common.BytesToHash(rh)

	logger.Info("FinalizeBlockExecution", "block", current.Header.Number, "txn", current.Txns.Len(), "gas", current.Header.GasUsed, "receipt", current.Receipts.Len(), "payload", cfg.payloadId)

	return nil
}

func getNextTransactions(
	ctx context.Context,
	cfg BuilderExecCfg,
	chainID *uint256.Int,
	header *types.Header,
	amount int,
	executionAt uint64,
	alreadyYielded mapset.Set[[32]byte],
	simStateReader state.StateReader,
	simStateWriter state.StateWriter,
	logger log.Logger,
) ([]types.Transaction, error) {
	availableRlpSpace := cfg.builderState.BuiltBlock.AvailableRlpSpace(cfg.chainConfig)
	remainingGas := header.GasLimit - header.GasUsed
	remainingBlobGas := uint64(0)
	if header.BlobGasUsed != nil {
		maxBlobs := cfg.chainConfig.GetMaxBlobsPerBlock(header.Time)
		if cfg.builderState.BuilderConfig.MaxBlobsPerBlock != nil {
			maxBlobs = min(maxBlobs, *cfg.builderState.BuilderConfig.MaxBlobsPerBlock)
		}
		remainingBlobGas = maxBlobs*params.GasPerBlob - *header.BlobGasUsed
	}

	provideOpts := []txnprovider.ProvideOption{
		txnprovider.WithAmount(amount),
		txnprovider.WithParentBlockNum(executionAt),
		txnprovider.WithBlockTime(header.Time),
		txnprovider.WithGasTarget(remainingGas),
		txnprovider.WithBlobGasTarget(remainingBlobGas),
		txnprovider.WithTxnIdsFilter(alreadyYielded),
		txnprovider.WithAvailableRlpSpace(availableRlpSpace),
	}

	txns, err := cfg.txnProvider.ProvideTxns(ctx, provideOpts...)
	if err != nil {
		return nil, err
	}

	blockNum := executionAt + 1
	txns, err = filterBadTransactions(txns, chainID, cfg.chainConfig, blockNum, header, simStateReader, simStateWriter, logger)
	if err != nil {
		return nil, err
	}

	return txns, nil
}

func filterBadTransactions(transactions []types.Transaction, chainID *uint256.Int, config *chain.Config, blockNumber uint64, header *types.Header, simStateReader state.StateReader, simStateWriter state.StateWriter, logger log.Logger) ([]types.Transaction, error) {
	initialCnt := len(transactions)
	var filtered []types.Transaction
	gasBailout := false

	missedTxs := 0
	badChainId := 0
	noSenderCnt := 0
	noAccountCnt := 0
	nonceTooLowCnt := 0
	notEOACnt := 0
	feeTooLowCnt := 0
	balanceTooLowCnt := 0
	overflowCnt := 0
	for len(transactions) > 0 && missedTxs != len(transactions) {
		transaction := transactions[0]
		transactionChainId := transaction.GetChainID()
		if !transactionChainId.IsZero() && transactionChainId.Cmp(chainID) != 0 {
			transactions = transactions[1:]
			badChainId++
			continue
		}
		senderAddress, ok := transaction.GetSender()
		if !ok {
			transactions = transactions[1:]
			noSenderCnt++
			continue
		}
		account, err := simStateReader.ReadAccountData(senderAddress)
		if err != nil {
			return nil, err
		}
		if account == nil {
			transactions = transactions[1:]
			noAccountCnt++
			continue
		}
		// Check transaction nonce
		if account.Nonce > transaction.GetNonce() {
			transactions = transactions[1:]
			nonceTooLowCnt++
			continue
		}
		if account.Nonce < transaction.GetNonce() {
			missedTxs++
			transactions = append(transactions[1:], transaction)
			continue
		}
		missedTxs = 0

		// Make sure the sender is an EOA (EIP-3607)
		if !account.IsEmptyCodeHash() && transaction.Type() != types.AccountAbstractionTxType {
			isEoaCodeAllowed := false
			if config.IsPrague(header.Time) || config.IsBhilai(header.Number.Uint64()) {
				code, err := simStateReader.ReadAccountCode(senderAddress)
				if err != nil {
					return nil, err
				}

				_, isDelegated := types.ParseDelegation(code)
				isEoaCodeAllowed = isDelegated // non-empty code allowed for eoa if it points to delegation
			}

			if !isEoaCodeAllowed {
				transactions = transactions[1:]
				notEOACnt++
				continue
			}
		}

		if config.IsLondon(blockNumber) {
			baseFee256 := uint256.NewInt(0)
			if overflow := baseFee256.SetFromBig(header.BaseFee); overflow {
				return nil, fmt.Errorf("bad baseFee %s", header.BaseFee)
			}
			// Make sure the transaction gasFeeCap is greater than the block's baseFee.
			if !transaction.GetFeeCap().IsZero() || !transaction.GetTipCap().IsZero() {
				if err := protocol.CheckEip1559TxGasFeeCap(senderAddress, transaction.GetFeeCap(), transaction.GetTipCap(), baseFee256, false /* isFree */); err != nil {
					transactions = transactions[1:]
					feeTooLowCnt++
					continue
				}
			}
		}
		txnGasLimit := transaction.GetGasLimit()
		value := transaction.GetValue()
		accountBalance := account.Balance

		want := uint256.NewInt(txnGasLimit)
		want, overflow := want.MulOverflow(want, transaction.GetFeeCap())
		if overflow {
			transactions = transactions[1:]
			overflowCnt++
			continue
		}
		want, overflow = want.AddOverflow(want, value)
		if overflow {
			transactions = transactions[1:]
			overflowCnt++
			continue
		}

		if accountBalance.Cmp(want) < 0 {
			if !gasBailout {
				transactions = transactions[1:]
				balanceTooLowCnt++
				continue
			}
		}

		newAccount := new(accounts.Account)
		*newAccount = *account
		// Updates account in the simulation
		newAccount.Nonce++
		newAccount.Balance.Sub(&account.Balance, want)
		if err := simStateWriter.UpdateAccountData(senderAddress, account, newAccount); err != nil {
			return nil, err
		}
		// Mark transaction as valid
		filtered = append(filtered, transaction)
		transactions = transactions[1:]
	}
	logger.Info("Filtration", "initial", initialCnt, "no sender", noSenderCnt, "no account", noAccountCnt, "nonce too low", nonceTooLowCnt, "nonceTooHigh", missedTxs, "sender not EOA", notEOACnt, "fee too low", feeTooLowCnt, "overflow", overflowCnt, "balance too low", balanceTooLowCnt, "bad chain id", badChainId, "filtered", len(filtered))
	return filtered, nil
}

func addTransactionsToBlock(
	ctx context.Context,
	logPrefix string,
	current *BuiltBlock,
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	getHeader func(hash common.Hash, number uint64) (*types.Header, error),
	engine rules.Engine,
	txns types.Transactions,
	coinbase accounts.Address,
	ibs *state.IntraBlockState,
	stateWriter *state.Writer,
	txNum *uint64,
	simSd *execctx.SharedDomains,
	balIO *state.VersionedIO,
	interrupt *atomic.Bool,
	payloadId uint64,
	logger log.Logger,
) (types.Logs, bool, error) {
	header := current.Header
	txnIdx := ibs.TxnIndex() + 1
	gasPool := new(protocol.GasPool).AddGas(header.GasLimit - header.GasUsed)
	if header.BlobGasUsed != nil {
		gasPool.AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time) - *header.BlobGasUsed)
	}
	signer := types.MakeSigner(chainConfig, header.Number.Uint64(), header.Time)

	var coalescedLogs types.Logs
	recordTxIO := func() {
		if balIO == nil {
			return
		}
		version := ibs.Version()
		balIO.RecordReads(version, ibs.VersionedReads())
		balIO.RecordWrites(version, ibs.VersionedWrites(false))
		balIO.RecordAccesses(version, ibs.AccessedAddresses())
		ibs.ResetVersionedIO()
	}
	clearTxIO := func() {
		if balIO == nil {
			return
		}
		ibs.AccessedAddresses()
		ibs.ResetVersionedIO()
	}

	var builderCommitTx = func(txn types.Transaction, coinbase accounts.Address, vmConfig *vm.Config, chainConfig *chain.Config, ibs *state.IntraBlockState, current *BuiltBlock) ([]*types.Log, error) {
		stateWriter.SetTxNum(*txNum)
		simSd.SetTxNum(*txNum)
		ibs.SetTxContext(current.Header.Number.Uint64(), txnIdx)
		gasSnap := gasPool.Gas()
		blobGasSnap := gasPool.BlobGas()
		snap := ibs.PushSnapshot()
		defer ibs.PopSnapshot(snap)

		if txn.Type() == types.AccountAbstractionTxType {
			aaTxn := txn.(*types.AccountAbstractionTransaction)
			blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, getHeader), engine, coinbase, chainConfig)
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
			if err = ibs.FinalizeTx(evm.ChainRules(), stateWriter); err != nil {
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
		receipt, err := protocol.ApplyTransaction(chainConfig, protocol.GetHashFn(header, getHeader), engine, coinbase, gasPool, ibs, stateWriter, header, txn, gasUsed, *vmConfig)
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
			logger.Debug("Transaction adding was requested to stop", "payload", payloadId)
			// ensure we run for at least 500ms after the request to stop comes in from GetPayload
			stopped = time.NewTicker(500 * time.Millisecond)
		}
		// If we don't have enough gas for any further transactions then we're done
		if gasPool.Gas() < params.TxGas {
			logger.Debug(fmt.Sprintf("[%s] Not enough gas for further transactions", logPrefix), "have", gasPool, "want", params.TxGas)
			done = true
			break
		}

		rlpSpacePostTxn := current.AvailableRlpSpace(chainConfig, txn)
		if rlpSpacePostTxn < 0 {
			rlpSpacePreTxn := current.AvailableRlpSpace(chainConfig)
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
		if txn.Protected() && !chainConfig.IsSpuriousDragon(header.Number.Uint64()) {
			logger.Debug(fmt.Sprintf("[%s] Ignoring replay protected transaction", logPrefix), "hash", txn.Hash(), "eip155", chainConfig.SpuriousDragonBlock)
			continue
		}

		// Start executing the transaction
		logs, err := builderCommitTx(txn, coinbase, vmConfig, chainConfig, ibs, current)
		if err == nil {
			recordTxIO()
		} else {
			clearTxIO()
		}
		if errors.Is(err, protocol.ErrGasLimitReached) {
			// Skip the env out-of-gas transaction
			logger.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash(), "sender", from)
		} else if errors.Is(err, protocol.ErrNonceTooLow) {
			// New head notification data race between the transaction pool and builder, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "err", err)
		} else if errors.Is(err, protocol.ErrNonceTooHigh) {
			// Reorg notification data race between the transaction pool and builder, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
		} else if err == nil {
			// Everything ok, collect the logs and proceed to the next transaction
			logger.Trace(fmt.Sprintf("[%s] Added transaction", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "payload", payloadId)
			coalescedLogs = append(coalescedLogs, logs...)
			txnIdx++
			*txNum++ // consumed by this user transaction
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

func NotifyPendingLogs(logPrefix string, notifier stagedsync.ChainEventNotifier, logs types.Logs, logger log.Logger) {
	if len(logs) == 0 {
		return
	}

	if notifier == nil {
		logger.Debug(fmt.Sprintf("[%s] rpc notifier is not set, rpc daemon won't be updated about pending logs", logPrefix))
		return
	}
	notifier.OnNewPendingLogs(logs)
}
