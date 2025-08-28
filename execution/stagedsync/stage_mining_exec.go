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

package stagedsync

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/txnprovider"
)

type MiningExecCfg struct {
	db          kv.RwDB
	miningState MiningState
	notifier    ChainEventNotifier
	chainConfig *chain.Config
	engine      consensus.Engine
	blockReader services.FullBlockReader
	vmConfig    *vm.Config
	tmpdir      string
	interrupt   *int32
	payloadId   uint64
	txnProvider txnprovider.TxnProvider
}

func StageMiningExecCfg(
	db kv.RwDB,
	miningState MiningState,
	notifier ChainEventNotifier,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	tmpdir string,
	interrupt *int32,
	payloadId uint64,
	txnProvider txnprovider.TxnProvider,
	blockReader services.FullBlockReader,
) MiningExecCfg {
	return MiningExecCfg{
		db:          db,
		miningState: miningState,
		notifier:    notifier,
		chainConfig: chainConfig,
		engine:      engine,
		blockReader: blockReader,
		vmConfig:    vmConfig,
		tmpdir:      tmpdir,
		interrupt:   interrupt,
		payloadId:   payloadId,
		txnProvider: txnProvider,
	}
}

// SpawnMiningExecStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningExecStage(s *StageState, txc wrap.TxContainer, cfg MiningExecCfg, sendersCfg SendersCfg, execCfg ExecuteBlockCfg, ctx context.Context, logger log.Logger, u Unwinder) error {
	cfg.vmConfig.NoReceipts = false
	chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock
	preparedTxns := current.PreparedTxns

	var (
		stateReader state.StateReader
	)
	stateReader = state.NewReaderV3(txc.Doms.AsGetter(txc.Tx))
	ibs := state.New(stateReader)
	// Clique consensus needs forced author in the evm context
	//if cfg.chainConfig.Consensus == chain.CliqueConsensus {
	//	execCfg.author = &cfg.miningState.MiningConfig.Etherbase
	//}
	execCfg.author = &cfg.miningState.MiningConfig.Etherbase

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		if execCfg.blockReader == nil {
			return rawdb.ReadHeader(txc.Tx, hash, number), nil
		}
		return execCfg.blockReader.Header(ctx, txc.Tx, hash, number)
	}

	mb := membatchwithdb.NewMemoryBatch(txc.Tx, cfg.tmpdir, logger)
	defer mb.Close()
	sd, err := dbstate.NewSharedDomains(mb, logger)
	if err != nil {
		return err
	}
	defer sd.Close()

	txNum := sd.TxNum()

	if len(preparedTxns) > 0 {
		logs, _, err := addTransactionsToMiningBlock(ctx, logPrefix, current, cfg.chainConfig, cfg.vmConfig, getHeader, cfg.engine, preparedTxns, cfg.miningState.MiningConfig.Etherbase, ibs, cfg.interrupt, cfg.payloadId, logger)
		if err != nil {
			return err
		}
		NotifyPendingLogs(logPrefix, cfg.notifier, logs, logger)
	} else {

		yielded := mapset.NewSet[[32]byte]()
		var simStateReader state.StateReader
		var simStateWriter state.StateWriter

		simStateWriter = state.NewWriter(sd.AsPutDel(txc.Tx), nil, txNum)
		simStateReader = state.NewReaderV3(sd.AsGetter(txc.Tx))

		executionAt, err := s.ExecutionAt(mb)
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
				logs, stop, err := addTransactionsToMiningBlock(ctx, logPrefix, current, cfg.chainConfig, cfg.vmConfig, getHeader, cfg.engine, txns, cfg.miningState.MiningConfig.Etherbase, ibs, interrupt, cfg.payloadId, logger)
				if err != nil {
					return err
				}
				NotifyPendingLogs(logPrefix, cfg.notifier, logs, logger)
				if stop {
					break
				}
			} else {
				break
			}

			// if we yielded less than the count we wanted, assume the txpool has run dry now and stop to save another loop
			if len(txns) < amount {
				if interrupt != nil && atomic.LoadInt32(interrupt) == 0 {
					// if we are in interrupt mode, then keep on poking the txpool until we get interrupted
					// since there may be new txns that can arrive
					time.Sleep(50 * time.Millisecond)
				} else {
					break
				}
			}
		}

		metrics.UpdateBlockProducerProductionDelay(current.ParentHeaderTime, current.Header.Number.Uint64(), logger)
	}

	logger.Debug("SpawnMiningExecStage", "block", current.Header.Number, "txn", current.Txns.Len(), "payload", cfg.payloadId)
	if current.Uncles == nil {
		current.Uncles = []*types.Header{}
	}
	if current.Txns == nil {
		current.Txns = []types.Transaction{}
	}
	if current.Receipts == nil {
		current.Receipts = types.Receipts{}
	}
	chainReader := ChainReaderImpl{config: cfg.chainConfig, tx: txc.Tx, blockReader: cfg.blockReader, logger: logger}

	if err := cfg.engine.Prepare(chainReader, current.Header, ibs); err != nil {
		return err
	}

	var block *types.Block
	block, current.Requests, err = core.FinalizeBlockExecution(cfg.engine, stateReader, current.Header, current.Txns, current.Uncles, &state.NoopWriter{}, cfg.chainConfig, ibs, current.Receipts, current.Withdrawals, chainReader, true, logger, nil)
	if err != nil {
		return fmt.Errorf("cannot finalize block execution: %s", err)
	}

	// Simulate the block execution to get the final state root
	if err = rawdb.WriteHeader(txc.Tx, block.Header()); err != nil {
		return fmt.Errorf("cannot write header: %s", err)
	}
	blockHeight := block.NumberU64()

	if err = rawdb.WriteCanonicalHash(txc.Tx, block.Hash(), blockHeight); err != nil {
		return fmt.Errorf("cannot write canonical hash: %s", err)
	}
	if err = rawdb.WriteHeadHeaderHash(txc.Tx, block.Hash()); err != nil {
		return err
	}
	if _, err = rawdb.WriteRawBodyIfNotExists(txc.Tx, block.Hash(), blockHeight, block.RawBody()); err != nil {
		return fmt.Errorf("cannot write body: %s", err)
	}
	if err = rawdb.AppendCanonicalTxNums(txc.Tx, blockHeight); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(txc.Tx, kv.Headers, blockHeight); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(txc.Tx, stages.Bodies, blockHeight); err != nil {
		return err
	}
	senderS := &StageState{state: s.state, ID: stages.Senders, BlockNumber: blockHeight - 1}
	if err = SpawnRecoverSendersStage(sendersCfg, senderS, nil, txc.Tx, blockHeight, ctx, logger); err != nil {
		return err
	}

	// This flag will skip checking the state root
	execCfg.blockProduction = true
	execS := &StageState{state: s.state, ID: stages.Execution, BlockNumber: blockHeight - 1}
	if err = ExecBlockV3(execS, u, txc, blockHeight, context.Background(), execCfg, false, logger, true); err != nil {
		logger.Error("cannot execute block execution", "err", err)
		return err
	}

	rh, err := txc.Doms.ComputeCommitment(ctx, true, blockHeight, txNum, s.LogPrefix())
	if err != nil {
		return fmt.Errorf("ParallelExecutionState.Apply: %w", err)
	}
	current.Header.Root = common.BytesToHash(rh)

	logger.Info("FinalizeBlockExecution", "block", current.Header.Number, "txn", current.Txns.Len(), "gas", current.Header.GasUsed, "receipt", current.Receipts.Len(), "payload", cfg.payloadId)

	return nil
}

func getNextTransactions(
	ctx context.Context,
	cfg MiningExecCfg,
	chainID *uint256.Int,
	header *types.Header,
	amount int,
	executionAt uint64,
	alreadyYielded mapset.Set[[32]byte],
	simStateReader state.StateReader,
	simStateWriter state.StateWriter,
	logger log.Logger,
) ([]types.Transaction, error) {
	availableRlpSpace := cfg.miningState.MiningBlock.AvailableRlpSpace(cfg.chainConfig)
	remainingGas := header.GasLimit - header.GasUsed
	remainingBlobGas := uint64(0)
	if header.BlobGasUsed != nil {
		remainingBlobGas = cfg.chainConfig.GetMaxBlobGasPerBlock(header.Time) - *header.BlobGasUsed
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
		sender, ok := transaction.GetSender()
		if !ok {
			transactions = transactions[1:]
			noSenderCnt++
			continue
		}
		account, err := simStateReader.ReadAccountData(sender)
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
				code, err := simStateReader.ReadAccountCode(sender)
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
				if err := core.CheckEip1559TxGasFeeCap(sender, transaction.GetFeeCap(), transaction.GetTipCap(), baseFee256, false /* isFree */); err != nil {
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
		if err := simStateWriter.UpdateAccountData(sender, account, newAccount); err != nil {
			return nil, err
		}
		// Mark transaction as valid
		filtered = append(filtered, transaction)
		transactions = transactions[1:]
	}
	logger.Info("Filtration", "initial", initialCnt, "no sender", noSenderCnt, "no account", noAccountCnt, "nonce too low", nonceTooLowCnt, "nonceTooHigh", missedTxs, "sender not EOA", notEOACnt, "fee too low", feeTooLowCnt, "overflow", overflowCnt, "balance too low", balanceTooLowCnt, "bad chain id", badChainId, "filtered", len(filtered))
	return filtered, nil
}

func addTransactionsToMiningBlock(
	ctx context.Context,
	logPrefix string,
	current *MiningBlock,
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	getHeader func(hash common.Hash, number uint64) (*types.Header, error),
	engine consensus.Engine,
	txns types.Transactions,
	coinbase common.Address,
	ibs *state.IntraBlockState,
	interrupt *int32,
	payloadId uint64,
	logger log.Logger,
) (types.Logs, bool, error) {
	header := current.Header
	txnIdx := ibs.TxnIndex() + 1
	gasPool := new(core.GasPool).AddGas(header.GasLimit - header.GasUsed)
	if header.BlobGasUsed != nil {
		gasPool.AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time) - *header.BlobGasUsed)
	}
	signer := types.MakeSigner(chainConfig, header.Number.Uint64(), header.Time)

	var coalescedLogs types.Logs
	noop := state.NewNoopWriter()

	var miningCommitTx = func(txn types.Transaction, coinbase common.Address, vmConfig *vm.Config, chainConfig *chain.Config, ibs *state.IntraBlockState, current *MiningBlock) ([]*types.Log, error) {
		ibs.SetTxContext(current.Header.Number.Uint64(), txnIdx)
		gasSnap := gasPool.Gas()
		blobGasSnap := gasPool.BlobGas()
		snap := ibs.Snapshot()

		if txn.Type() == types.AccountAbstractionTxType {
			aaTxn := txn.(*types.AccountAbstractionTransaction)
			blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, &coinbase, chainConfig)
			evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, *vmConfig)
			paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, ibs, gasPool, header, evm, chainConfig)
			if err != nil {
				ibs.RevertToSnapshot(snap, err)
				gasPool = new(core.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
				return nil, err
			}

			status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, paymasterContext, validationGasUsed, gasPool, evm, header, ibs)
			if err != nil {
				ibs.RevertToSnapshot(snap, err)
				gasPool = new(core.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
				return nil, err
			}

			header.GasUsed += gasUsed
			logs := ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), header.Number.Uint64(), header.Hash())
			receipt := aa.CreateAAReceipt(txn.Hash(), status, gasUsed, header.GasUsed, header.Number.Uint64(), uint64(ibs.TxnIndex()), logs)

			current.AddTxn(txn)
			current.Receipts = append(current.Receipts, receipt)
			return receipt.Logs, nil
		}

		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, &coinbase, gasPool, ibs, noop, header, txn, &header.GasUsed, header.BlobGasUsed, *vmConfig)
		if err != nil {
			ibs.RevertToSnapshot(snap, err)
			gasPool = new(core.GasPool).AddGas(gasSnap).AddBlobGas(blobGasSnap) // restore gasPool as well as ibs
			return nil, err
		}

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

		if interrupt != nil && atomic.LoadInt32(interrupt) != 0 && stopped == nil {
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
		logs, err := miningCommitTx(txn, coinbase, vmConfig, chainConfig, ibs, current)
		if errors.Is(err, core.ErrGasLimitReached) {
			// Skip the env out-of-gas transaction
			logger.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash(), "sender", from)
		} else if errors.Is(err, core.ErrNonceTooLow) {
			// New head notification data race between the transaction pool and miner, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "err", err)
		} else if errors.Is(err, core.ErrNonceTooHigh) {
			// Reorg notification data race between the transaction pool and miner, skip
			logger.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
		} else if err == nil {
			// Everything ok, collect the logs and proceed to the next transaction
			logger.Trace(fmt.Sprintf("[%s] Added transaction", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce(), "payload", payloadId)
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

func NotifyPendingLogs(logPrefix string, notifier ChainEventNotifier, logs types.Logs, logger log.Logger) {
	if len(logs) == 0 {
		return
	}

	if notifier == nil {
		logger.Debug(fmt.Sprintf("[%s] rpc notifier is not set, rpc daemon won't be updated about pending logs", logPrefix))
		return
	}
	notifier.OnNewPendingLogs(logs)
}
