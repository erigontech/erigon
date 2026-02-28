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
	context0 "context"
	"fmt"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
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
func SpawnBuilderExecStage(ctx context0.Context, s *stagedsync.StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, cfg BuilderExecCfg, sendersCfg stagedsync.SendersCfg, execCfg stagedsync.ExecuteBlockCfg, logger log.Logger, u stagedsync.Unwinder) (err error) {
	cfg.vmConfig.NoReceipts = false
	chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
	logPrefix := s.LogPrefix()
	current := cfg.builderState.BuiltBlock

	stateReader := state.NewReaderV3(sd.AsGetter(tx))
	ibs := state.New(stateReader)
	defer ibs.Release(false)
	ibs.SetTxContext(current.Header.Number.Uint64(), -1)

	ba := exec.NewBlockAssembler(exec.AssemblerCfg{
		ChainConfig:     cfg.chainConfig,
		Engine:          cfg.engine,
		BlockReader:     cfg.blockReader,
		ExperimentalBAL: execCfg.IsExperimentalBAL(),
	}, cfg.payloadId, current.ParentHeaderTime, current.Header, current.Uncles, current.Withdrawals)

	if ba.HasBAL() {
		ibs.SetVersionMap(state.NewVersionMap(nil))
	}

	execCfg = execCfg.WithAuthor(accounts.InternAddress(cfg.builderState.BuilderConfig.Etherbase))

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		if execCfg.BlockReader() == nil {
			return rawdb.ReadHeader(tx, hash, number), nil
		}
		return execCfg.BlockReader().Header(ctx, tx, hash, number)
	}

	mb := membatchwithdb.NewMemoryBatch(tx, cfg.tmpdir, logger)
	defer mb.Close()
	simSd, err := execctx.NewSharedDomains(ctx, mb, logger)
	if err != nil {
		return err
	}
	defer simSd.Close()

	txNum, _, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}

	ba.Initialize(ibs, tx, logger)

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
			logs, stop, err := ba.AddTransactions(ctx, getHeader, txns, coinbase, cfg.vmConfig, ibs, interrupt, logPrefix, logger)
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

	logger.Debug("SpawnBuilderExecStage", "block", current.Header.Number, "txn", ba.Txns.Len(), "payload", cfg.payloadId)
	if ba.Uncles == nil {
		ba.Uncles = []*types.Header{}
	}
	if ba.Txns == nil {
		ba.Txns = []types.Transaction{}
	}
	if ba.Receipts == nil {
		ba.Receipts = types.Receipts{}
	}

	block, err := ba.AssembleBlock(stateReader, ibs, tx, logger)
	if err != nil {
		return err
	}

	// Copy results back to BuiltBlock
	current.Txns = ba.Txns
	current.Receipts = ba.Receipts
	current.Requests = ba.Requests
	current.BlockAccessList = ba.BlockAccessList

	// Note: This gets reset in BuilderFinish - but we need it here to
	// process execv3 - when we remove that this becomes redundant
	header := block.HeaderNoCopy()

	if execCfg.ChainConfig().IsPrague(header.Time) {
		hash := common.Hash{}
		if len(current.Requests) > 0 {
			hash = *current.Requests.Hash()
		}
		header.RequestsHash = &hash
	}

	blockHeight := block.NumberU64()

	writeBlockForExecution := func(rwTx kv.TemporalRwTx) error {
		if err = rawdb.WriteHeader(rwTx, block.Header()); err != nil {
			return fmt.Errorf("cannot write header: %s", err)
		}
		if err = rawdb.WriteCanonicalHash(rwTx, block.Hash(), blockHeight); err != nil {
			return fmt.Errorf("cannot write canonical hash: %s", err)
		}
		if err = rawdb.WriteHeadHeaderHash(rwTx, block.Hash()); err != nil {
			return err
		}
		if _, err = rawdb.WriteRawBodyIfNotExists(rwTx, block.Hash(), blockHeight, block.RawBody()); err != nil {
			return fmt.Errorf("cannot write body: %s", err)
		}
		if err = rawdb.AppendCanonicalTxNums(rwTx, blockHeight); err != nil {
			return err
		}
		if err = stages.SaveStageProgress(rwTx, kv.Headers, blockHeight); err != nil {
			return err
		}
		if err = stages.SaveStageProgress(rwTx, stages.Bodies, blockHeight); err != nil {
			return err
		}
		senderS := &stagedsync.StageState{State: s.State, ID: stages.Senders, BlockNumber: blockHeight - 1}
		if err = stagedsync.SpawnRecoverSendersStage(sendersCfg, senderS, nil, rwTx, blockHeight, ctx, logger); err != nil {
			return err
		}
		return nil
	}

	// Simulate the block execution to get the final state root
	if err = writeBlockForExecution(tx); err != nil {
		return err
	}

	// This flag will skip checking the state root
	execS := &stagedsync.StageState{State: s.State, ID: stages.Execution, BlockNumber: blockHeight - 1}
	forceParallel := dbg.Exec3Parallel || cfg.chainConfig.IsAmsterdam(current.Header.Time)
	execTx := tx
	execSd := sd
	var execCleanup func()
	if forceParallel {
		// get the underlying TemporalTx from MemoryMutation and create temporary SharedDomain
		if _, ok := tx.(*temporal.RwTx); !ok {
			type txUnwrapper interface {
				UnderlyingTx() kv.TemporalTx
			}
			if unwrap, ok := tx.(txUnwrapper); ok {
				if rwTx, ok := unwrap.UnderlyingTx().(kv.TemporalRwTx); ok {
					tempSd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
					if err != nil {
						return err
					}
					execTx = rwTx
					execSd = tempSd
					execCleanup = func() {
						tempSd.Close()
					}
					if err = writeBlockForExecution(execTx); err != nil {
						execCleanup()
						return err
					}
				}
			}
		}
		if _, ok := execTx.(*temporal.RwTx); !ok {
			return fmt.Errorf("parallel execution requires *temporal.RwTx, got %T", execTx)
		}
	}
	if execCleanup != nil {
		defer execCleanup()
	}

	if err = stagedsync.ExecV3(ctx, execS, u, execCfg, execSd, execTx, forceParallel, blockHeight, logger); err != nil {
		logger.Error("cannot execute block execution", "err", err)
		return err
	}

	commitmentTxNum, _, err := execSd.SeekCommitment(ctx, execTx)
	if err != nil {
		return fmt.Errorf("seek commitment failed: %w", err)
	}
	rh, err := execSd.ComputeCommitment(ctx, execTx, true, blockHeight, commitmentTxNum, s.LogPrefix(), nil)
	if err != nil {
		return fmt.Errorf("compute commitment failed: %w", err)
	}
	current.Header.Root = common.BytesToHash(rh)

	logger.Info("FinalizeBlockExecution", "block", current.Header.Number, "txn", current.Txns.Len(), "gas", current.Header.GasUsed, "receipt", current.Receipts.Len(), "payload", cfg.payloadId)

	return nil
}

func getNextTransactions(
	ctx context0.Context,
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

	allTxns, err := cfg.txnProvider.ProvideTxns(ctx, provideOpts...)
	if err != nil {
		return nil, err
	}

	blockNum := executionAt + 1
	txns, err := filterBadTransactions(allTxns, chainID, cfg.chainConfig, blockNum, header, simStateReader, simStateWriter, logger)
	if err != nil {
		return nil, err
	}

	// Remove nonce-too-high transactions from alreadyYielded so they can be reconsidered
	// in subsequent iterations. When best() skips blob TXs that exceed remaining blob gas,
	// it can create nonce gaps in the returned set. filterBadTransactions rejects the
	// higher-nonce TXs, but they get stuck in the yielded set and are never returned again.
	// By removing only nonce-too-high TXs (nonce > sim state nonce), we allow them to be
	// reconsidered after earlier-nonce TXs are accepted. TXs rejected for other reasons
	// (nonce-too-low, fee-too-low, etc.) remain yielded to avoid infinite re-evaluation.
	if len(txns) < len(allTxns) && alreadyYielded != nil {
		accepted := make(map[[32]byte]struct{}, len(txns))
		for _, tx := range txns {
			accepted[tx.Hash()] = struct{}{}
		}
		for _, tx := range allTxns {
			h := tx.Hash()
			if _, ok := accepted[h]; ok {
				continue
			}
			sender, ok := tx.GetSender()
			if !ok {
				continue
			}
			account, err := simStateReader.ReadAccountData(sender)
			if err != nil || account == nil {
				continue
			}
			if tx.GetNonce() > account.Nonce {
				alreadyYielded.Remove(h)
			}
		}
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
			baseFee256 := header.BaseFee
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
