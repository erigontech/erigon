package builder

import (
	context0 "context"
	"errors"
	"fmt"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	gevmadapter "github.com/erigontech/erigon/execution/vm/gevm"
)

func execBlockGevm(ctx context0.Context, sd *execctx.SharedDomains, tx kv.TemporalTx, executionAt uint64, cfg BuilderExecCfg, execCfg stagedsync.ExecuteBlockCfg, logger log.Logger) error {
	const logPrefix = "BuilderExec"

	chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
	current := cfg.builderState.BuiltBlock
	current.PayloadId = cfg.payloadId

	txNum, _, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	sd.SetTxNum(txNum)
	sd.GetCommitmentContext().SetDeferBranchUpdates(false)

	filterMb, err := membatchwithdb.NewMemoryBatch(tx, cfg.tmpdir, logger)
	if err != nil {
		return err
	}
	defer filterMb.Close()
	filterSd, err := execctx.NewSharedDomains(ctx, filterMb, logger)
	if err != nil {
		return err
	}
	defer filterSd.Close()
	filterWriter := state.NewWriter(filterSd.AsPutDel(filterMb), nil, txNum)
	filterReader := state.NewReaderV3(filterSd.AsGetter(filterMb))

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		if execCfg.BlockReader() == nil {
			return rawdb.ReadHeader(tx, hash, number), nil
		}
		return execCfg.BlockReader().Header(ctx, tx, hash, number)
	}

	coinbase := accounts.InternAddress(cfg.builderState.BuilderConfig.Etherbase)
	blockContext := protocol.NewEVMBlockContext(current.Header, protocol.GetHashFn(current.Header, getHeader), cfg.engine, coinbase, cfg.chainConfig)
	rules := blockContext.Rules(cfg.chainConfig)
	blockExec := gevmadapter.NewExecutorForDomains(tx, sd, nil, blockContext, cfg.chainConfig, cfg.vmConfig.Tracer)
	defer blockExec.Close()

	if cfg.chainConfig.IsCancun(current.Header.Time) && current.Header.ParentBeaconBlockRoot != nil {
		if _, err := blockExec.SystemCall(params.BeaconRootsAddress, current.Header.ParentBeaconBlockRoot.Bytes()); err != nil {
			logger.Warn("Failed to call beacon roots contract", "err", err)
		}
	}
	if cfg.chainConfig.IsPrague(current.Header.Time) {
		if err := blockExec.StoreBlockHash(current.Header); err != nil {
			return err
		}
	}

	gasUsed := protocol.GasUsed{}
	gasPool := new(protocol.GasPool).AddGas(current.Header.GasLimit)
	if current.Header.BlobGasUsed != nil {
		gasPool.AddBlobGas(cfg.chainConfig.GetMaxBlobGasPerBlock(current.Header.Time))
	}

	yielded := mapset.NewSet[[32]byte]()
	interrupt := cfg.interrupt
	const amount = 50
	done := false
	var stopped *time.Ticker
	defer func() {
		if stopped != nil {
			stopped.Stop()
		}
	}()

	for !done {
		txns, err := getNextTransactions(ctx, cfg, chainID, current.Header, gasUsed, amount, executionAt, yielded, filterReader, filterWriter, logger)
		if err != nil {
			return err
		}
		for _, txn := range txns {
			if stopped != nil {
				select {
				case <-stopped.C:
					done = true
					break
				default:
				}
			}
			if done {
				break
			}
			if err := common.Stopped(ctx.Done()); err != nil {
				return err
			}
			if interrupt != nil && interrupt.Load() && stopped == nil {
				logger.Debug("Transaction adding was requested to stop", "payload", current.PayloadId)
				stopped = time.NewTicker(500 * time.Millisecond)
			}
			if gasPool.Gas() < params.TxGas {
				logger.Debug(fmt.Sprintf("[%s] Not enough gas for further transactions", logPrefix), "have", gasPool, "want", params.TxGas)
				done = true
				break
			}
			if current.AvailableRlpSpace(cfg.chainConfig, txn) < 0 {
				continue
			}
			logs, receipt, err := executeBuilderTxGevm(blockExec, txn, current, cfg.vmConfig, cfg.chainConfig, rules, gasPool, &gasUsed)
			if err != nil {
				if errors.Is(err, protocol.ErrGasLimitReached) {
					logger.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash())
				} else if errors.Is(err, protocol.ErrNonceTooLow) {
					logger.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "nonce", txn.GetNonce(), "err", err)
				} else if errors.Is(err, protocol.ErrNonceTooHigh) {
					logger.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "nonce", txn.GetNonce())
				} else {
					logger.Debug(fmt.Sprintf("[%s] Skipping transaction", logPrefix), "hash", txn.Hash(), "err", err)
				}
				continue
			}
			current.AddTxn(txn)
			current.Receipts = append(current.Receipts, receipt)
			NotifyPendingLogs(logPrefix, cfg.notifier, logs, logger)
			logger.Trace(fmt.Sprintf("[%s] Added transaction", logPrefix), "hash", txn.Hash(), "nonce", txn.GetNonce(), "payload", cfg.payloadId)
		}
		if len(txns) < amount {
			if interrupt != nil && !interrupt.Load() {
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
		}
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return blockExec.SystemCall(contract, data)
	}
	rewards, err := cfg.engine.CalculateRewards(cfg.chainConfig, current.Header, current.Uncles, syscall)
	if err != nil {
		return err
	}
	for _, r := range rewards {
		if err := blockExec.AddBalance(r.Beneficiary, r.Amount); err != nil {
			return err
		}
	}
	requests, err := blockExec.FinalizePostMergeRequests(cfg.chainConfig, current.Header, current.Receipts, current.Withdrawals)
	if err != nil {
		return err
	}
	current.Requests = requests
	if cfg.chainConfig.IsPrague(current.Header.Time) {
		current.Header.RequestsHash = current.Requests.Hash()
	}

	blockExec.SetWriter(state.NewWriter(sd.AsPutDel(tx), nil, txNum))
	if err := blockExec.CommitBlock(rules); err != nil {
		return err
	}

	rh, err := sd.ComputeCommitment(ctx, tx, false, current.Header.Number.Uint64(), txNum, logPrefix, nil)
	if err != nil {
		return fmt.Errorf("compute commitment failed: %w", err)
	}
	current.Header.Root = common.BytesToHash(rh)

	metrics.UpdateBlockProducerProductionDelay(current.ParentHeaderTime, current.Header.Number.Uint64(), logger)
	logger.Info("FinalizeBlockExecution", "block", current.Header.Number, "txn", len(current.Txns), "gas", current.Header.GasUsed, "receipt", len(current.Receipts), "payload", cfg.payloadId)
	return nil
}

func executeBuilderTxGevm(blockExec *gevmadapter.Executor, txn types.Transaction, current *exec.AssembledBlock, vmConfig *vm.Config, chainConfig *chain.Config, rules *chain.Rules, gasPool *protocol.GasPool, gasUsed *protocol.GasUsed) (types.Logs, *types.Receipt, error) {
	if err := gasPool.SubGas(txn.GetGasLimit()); err != nil {
		return nil, nil, err
	}
	if txn.GetBlobGas() > 0 {
		if err := gasPool.SubBlobGas(txn.GetBlobGas()); err != nil {
			gasPool.AddGas(txn.GetGasLimit())
			return nil, nil, err
		}
	}
	msg, err := txn.AsMessage(*types.MakeSigner(chainConfig, current.Header.Number.Uint64(), current.Header.Time), current.Header.BaseFee, rules)
	if err != nil {
		gasPool.AddGas(txn.GetGasLimit())
		gasPool.AddBlobGas(txn.GetBlobGas())
		return nil, nil, err
	}
	msg.SetCheckNonce(!vmConfig.StatelessExec)
	result, logs, err := blockExec.Execute(txn, msg)
	if err != nil {
		gasPool.AddGas(txn.GetGasLimit())
		gasPool.AddBlobGas(txn.GetBlobGas())
		return nil, nil, err
	}
	if max(gasUsed.BlockRegular+result.BlockRegularGasUsed, gasUsed.BlockState+result.BlockStateGasUsed) > current.Header.GasLimit {
		gasPool.AddGas(txn.GetGasLimit())
		gasPool.AddBlobGas(txn.GetBlobGas())
		return nil, nil, protocol.ErrGasLimitReached
	}
	gasPool.AddGas(txn.GetGasLimit() - result.ReceiptGasUsed)
	gasUsed.Receipt += result.ReceiptGasUsed
	gasUsed.BlockRegular += result.BlockRegularGasUsed
	gasUsed.BlockState += result.BlockStateGasUsed
	gasUsed.Blob += txn.GetBlobGas()
	protocol.SetGasUsed(current.Header, gasUsed)

	receipt := createBuilderReceiptGevm(current, txn, msg, result, logs, gasUsed.Receipt)
	return logs, receipt, nil
}

func createBuilderReceiptGevm(current *exec.AssembledBlock, txn types.Transaction, msg *types.Message, result evmtypes.ExecutionResult, logs types.Logs, cumulativeGasUsed uint64) *types.Receipt {
	txIndex := len(current.Txns)
	firstLogIndex := uint32(0)
	if len(current.Receipts) > 0 {
		prev := current.Receipts[len(current.Receipts)-1]
		firstLogIndex = prev.FirstLogIndexWithinBlock + uint32(len(prev.Logs))
	}
	for i := range logs {
		logs[i].Index = hexutil.Uint(uint64(firstLogIndex) + uint64(i))
		logs[i].TxHash = txn.Hash()
		logs[i].BlockNumber = hexutil.Uint64(current.Header.Number.Uint64())
		logs[i].BlockHash = current.Header.Hash()
	}
	receipt := &types.Receipt{
		BlockNumber:              uint256.NewInt(current.Header.Number.Uint64()),
		BlockHash:                current.Header.Hash(),
		TransactionIndex:         uint(txIndex),
		Type:                     txn.Type(),
		GasUsed:                  result.ReceiptGasUsed,
		CumulativeGasUsed:        cumulativeGasUsed,
		TxHash:                   txn.Hash(),
		Logs:                     logs,
		FirstLogIndexWithinBlock: firstLogIndex,
	}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	if msg.To().IsNil() {
		receipt.ContractAddress = types.CreateAddress(msg.From().Value(), txn.GetNonce())
	}
	if txn.Type() == types.BlobTxType {
		receipt.BlobGasUsed = txn.GetBlobGas()
	}
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	return receipt
}
