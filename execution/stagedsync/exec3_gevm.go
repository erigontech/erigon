package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	gevmadapter "github.com/erigontech/erigon/execution/vm/gevm"
)

func (se *serialExecutor) executeBlockGevm(ctx context.Context, tasks []exec.Task, isInitialCycle bool) (bool, error) {
	if se.blockExecMetrics == nil {
		se.blockExecMetrics = newBlockExecMetrics()
	}
	defer func(t time.Time) {
		se.blockExecMetrics.BlockCount.Add(1)
		se.blockExecMetrics.Duration.Add(time.Since(t))
	}(time.Now())

	blockReceipts := make(types.Receipts, 0, len(tasks))
	var startTxIndex int
	if len(tasks) > 0 {
		startTxIndex = max(tasks[0].(*exec.TxTask).TxIndex, 0)
	}

	var blockExec *gevmadapter.Executor
	var signer *types.Signer
	var blockRules *chain.Rules
	initBlockExec := func(txTask *exec.TxTask, runSystemCalls bool) error {
		se.onBlockStart(ctx, txTask.BlockNumber(), txTask.BlockHash())
		signer = types.MakeSigner(se.cfg.chainConfig, txTask.BlockNumber(), txTask.Header.Time)
		blockRules = txTask.Rules()
		blockExec = gevmadapter.NewExecutorForDomains(se.applyTx, se.rs.Domains(), nil, txTask.EvmBlockContext, se.cfg.chainConfig, txTask.TracingHooks())
		if !runSystemCalls {
			return nil
		}
		if se.cfg.chainConfig.IsCancun(txTask.Header.Time) && txTask.Header.ParentBeaconBlockRoot != nil {
			if _, err := blockExec.SystemCall(params.BeaconRootsAddress, txTask.Header.ParentBeaconBlockRoot.Bytes()); err != nil {
				se.logger.Warn("Failed to call beacon roots contract", "err", err)
			}
		}
		if se.cfg.chainConfig.IsPrague(txTask.Header.Time) {
			if err := blockExec.StoreBlockHash(txTask.Header); err != nil {
				blockExec.Close()
				return err
			}
		}
		return nil
	}
	for _, task := range tasks {
		txTask := task.(*exec.TxTask)
		txTask.Config = se.cfg.chainConfig
		txTask.Engine = se.cfg.engine

		result := &exec.TxResult{Task: txTask}
		switch {
		case txTask.TxIndex == -1:
			if txTask.BlockNumber() == 0 {
				if err := se.applyGenesisGevm(txTask); err != nil {
					return false, err
				}
				break
			}
			if err := initBlockExec(txTask, true); err != nil {
				return false, err
			}
		case txTask.IsBlockEnd():
			if txTask.BlockNumber() == 0 {
				result.TraceTos = map[accounts.Address]struct{}{}
				break
			}
			if blockExec == nil {
				if err := initBlockExec(txTask, startTxIndex == 0); err != nil {
					return false, err
				}
			}
			if blockExec != nil {
				if blockRules == nil {
					blockRules = txTask.Rules()
				}
				syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
					return blockExec.SystemCall(contract, data)
				}
				rewards, err := se.cfg.engine.CalculateRewards(se.cfg.chainConfig, txTask.Header, txTask.Uncles, syscall)
				if err != nil {
					blockExec.Close()
					return false, err
				}
				for _, r := range rewards {
					if err := blockExec.AddBalance(r.Beneficiary, r.Amount); err != nil {
						blockExec.Close()
						return false, err
					}
				}
				if misc.IsPoSHeader(txTask.Header) {
					if err := blockExec.FinalizePostMerge(se.cfg.chainConfig, txTask.Header, blockReceipts, txTask.Withdrawals); err != nil {
						blockExec.Close()
						return false, err
					}
				}
				blockExec.SetWriter(state.NewWriter(se.doms.AsPutDel(se.applyTx), se.accumulator, txTask.TxNum))
				if err := blockExec.CommitBlock(blockRules); err != nil {
					blockExec.Close()
					return false, err
				}
				blockExec.Close()
			}
			result.TraceTos = map[accounts.Address]struct{}{}
		default:
			if blockExec == nil {
				if err := initBlockExec(txTask, startTxIndex == 0); err != nil {
					return false, err
				}
			}
			result.ExecutionResult, result.Logs, result.Err = blockExec.ExecuteTransaction(txTask.Tx(), *signer)
		}

		if err := se.handleGevmResult(ctx, txTask, result, &blockReceipts, startTxIndex, isInitialCycle); err != nil {
			if blockExec != nil {
				blockExec.Close()
			}
			return false, err
		}
		if task.IsBlockEnd() {
			se.executedGas.Add(int64(max(se.blockGasUsed, se.blockStateGasUsed)))
			se.blockGasUsed = 0
			se.blockStateGasUsed = 0
			se.blobGasUsed = 0
			blockExec = nil
			signer = nil
			blockRules = nil
		}
	}
	return true, nil
}

func (se *serialExecutor) applyGenesisGevm(txTask *exec.TxTask) error {
	if se.cfg.genesis == nil {
		return nil
	}
	writer := state.NewWriter(se.doms.AsPutDel(se.applyTx), se.accumulator, txTask.TxNum)
	empty := accounts.Account{CodeHash: accounts.EmptyCodeHash}
	for addr, alloc := range se.cfg.genesis.Alloc {
		var balance uint256.Int
		if alloc.Balance != nil {
			var overflow bool
			b, overflow := uint256.FromBig(alloc.Balance)
			if overflow {
				return fmt.Errorf("genesis balance overflow for %x", addr)
			}
			balance = *b
		}
		address := accounts.InternAddress(addr)
		account := accounts.Account{
			Nonce:    alloc.Nonce,
			Balance:  balance,
			CodeHash: accounts.EmptyCodeHash,
		}
		if len(alloc.Code) > 0 || len(alloc.Storage) > 0 {
			account.Incarnation = state.FirstContractIncarnation
		}
		if len(alloc.Code) > 0 {
			account.CodeHash = accounts.InternCodeHash(crypto.HashData(alloc.Code))
			if err := writer.UpdateAccountCode(address, state.FirstContractIncarnation, account.CodeHash, alloc.Code); err != nil {
				return err
			}
		}
		if err := writer.UpdateAccountData(address, &empty, &account); err != nil {
			return err
		}
		for key, value := range alloc.Storage {
			var slot uint256.Int
			slot.SetBytes(value.Bytes())
			if err := writer.WriteAccountStorage(address, account.Incarnation, accounts.InternKey(key), uint256.Int{}, slot); err != nil {
				return err
			}
		}
	}
	return nil
}

func (se *serialExecutor) handleGevmResult(ctx context.Context, txTask *exec.TxTask, result *exec.TxResult, blockReceipts *types.Receipts, startTxIndex int, isInitialCycle bool) error {
	if errors.Is(result.Err, context.Canceled) {
		return result.Err
	}
	if result.Err != nil {
		return fmt.Errorf("%w, txnIdx=%d, %v", rules.ErrInvalidBlock, txTask.TxIndex, result.Err)
	}

	se.txCount++
	se.blockGasUsed += result.ExecutionResult.BlockRegularGasUsed
	se.blockStateGasUsed += result.ExecutionResult.BlockStateGasUsed
	if txTask.Tx() != nil {
		se.blobGasUsed += txTask.Tx().GetBlobGas()
	}

	var applyReceipt *types.Receipt
	if txTask.TxIndex >= 0 && !txTask.IsBlockEnd() {
		var prev *types.Receipt
		if txTask.TxIndex > 0 && txTask.TxIndex-startTxIndex > 0 {
			prev = (*blockReceipts)[txTask.TxIndex-startTxIndex-1]
		}
		receipt, err := createGevmNextReceipt(result, prev)
		if err != nil {
			return err
		}
		if hooks := result.TracingHooks(); hooks != nil && hooks.OnTxEnd != nil {
			hooks.OnTxEnd(receipt, result.Err)
		}
		*blockReceipts = append(*blockReceipts, receipt)
		applyReceipt = receipt
	} else if txTask.IsBlockEnd() {
		checkBloom := !se.cfg.vmConfig.StatelessExec && !se.cfg.vmConfig.NoReceipts
		checkReceipts := checkBloom && se.cfg.chainConfig.IsByzantium(txTask.BlockNumber())
		if txTask.BlockNumber() > 0 && startTxIndex == 0 {
			blockGasUsed := max(se.blockGasUsed, se.blockStateGasUsed)
			if err := protocol.BlockPostValidation(blockGasUsed, se.blobGasUsed, checkReceipts, checkBloom, *blockReceipts, txTask.Header, txTask.Txs, se.cfg.chainConfig, se.logger); err != nil {
				return fmt.Errorf("%w, txnIdx=%d, %w", rules.ErrInvalidBlock, txTask.TxIndex, err)
			}
		}
		if startTxIndex == 0 && !isInitialCycle {
			se.cfg.notifications.RecentReceipts.Add(*blockReceipts, txTask.Txs, txTask.Header)
		}
		if se.cfg.chainConfig.Bor != nil && txTask.TxIndex >= 1 && len(*blockReceipts) > 0 {
			applyReceipt = (*blockReceipts)[len(*blockReceipts)-1]
		}
	}

	if !txTask.HistoryExecution {
		if err := se.rs.ApplyStateWrites(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum, nil,
			txTask.BalanceIncreaseSet, txTask.Rules(), nil); err != nil {
			return err
		}
		if err := se.rs.ApplyTxIndexes(se.applyTx, txTask.TxNum, applyReceipt, se.blobGasUsed,
			result.Logs, result.TraceFroms, result.TraceTos); err != nil {
			return err
		}
		if err := se.rs.CommitStepBoundary(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum); err != nil {
			return err
		}
	}
	se.doms.SetTxNum(txTask.TxNum)
	se.lastBlockResult = &blockResult{BlockNum: txTask.BlockNumber(), lastTxNum: txTask.TxNum}
	se.lastExecutedTxNum.Store(int64(txTask.TxNum))
	se.lastExecutedBlockNum.Store(int64(txTask.BlockNumber()))
	return nil
}

func createGevmNextReceipt(result *exec.TxResult, prev *types.Receipt) (*types.Receipt, error) {
	txIndex := result.Task.Version().TxIndex
	if txIndex < 0 || result.IsBlockEnd() {
		return nil, nil
	}

	var cumulativeGasUsed uint64
	var firstLogIndex uint32
	if txIndex > 0 && prev != nil {
		cumulativeGasUsed = prev.CumulativeGasUsed
		firstLogIndex = prev.FirstLogIndexWithinBlock + uint32(len(prev.Logs))
	}
	cumulativeGasUsed += result.ExecutionResult.ReceiptGasUsed

	logIndex := firstLogIndex
	for i := range result.Logs {
		result.Logs[i].Index = hexutil.Uint(logIndex)
		logIndex++
	}

	blockNum := result.Version().BlockNum
	receipt := &types.Receipt{
		BlockNumber:              uint256.NewInt(blockNum),
		BlockHash:                result.BlockHash(),
		TransactionIndex:         uint(txIndex),
		Type:                     result.TxType(),
		GasUsed:                  result.ExecutionResult.ReceiptGasUsed,
		CumulativeGasUsed:        cumulativeGasUsed,
		TxHash:                   result.TxHash(),
		Logs:                     result.Logs,
		FirstLogIndexWithinBlock: firstLogIndex,
	}
	for _, l := range receipt.Logs {
		l.TxHash = receipt.TxHash
		l.BlockNumber = hexutil.Uint64(blockNum)
		l.BlockHash = receipt.BlockHash
	}
	if result.ExecutionResult.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	if tx := result.Tx(); tx != nil && tx.GetTo() == nil {
		txSender, err := result.TxSender()
		if err != nil {
			return nil, err
		}
		receipt.ContractAddress = types.CreateAddress(txSender.Value(), tx.GetNonce())
	}
	result.Receipt = receipt
	return receipt, nil
}
