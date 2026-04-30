// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package protocol

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
)

var (
	blockExecutionTimer = metrics.GetOrCreateSummary("chain_execution_seconds")
)

type SyncMode string

const (
	// See gas_limit in https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	SysCallGasLimit = uint64(30_000_000)
)

type RejectedTx struct {
	Index int    `json:"index"    gencodec:"required"`
	Err   string `json:"error"    gencodec:"required"`
}

type RejectedTxs []*RejectedTx

type EphemeralExecResult struct {
	StateRoot        common.Hash         `json:"stateRoot"`
	TxRoot           common.Hash         `json:"txRoot"`
	ReceiptRoot      common.Hash         `json:"receiptsRoot"`
	LogsHash         common.Hash         `json:"logsHash"`
	Bloom            types.Bloom         `json:"logsBloom"        gencodec:"required"`
	Receipts         types.Receipts      `json:"receipts"`
	Rejected         RejectedTxs         `json:"rejected,omitempty"`
	Difficulty       *uint256.Int        `json:"currentDifficulty" gencodec:"required"`
	GasUsed          math.HexOrDecimal64 `json:"gasUsed"`
	StateSyncReceipt *types.Receipt      `json:"-"`
}

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerally(
	chainConfig *chain.Config, vmConfig *vm.Config,
	blockHashFunc func(n uint64) (common.Hash, error),
	engine rules.Engine, block *types.Block,
	stateReader state.StateReader, stateWriter state.StateWriter,
	chainReader rules.ChainReader, getTracer func(txIndex int, txHash common.Hash) (*tracing.Hooks, error),
	logger log.Logger,
) (res *EphemeralExecResult, executeBlockErr error) {
	defer blockExecutionTimer.ObserveDuration(time.Now())
	ibs := state.New(stateReader)
	defer ibs.Release(false)
	ibs.SetHooks(vmConfig.Tracer)
	header := block.Header()

	gasUsed := new(GasUsed)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnBlockStart != nil {
		td := chainReader.GetTd(block.ParentHash(), block.NumberU64()-1)
		vmConfig.Tracer.OnBlockStart(tracing.BlockEvent{
			Block:     block,
			TD:        td,
			Finalized: chainReader.CurrentFinalizedHeader(),
			Safe:      chainReader.CurrentSafeHeader(),
		})
	}

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnBlockEnd != nil {
		defer func() {
			vmConfig.Tracer.OnBlockEnd(executeBlockErr)
		}()
	}

	if err := InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, stateWriter, logger, vmConfig.Tracer); err != nil {
		return nil, err
	}

	var rejectedTxs []*RejectedTx
	includedTxs := make(types.Transactions, 0, block.Transactions().Len())
	receipts := make(types.Receipts, 0, block.Transactions().Len())
	blockNum := block.NumberU64()

	for i, txn := range block.Transactions() {
		ibs.SetTxContext(blockNum, i)
		writeTrace := false
		if vmConfig.Tracer == nil && getTracer != nil {
			tracer, err := getTracer(i, txn.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}
		receipt, err := ApplyTransaction(chainConfig, blockHashFunc, engine, accounts.NilAddress, gp, ibs, stateWriter, header, txn, gasUsed, *vmConfig)
		if writeTrace && vmConfig.Tracer != nil && vmConfig.Tracer.Flush != nil {
			vmConfig.Tracer.Flush(txn)
			vmConfig.Tracer = nil
		}

		if err != nil {
			if !vmConfig.StatelessExec {
				return nil, fmt.Errorf("could not apply txn %d from block %d [%v]: %w", i, block.NumberU64(), txn.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, txn)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		if dbg.LogHashMismatchReason() {
			ethutils.LogReceipts(log.LvlWarn, "receipt hash mismatch in ExecuteBlockEphemerally", receipts, includedTxs, chainConfig, header, logger)
		}

		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	// EIP-8037: compute block-level Bottleneck for Amsterdam.
	// Pre-Amsterdam: blockStateGasUsed is 0, so this is a no-op.
	blockGasUsed := gasUsed.BlockGasUsed()
	if !vmConfig.StatelessExec && blockGasUsed != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", blockGasUsed, header.GasUsed)
	}

	if header.BlobGasUsed != nil && gasUsed.Blob != *header.BlobGasUsed {
		return nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", gasUsed.Blob, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	var newBlock *types.Block
	var err error
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		newBlock, _, err = FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, true, logger, vmConfig.Tracer)
		if err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	newRoot := newBlock.Root()
	execRs := &EphemeralExecResult{
		StateRoot:   newRoot,
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    rlpHash(blockLogs),
		Receipts:    receipts,
		Difficulty:  &header.Difficulty,
		GasUsed:     math.HexOrDecimal64(blockGasUsed),
		Rejected:    rejectedTxs,
	}

	if chainConfig.Bor != nil {
		var logs []*types.Log
		for _, receipt := range receipts {
			logs = append(logs, receipt.Logs...)
		}

		stateSyncReceipt := &types.Receipt{}
		if chainConfig.Rules == chain.BorRules && len(blockLogs) > 0 {
			slices.SortStableFunc(blockLogs, func(i, j *types.Log) int { return cmp.Compare(i.Index, j.Index) })

			if len(blockLogs) > len(logs) {
				stateSyncReceipt.Logs = blockLogs[len(logs):] // get state-sync logs from `state.Logs()`

				// fill the state sync with the correct information
				bortypes.DeriveFieldsForBorReceipt(stateSyncReceipt, block.Hash(), block.NumberU64(), receipts)
				stateSyncReceipt.Status = types.ReceiptStatusSuccessful
			}
		}

		execRs.StateSyncReceipt = stateSyncReceipt
	}

	return execRs, nil
}

var rlpHash = types.RlpHash

func SysCallContract(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, header *types.Header, engine rules.EngineReader, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	return SysCallContractWithStateWriter(contract, data, chainConfig, ibs, nil, header, engine, constCall, vmCfg)
}

func SysCallContractWithStateWriter(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, engine rules.EngineReader, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	isBor := chainConfig.Bor != nil
	var author accounts.Address
	if isBor {
		author = accounts.InternAddress(header.Coinbase)
	} else {
		author = params.SystemAddress
	}
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, author, chainConfig)
	return SysCallContractWithBlockContextAndStateWriter(contract, data, chainConfig, ibs, stateWriter, blockContext, constCall, vmCfg)
}

func SysCallContractWithBlockContext(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, blockContext evmtypes.BlockContext, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	return SysCallContractWithBlockContextAndStateWriter(contract, data, chainConfig, ibs, nil, blockContext, constCall, vmCfg)
}

func SysCallContractWithBlockContextAndStateWriter(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, stateWriter state.StateWriter, blockContext evmtypes.BlockContext, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	isBor := chainConfig.Bor != nil
	msg := types.NewMessage(
		params.SystemAddress,
		contract,
		0, &u256.Num0,
		SysCallGasLimit,
		&u256.Num0,
		nil, nil,
		data, nil,
		false, // checkNonce
		false, // checkTransaction
		false, // checkGas
		true,  // isFree
		nil,   // maxFeePerBlobGas
	)
	vmConfig := vmCfg
	vmConfig.NoReceipts = true
	vmConfig.RestoreState = constCall
	vmConfig.Tracer = nil // set to nil to avoid trace sysCallContract
	// Create a new context to be used in the EVM environment
	var txContext evmtypes.TxContext
	if isBor {
		txContext = evmtypes.TxContext{}
	} else {
		txContext = NewEVMTxContext(msg)
	}
	evm := vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)
	evm.SetGevmStateWriter(stateWriter)
	mdGas := mdgas.MdGas{
		Regular: msg.Gas(),
		State:   0, // state gas reservoir will consume from regular gas for sys calls
	}
	ret, _, err := evm.Call(
		msg.From(),
		msg.To(),
		msg.Data(),
		mdGas,
		*msg.Value(),
		false,
	)
	if isBor && err != nil {
		return nil, nil
	}

	return ret, err
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
func SysCreate(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, header *types.Header) (result []byte, err error) {
	msg := types.NewMessage(
		contract,
		accounts.NilAddress, // to
		0, &u256.Num0,
		SysCallGasLimit,
		&u256.Num0,
		nil, nil,
		data, nil,
		false, // checkNonce
		false, // checkGas
		false, // checkTransaction
		true,  // isFree
		nil,   // maxFeePerBlobGas
	)
	vmConfig := vm.Config{NoReceipts: true}
	// Create a new context to be used in the EVM environment
	author := contract
	txContext := NewEVMTxContext(msg)
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), nil, author, chainConfig)
	evm := vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)
	mdGas := mdgas.MdGas{
		Regular: msg.Gas(),
		State:   0, // state gas reservoir will consume from regular gas for sys calls
	}
	ret, _, err := evm.SysCreate(
		msg.From(),
		msg.Data(),
		mdGas,
		*msg.Value(),
		contract,
	)
	return ret, err
}

func FinalizeBlockExecution(
	engine rules.Engine, stateReader state.StateReader,
	header *types.Header, txs types.Transactions, uncles []*types.Header,
	stateWriter state.StateWriter, cc *chain.Config,
	ibs *state.IntraBlockState, receipts types.Receipts,
	withdrawals []*types.Withdrawal, chainReader rules.ChainReader,
	isMining bool,
	logger log.Logger,
	tracer *tracing.Hooks,
) (newBlock *types.Block, retRequests types.FlatRequests, err error) {
	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		ret, err := SysCallContractWithStateWriter(contract, data, cc, ibs, stateWriter, header, engine, false /* constCall */, vm.Config{})
		return ret, err
	}

	if isMining {
		newBlock, retRequests, err = engine.FinalizeAndAssemble(cc, header, ibs, txs, uncles, receipts, withdrawals, chainReader, syscall, nil, logger)
	} else {
		retRequests, err = engine.Finalize(cc, header, ibs, uncles, receipts, withdrawals, chainReader, syscall, false, logger)
	}
	if err != nil {
		return nil, nil, err
	}

	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, accounts.NilAddress, cc)
	if err := ibs.CommitBlock(blockContext.Rules(cc), stateWriter); err != nil {
		return nil, nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	}

	return newBlock, retRequests, nil
}

func InitializeBlockExecution(engine rules.Engine, chain rules.ChainHeaderReader, header *types.Header,
	cc *chain.Config, ibs *state.IntraBlockState, stateWriter state.StateWriter, logger log.Logger, tracer *tracing.Hooks,
) error {
	if stateWriter == nil {
		stateWriter = state.NewNoopWriter()
	}
	err := engine.Initialize(cc, chain, header, ibs, func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		ret, err := SysCallContractWithStateWriter(contract, data, cc, ibState, stateWriter, header, engine, constCall, vm.Config{})
		return ret, err
	}, logger, tracer)
	if err != nil {
		return err
	}
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, accounts.NilAddress, cc)
	return ibs.FinalizeTx(blockContext.Rules(cc), stateWriter)
}

func InitializeBlockExecutionGevm(cc *chain.Config, engine rules.Engine, header *types.Header, stateReader state.StateReader, stateWriter state.StateWriter) error {
	if cc == nil || engine == nil || header == nil {
		return nil
	}
	if stateReader == nil || stateWriter == nil {
		return fmt.Errorf("gevm block initialization requires state reader and writer")
	}

	if misc.IsPoSHeader(header) {
		if cc.IsCancun(header.Time) && header.ParentBeaconBlockRoot != nil {
			if _, err := sysCallContractGevm(params.BeaconRootsAddress, header.ParentBeaconBlockRoot.Bytes(), cc, stateReader, stateWriter, header, engine, false); err != nil {
				log.Warn("Failed to call beacon roots contract", "err", err)
			}
		}
		if cc.IsPrague(header.Time) {
			if err := storeBlockHashEip2935Gevm(header, stateReader, stateWriter); err != nil {
				return err
			}
		}
		return nil
	}

	if engine.Type() != chain.EtHashRules {
		return nil
	}
	if cc.DAOForkBlock == nil || header.Number.Uint64() != *cc.DAOForkBlock {
		return nil
	}

	refundOriginal, err := readAccountOrZero(stateReader, misc.DAORefundContract)
	if err != nil {
		return err
	}
	refundAccount := refundOriginal

	for _, addr := range misc.DAODrainList() {
		original, err := readAccountOrZero(stateReader, addr)
		if err != nil {
			return err
		}
		if original.Balance.IsZero() {
			continue
		}
		updated := original
		refundAccount.Balance.Add(&refundAccount.Balance, &original.Balance)
		updated.Balance = u256.N0
		if err := stateWriter.UpdateAccountData(addr, &original, &updated); err != nil {
			return err
		}
	}
	return stateWriter.UpdateAccountData(misc.DAORefundContract, &refundOriginal, &refundAccount)
}

func sysCallContractGevm(contract accounts.Address, data []byte, chainConfig *chain.Config, stateReader state.StateReader, stateWriter state.StateWriter, header *types.Header, engine rules.EngineReader, constCall bool) ([]byte, error) {
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, params.SystemAddress, chainConfig)
	return SysCallContractWithBlockContextAndStateWriter(contract, data, chainConfig, nil, stateWriter, blockContext, constCall, vm.Config{
		UseGevm:        true,
		DisableGevmEnv: true,
		StateReader:    stateReader,
		StateWriter:    stateWriter,
	})
}

func storeBlockHashEip2935Gevm(header *types.Header, stateReader state.StateReader, stateWriter state.StateWriter) error {
	codeSize, err := stateReader.ReadAccountCodeSize(params.HistoryStorageAddress)
	if err != nil {
		return err
	}
	if codeSize == 0 || header.Number.Uint64() == 0 {
		return nil
	}
	slotNum := (header.Number.Uint64() - 1) % params.BlockHashHistoryServeWindow
	storageSlot := accounts.InternKey(common.BytesToHash(uint256.NewInt(slotNum).Bytes()))
	value := *uint256.NewInt(0).SetBytes32(header.ParentHash.Bytes())
	original, _, err := stateReader.ReadAccountStorage(params.HistoryStorageAddress, storageSlot)
	if err != nil {
		return err
	}
	account, err := readAccountOrZero(stateReader, params.HistoryStorageAddress)
	if err != nil {
		return err
	}
	return stateWriter.WriteAccountStorage(params.HistoryStorageAddress, account.GetIncarnation(), storageSlot, original, value)
}

func FinalizeBlockExecutionGevm(cc *chain.Config, engine rules.Engine, header *types.Header, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, stateReader state.StateReader, stateWriter state.StateWriter) error {
	if cc == nil || engine == nil || header == nil || stateReader == nil || stateWriter == nil {
		return nil
	}
	if misc.IsPoSHeader(header) {
		for _, w := range withdrawals {
			if w.Amount == 0 {
				continue
			}
			amountInWei := new(uint256.Int).Mul(uint256.NewInt(w.Amount), uint256.NewInt(common.GWei))
			original, err := readAccountOrZero(stateReader, accounts.InternAddress(w.Address))
			if err != nil {
				return err
			}
			updated := original
			updated.Balance.Add(&updated.Balance, amountInWei)
			if err := stateWriter.UpdateAccountData(accounts.InternAddress(w.Address), &original, &updated); err != nil {
				return err
			}
		}

		if cc.IsPrague(header.Time) {
			rs := make(types.FlatRequests, 0, 3)
			var allLogs types.Logs
			for i, rec := range receipts {
				if rec == nil {
					return fmt.Errorf("nil receipt: block %d, txId %d, receipts %s", header.Number, i, receipts)
				}
				allLogs = append(allLogs, rec.Logs...)
			}
			depositReqs, err := misc.ParseDepositLogs(allLogs, cc.DepositContract)
			if err != nil {
				return fmt.Errorf("error: could not parse requests logs: %v", err)
			}
			if depositReqs != nil {
				rs = append(rs, *depositReqs)
			}
			withdrawalReq, err := dequeueRequestsGevm(cc.GetWithdrawalRequestContract(), types.WithdrawalRequestType, "[EIP-7002] Syscall failure: Empty Code at WithdrawalRequestAddress=%x", cc, stateReader, stateWriter, header, engine)
			if err != nil {
				return err
			}
			if withdrawalReq != nil {
				rs = append(rs, *withdrawalReq)
			}
			consolidations, err := dequeueRequestsGevm(cc.GetConsolidationRequestContract(), types.ConsolidationRequestType, "[EIP-7251] Syscall failure: Empty Code at ConsolidationRequestAddress=%x", cc, stateReader, stateWriter, header, engine)
			if err != nil {
				return err
			}
			if consolidations != nil {
				rs = append(rs, *consolidations)
			}
			if header.RequestsHash != nil {
				rh := rs.Hash()
				if *header.RequestsHash != *rh {
					return fmt.Errorf("error: invalid requests root hash in header, expected: %v, got:%v", header.RequestsHash, rh)
				}
			}
		}
		return nil
	}

	if engine.Type() != chain.EtHashRules {
		return nil
	}

	rewards, err := engine.CalculateRewards(cc, header, uncles, nil)
	if err != nil {
		return err
	}
	for _, reward := range rewards {
		if reward.Amount.IsZero() {
			continue
		}
		original, err := readAccountOrZero(stateReader, reward.Beneficiary)
		if err != nil {
			return err
		}
		updated := original
		updated.Balance.Add(&updated.Balance, &reward.Amount)
		if err := stateWriter.UpdateAccountData(reward.Beneficiary, &original, &updated); err != nil {
			return err
		}
	}
	return nil
}

func dequeueRequestsGevm(contract accounts.Address, requestType byte, emptyCodeErr string, chainConfig *chain.Config, stateReader state.StateReader, stateWriter state.StateWriter, header *types.Header, engine rules.EngineReader) (*types.FlatRequest, error) {
	codeSize, err := stateReader.ReadAccountCodeSize(contract)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf(emptyCodeErr, contract)
	}
	res, err := sysCallContractGevm(contract, nil, chainConfig, stateReader, stateWriter, header, engine, false)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &types.FlatRequest{Type: requestType, RequestData: res}, nil
	}
	return nil, nil
}

func readAccountOrZero(stateReader state.StateReader, addr accounts.Address) (accounts.Account, error) {
	account, err := stateReader.ReadAccountData(addr)
	if err != nil {
		return accounts.Account{}, err
	}
	if account == nil {
		return accounts.NewAccount(), nil
	}
	return *account, nil
}

var alwaysSkipReceiptCheck = dbg.EnvBool("EXEC_SKIP_RECEIPT_CHECK", false)

func BlockPostValidation(blockGasUsed, blobGasUsed uint64, checkReceipts bool, receipts types.Receipts, h *types.Header, txns types.Transactions, chainConfig *chain.Config, logger log.Logger) error {
	if blockGasUsed != h.GasUsed {
		logger.Warn("gas used mismatch", "block", h.Number.Uint64(), "header", h.GasUsed, "execution", blockGasUsed,
			"diff", int64(blockGasUsed)-int64(h.GasUsed), "txCount", len(txns), "receiptCount", len(receipts))
		// Dump per-tx gas for debugging
		var cumGas uint64
		for i, r := range receipts {
			txGas := r.GasUsed
			cumGas += txGas
			var txHash string
			if i < len(txns) {
				txHash = txns[i].Hash().Hex()[:18]
			}
			logger.Warn("  tx gas detail", "block", h.Number.Uint64(), "txIdx", i, "txHash", txHash,
				"gasUsed", txGas, "cumGasUsed", r.CumulativeGasUsed, "computedCumGas", cumGas, "status", r.Status)
		}
		return fmt.Errorf("gas used by execution: %d, in header: %d, headerNum=%d, %x",
			blockGasUsed, h.GasUsed, h.Number.Uint64(), h.Hash())
	}

	if h.BlobGasUsed != nil && blobGasUsed != *h.BlobGasUsed {
		return fmt.Errorf("blobGasUsed by execution: %d, in header: %d, headerNum=%d, %x",
			blobGasUsed, *h.BlobGasUsed, h.Number.Uint64(), h.Hash())
	}

	if checkReceipts && !alwaysSkipReceiptCheck {
		for _, r := range receipts {
			r.Bloom = types.CreateBloom(types.Receipts{r})
		}
		receiptHash := types.DeriveSha(receipts)
		if receiptHash != h.ReceiptHash {
			if dbg.LogHashMismatchReason() {
				ethutils.LogReceipts(log.LvlWarn, "receipt hash mismatch in BlockPostValidation", receipts, txns, chainConfig, h, logger)
			}
			return fmt.Errorf("receiptHash mismatch: %x != %x, headerNum=%d, %x",
				receiptHash, h.ReceiptHash, h.Number.Uint64(), h.Hash())
		}

		lbloom := types.CreateBloom(receipts)
		if lbloom != h.Bloom {
			return fmt.Errorf("invalid bloom (remote: %x  local: %x)", h.Bloom, lbloom)
		}
	}

	if dbg.TraceLogs && dbg.TraceBlock(h.Number.Uint64()) {
		ethutils.LogReceipts(log.LvlInfo, "trace logs", receipts, txns, chainConfig, h, logger)
	}

	return nil
}
