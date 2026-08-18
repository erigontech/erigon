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
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
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
	StateRoot   common.Hash           `json:"stateRoot"`
	TxRoot      common.Hash           `json:"txRoot"`
	ReceiptRoot common.Hash           `json:"receiptsRoot"`
	LogsHash    common.Hash           `json:"logsHash"`
	Bloom       types.Bloom           `json:"logsBloom"        gencodec:"required"`
	Receipts    types.Receipts        `json:"receipts"`
	Rejected    RejectedTxs           `json:"rejected,omitempty"`
	Difficulty  *math.HexOrDecimal256 `json:"currentDifficulty" gencodec:"required"`
	GasUsed     math.HexOrDecimal64   `json:"gasUsed"`
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
	defer ibs.Close()
	ibs.SetHooks(vmConfig.Tracer)
	header := block.Header()

	gasUsed := new(GasUsed)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnBlockStart != nil {
		vmConfig.Tracer.OnBlockStart(tracing.BlockEvent{
			Block:     block,
			TD:        chainReader.GetTd(block.ParentHash(), block.NumberU64()-1),
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
		// ApplyTransaction populated each receipt's Bloom, so merge those
		// instead of hashing all logs again.
		bloom = receipts.MergedBloom()
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
	newRoot := newBlock.Root()
	execRs := &EphemeralExecResult{
		StateRoot:   newRoot,
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    ibs.LogsRlpHash(),
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(header.Difficulty.ToBig()),
		GasUsed:     math.HexOrDecimal64(blockGasUsed),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}

func SysCallContract(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, header *types.Header, engine rules.EngineReader, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	return SysCallContractWithEVM(nil, contract, data, chainConfig, ibs, header, engine, constCall, vmCfg)
}

func SysCallContractWithBlockContext(contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, blockContext evmtypes.BlockContext, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	return sysCallContract(nil, contract, data, chainConfig, ibs, blockContext, constCall, vmCfg)
}

// NewSysCallEVM builds an EVM for SysCallContractWithEVM to reuse. Only
// chainConfig survives a call, so there is nothing else to seed.
func NewSysCallEVM(chainConfig *chain.Config, vmCfg vm.Config) *vm.EVM {
	return vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vmCfg)
}

// SysCallContractWithEVM runs a system call on an EVM the caller owns instead of
// building one per call. The EVM must belong to the calling goroutine, and the
// call overwrites its block context, tx context, IntraBlockState and vm.Config,
// so the caller must not need any of those to survive. A nil EVM, or one built
// for a different chainConfig, falls back to allocating one.
func SysCallContractWithEVM(evm *vm.EVM, contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, header *types.Header, engine rules.EngineReader, constCall bool, vmCfg vm.Config) (result []byte, err error) {
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, params.SystemAddress, chainConfig)
	return sysCallContract(evm, contract, data, chainConfig, ibs, blockContext, constCall, vmCfg)
}

func sysCallContract(evm *vm.EVM, contract accounts.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, blockContext evmtypes.BlockContext, constCall bool, vmCfg vm.Config) (result []byte, err error) {
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
	if evm == nil || evm.ChainConfig() != chainConfig {
		evm = vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)
	} else {
		evm.ResetBetweenBlocks(blockContext, txContext, ibs, vmConfig, blockContext.Rules(chainConfig))
	}
	mdGas := mdgas.MdGas{
		Execution: msg.Gas(),
		State:     0, // pre-Amsterdam: state-gas reservoir not used; spills into execution gas
	}
	if evm.ChainRules().IsAmsterdam {
		// EIP-8037: extra state-gas reservoir on top of the 30M execution budget
		// so system calls keep their pre-EIP-8037 execution margin.
		mdGas.State = params.StateGasSystemMaxSstores
	}
	ret, _, _, err := evm.Call(
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
		Execution: msg.Gas(),
		State:     0, // state gas reservoir will consume from execution gas for sys calls
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
		ret, err := SysCallContract(contract, data, cc, ibs, header, engine, false /* constCall */, vm.Config{})
		return ret, err
	}

	if ibs.IsVersioned() {
		ibs.StartAccessRecording()
	}

	if isMining {
		newBlock, retRequests, err = engine.FinalizeAndAssemble(cc, header, ibs, txs, uncles, receipts, withdrawals, chainReader, syscall, nil, logger)
	} else {
		retRequests, err = engine.Finalize(cc, header, ibs, uncles, receipts, withdrawals, chainReader, syscall, false, logger)
	}
	if err != nil {
		return nil, nil, err
	}

	// A versioned ibs (parallel-mode block assembly) commits from the versionMap
	// write-set — the caller applies ba.BalIO() via WriteSet.Normalize/Apply after
	// assembly, so so.data must not also be flushed here. versionMap==nil callers
	// (ExecuteBlockEphemerally, RPC) keep the so.data CommitBlock.
	if !ibs.IsVersioned() {
		blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, accounts.NilAddress, cc)
		if err := ibs.CommitBlock(blockContext.Rules(cc), stateWriter); err != nil {
			return nil, nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
		}
	}

	return newBlock, retRequests, nil
}

func InitializeBlockExecution(engine rules.Engine, chain rules.ChainHeaderReader, header *types.Header,
	cc *chain.Config, ibs *state.IntraBlockState, stateWriter state.StateWriter, logger log.Logger, tracer *tracing.Hooks,
) error {
	err := engine.Initialize(cc, chain, header, ibs, func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		ret, err := SysCallContract(contract, data, cc, ibState, header, engine, constCall, vm.Config{})
		return ret, err
	}, logger, tracer)
	if err != nil {
		return err
	}
	if stateWriter == nil {
		stateWriter = state.NewNoopWriter()
	}
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, accounts.NilAddress, cc)
	return ibs.FinalizeTx(blockContext.Rules(cc), stateWriter)
}
