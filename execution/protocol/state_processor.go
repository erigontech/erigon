// Copyright 2019 The go-ethereum Authors
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
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type GasUsed struct {
	Receipt uint64 // Gas used with refunds (what the user pays) - see EIP-7778
	Block   uint64 // Gas used for block limit accounting - see EIP-7778
	Blob    uint64 // Blob gas - see EIP-4844
}

func NewGasUsed(h *types.Header, receiptGas uint64) *GasUsed {
	gu := &GasUsed{Receipt: receiptGas, Block: h.GasUsed}
	if h.BlobGasUsed != nil {
		gu.Blob = *h.BlobGasUsed
	}
	return gu
}

func SetGasUsed(h *types.Header, gu *GasUsed) {
	h.GasUsed = gu.Block
	if h.BlobGasUsed != nil {
		h.BlobGasUsed = &gu.Blob
	}
}

// applyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func applyTransaction(config *chain.Config, engine rules.EngineReader, gp *GasPool, ibs *state.IntraBlockState,
	stateWriter state.StateWriter, header *types.Header, txn types.Transaction, gasUsed *GasUsed,
	evm *vm.EVM, cfg vm.Config) (*types.Receipt, error) {
	var (
		receipt *types.Receipt
		err     error
	)

	rules := evm.ChainRules()
	blockNum := header.Number.Uint64()
	msg, err := txn.AsMessage(*types.MakeSigner(config, blockNum, header.Time), header.BaseFee, rules)
	if err != nil {
		return nil, err
	}
	msg.SetCheckNonce(!cfg.StatelessExec)

	if cfg.Tracer != nil {
		if cfg.Tracer.OnTxStart != nil {
			cfg.Tracer.OnTxStart(evm.GetVMContext(), txn, msg.From())
		}
		if cfg.Tracer.OnTxEnd != nil {
			defer func() {
				cfg.Tracer.OnTxEnd(receipt, err)
			}()
		}
	}

	txContext := NewEVMTxContext(msg)
	if cfg.TraceJumpDest {
		txContext.TxHash = txn.Hash()
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, ibs)
	result, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		return nil, err
	}
	// Update the state with pending changes
	if err = ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, err
	}
	gasUsed.Receipt += result.ReceiptGasUsed
	gasUsed.Block += result.BlockGasUsed
	gasUsed.Blob += txn.GetBlobGas()

	// Set the receipt logs and create the bloom filter.
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	if !cfg.NoReceipts {
		// by the txn
		receipt = MakeReceipt(header.Number, header.Hash(), msg, txn, gasUsed.Receipt, result, ibs, evm)
	}

	return receipt, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine rules.EngineReader,
	author accounts.Address, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter,
	header *types.Header, txn types.Transaction, gasUsed *GasUsed, cfg vm.Config,
) (*types.Receipt, error) {
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)

	return applyTransaction(config, engine, gp, ibs, stateWriter, header, txn, gasUsed, vmenv, cfg)
}

func CreateEVM(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine rules.EngineReader, author accounts.Address, ibs *state.IntraBlockState, header *types.Header, cfg vm.Config) *vm.EVM {
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	return vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)
}

func ApplyTransactionWithEVM(config *chain.Config, engine rules.EngineReader, gp *GasPool,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter, header *types.Header, txn types.Transaction, gasUsed *GasUsed,
	cfg vm.Config, vmenv *vm.EVM,
) (*types.Receipt, error) {
	return applyTransaction(config, engine, gp, ibs, stateWriter, header, txn, gasUsed, vmenv, cfg)
}

// applyArbTransaction is the same as applyTransaction but returns the full EVM execution result.
func applyArbTransaction(config *chain.Config, engine rules.EngineReader, gp *GasPool, ibs state.IntraBlockStateArbitrum,
	stateWriter state.StateWriter, header *types.Header, txn types.Transaction, usedGas, usedBlobGas *uint64,
	evm *vm.EVM, cfg vm.Config) (*types.Receipt, *evmtypes.ExecutionResult, error) {

	var (
		receipt *types.Receipt
		err     error
	)

	rules := evm.ChainRules()
	blockNum := header.Number.Uint64()
	msg, err := txn.AsMessage(*types.MakeSigner(config, blockNum, header.Time), header.BaseFee, rules)
	if err != nil {
		return nil, nil, err
	}
	msg.SetCheckNonce(!cfg.StatelessExec)

	if cfg.Tracer != nil {
		if cfg.Tracer.OnTxStart != nil {
			cfg.Tracer.OnTxStart(evm.GetVMContext(), txn, msg.From())
		}
		if cfg.Tracer.OnTxEnd != nil {
			defer func() {
				cfg.Tracer.OnTxEnd(receipt, err)
			}()
		}
	}

	txContext := NewEVMTxContext(msg)
	if cfg.TraceJumpDest {
		txContext.TxHash = txn.Hash()
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, ibs.(*state.IntraBlockState))
	result, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, nil)
	if err != nil {
		return nil, nil, err
	}
	// Update the state with pending changes
	if err = ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, nil, err
	}
	*usedGas += result.ReceiptGasUsed
	if usedBlobGas != nil {
		*usedBlobGas += txn.GetBlobGas()
	}

	if !cfg.NoReceipts {
		receipt = &types.Receipt{Type: txn.Type(), CumulativeGasUsed: *usedGas}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = txn.Hash()
		receipt.GasUsed = result.ReceiptGasUsed
		if msg.To().IsNil() {
			receipt.ContractAddress = types.CreateAddress(evm.Origin.Value(), txn.GetNonce())
		}
		receipt.Logs = ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), blockNum, header.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(ibs.TxnIndex())

		if result.TopLevelDeployed != nil {
			receipt.ContractAddress = *result.TopLevelDeployed
		}
		evm.ProcessingHook.FillReceiptInfo(receipt)
	}

	return receipt, result, err
}

// ApplyArbTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment.
func ApplyArbTransaction(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine rules.EngineReader,
	author accounts.Address, gp *GasPool, ibs state.IntraBlockStateArbitrum, stateWriter state.StateWriter,
	header *types.Header, txn types.Transaction, usedGas, usedBlobGas *uint64, cfg vm.Config,
) (*types.Receipt, *evmtypes.ExecutionResult, error) {
	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs.(*state.IntraBlockState), config, cfg)
	return applyArbTransaction(config, engine, gp, ibs, stateWriter, header, txn, usedGas, usedBlobGas, vmenv, cfg)
}

// ApplyArbTransactionVmenv applies a transaction using the given EVM environment.
func ApplyArbTransactionVmenv(config *chain.Config, engine rules.EngineReader, gp *GasPool, ibs state.IntraBlockStateArbitrum, stateWriter state.StateWriter,
	header *types.Header, txn types.Transaction, usedGas, usedBlobGas *uint64, cfg vm.Config, vmenv *vm.EVM,
) (*types.Receipt, *evmtypes.ExecutionResult, error) {
	return applyArbTransaction(config, engine, gp, ibs, stateWriter, header, txn, usedGas, usedBlobGas, vmenv, cfg)
}

// ProcessParentBlockHash stores the parent block hash in the history storage contract
// as per EIP-2935/7709.
func ProcessParentBlockHash(prevHash common.Hash, evm *vm.EVM) {
	msg := types.NewMessage(
		params.SystemAddress,
		params.HistoryStorageAddress,
		0,
		&u256.Num0,
		30_000_000,
		&u256.Num0,
		nil, nil,
		prevHash[:],
		types.AccessList{},
		false,
		false,
		false,
		true,
		nil,
	)

	_, _, _ = evm.Call(msg.From(), msg.To(), msg.Data(), msg.Gas(), *msg.Value(), false)
}

func MakeReceipt(
	blockNumber *big.Int,
	blockHash common.Hash,
	msg *types.Message,
	txn types.Transaction,
	cumulativeGasUsed uint64,
	result *evmtypes.ExecutionResult,
	ibs *state.IntraBlockState,
	evm *vm.EVM,
) *types.Receipt {
	receipt := &types.Receipt{Type: txn.Type(), CumulativeGasUsed: cumulativeGasUsed}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = txn.Hash()
	receipt.GasUsed = result.ReceiptGasUsed
	// In the case of blob transaction, we need to possibly unwrap and store the gas used by blobs
	if t, ok := txn.(*types.BlobTxWrapper); ok {
		txn = &t.Tx
	}
	if txn.Type() == types.BlobTxType {
		receipt.BlobGasUsed = txn.GetBlobGas()
	}
	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To().IsNil() {
		receipt.ContractAddress = types.CreateAddress(evm.Origin.Value(), txn.GetNonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(ibs.TxnIndex())
	return receipt
}
