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

package core

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
)

// applyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func applyTransaction(config *chain.Config, engine consensus.EngineReader, gp *GasPool, ibs *state.IntraBlockState,
	stateWriter state.StateWriter, header *types.Header, txn types.Transaction, gasUsed, usedBlobGas *uint64,
	evm *vm.EVM, cfg vm.Config) (*types.Receipt, []byte, error) {
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
	evm.Reset(txContext, ibs)
	result, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		return nil, nil, err
	}
	// Update the state with pending changes
	if err = ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, nil, err
	}
	*gasUsed += result.GasUsed
	if usedBlobGas != nil {
		*usedBlobGas += txn.GetBlobGas()
	}

	// Set the receipt logs and create the bloom filter.
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	if !cfg.NoReceipts {
		// by the txn
		receipt = &types.Receipt{Type: txn.Type(), CumulativeGasUsed: *gasUsed}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = txn.Hash()
		receipt.GasUsed = result.GasUsed
		// if the transaction created a contract, store the creation address in the receipt.
		if msg.To() == nil {
			receipt.ContractAddress = types.CreateAddress(evm.Origin, txn.GetNonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), blockNum, header.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(ibs.TxnIndex())
	}

	return receipt, result.ReturnData, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine consensus.EngineReader,
	author *common.Address, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter,
	header *types.Header, txn types.Transaction, gasUsed, usedBlobGas *uint64, cfg vm.Config,
) (*types.Receipt, []byte, error) {
	// Create a new context to be used in the EVM environment

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)

	return applyTransaction(config, engine, gp, ibs, stateWriter, header, txn, gasUsed, usedBlobGas, vmenv, cfg)
}

func CreateEVM(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine consensus.EngineReader, author *common.Address, ibs *state.IntraBlockState, header *types.Header, cfg vm.Config) *vm.EVM {
	// Create a new context to be used in the EVM environment

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	return vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)
}

func ApplyTransactionWithEVM(config *chain.Config, engine consensus.EngineReader, gp *GasPool,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter, header *types.Header, txn types.Transaction, usedGas, usedBlobGas *uint64,
	cfg vm.Config, vmenv *vm.EVM,
) (*types.Receipt, []byte, error) {
	return applyTransaction(config, engine, gp, ibs, stateWriter, header, txn, usedGas, usedBlobGas, vmenv, cfg)
}
