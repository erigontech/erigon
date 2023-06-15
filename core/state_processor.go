// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
)

// applyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func applyTransaction(config *chain.Config, engine consensus.EngineReader, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx types.Transaction, usedGas *uint64, evm vm.VMInterface, cfg vm.Config) (*types.Receipt, []byte, error) {
	rules := evm.ChainRules()
	msg, err := tx.AsMessage(*types.MakeSigner(config, header.Number.Uint64()), header.BaseFee, rules)
	if err != nil {
		return nil, nil, err
	}
	msg.SetCheckNonce(!cfg.StatelessExec)

	if msg.FeeCap().IsZero() && engine != nil {
		// Only zero-gas transactions may be service ones
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return SysCallContract(contract, data, config, ibs, header, engine, true /* constCall */)
		}
		msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
	}

	txContext := NewEVMTxContext(msg)
	if cfg.TraceJumpDest {
		txContext.TxHash = tx.Hash()
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, ibs)

	result, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, nil, err
	}
	// Update the state with pending changes
	if err = ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, nil, err
	}
	*usedGas += result.UsedGas

	// Set the receipt logs and create the bloom filter.
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	var receipt *types.Receipt
	if !cfg.NoReceipts {
		// by the tx.
		receipt = &types.Receipt{Type: tx.Type(), CumulativeGasUsed: *usedGas}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas
		// if the transaction created a contract, store the creation address in the receipt.
		if msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(evm.TxContext().Origin, tx.GetNonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = ibs.GetLogs(tx.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(ibs.TxIndex())
	}

	return receipt, result.ReturnData, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *chain.Config, blockHashFunc func(n uint64) libcommon.Hash, engine consensus.EngineReader, author *libcommon.Address, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, []byte, error) {
	// Create a new context to be used in the EVM environment

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)

	return applyTransaction(config, engine, gp, ibs, stateWriter, header, tx, usedGas, vmenv, cfg)
}
