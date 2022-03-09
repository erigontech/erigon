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
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

// StructLogRes stores a structured log emitted by the EVM while replaying a
// transaction in debug mode
type StructLogRes struct {
	Pc      uint64             `json:"pc"`
	Op      string             `json:"op"`
	Gas     uint64             `json:"gas"`
	GasCost uint64             `json:"gasCost"`
	Depth   int                `json:"depth"`
	Error   error              `json:"error,omitempty"`
	Stack   *[]string          `json:"stack,omitempty"`
	Memory  *[]string          `json:"memory,omitempty"`
	Storage *map[string]string `json:"storage,omitempty"`
}

// FormatLogs formats EVM returned structured logs for json output
func FormatLogs(logs []vm.StructLog) []StructLogRes {
	formatted := make([]StructLogRes, len(logs))
	for index, trace := range logs {
		formatted[index] = StructLogRes{
			Pc:      trace.Pc,
			Op:      trace.Op.String(),
			Gas:     trace.Gas,
			GasCost: trace.GasCost,
			Depth:   trace.Depth,
			Error:   trace.Err,
		}
		if trace.Stack != nil {
			stack := make([]string, len(trace.Stack))
			for i, stackValue := range trace.Stack {
				stack[i] = fmt.Sprintf("%x", math.PaddedBigBytes(stackValue, 32))
			}
			formatted[index].Stack = &stack
		}
		if trace.Memory != nil {
			memory := make([]string, 0, (len(trace.Memory)+31)/32)
			for i := 0; i+32 <= len(trace.Memory); i += 32 {
				memory = append(memory, fmt.Sprintf("%x", trace.Memory[i:i+32]))
			}
			formatted[index].Memory = &memory
		}
		if trace.Storage != nil {
			storage := make(map[string]string)
			for i, storageValue := range trace.Storage {
				storage[fmt.Sprintf("%x", i)] = fmt.Sprintf("%x", storageValue)
			}
			formatted[index].Storage = &storage
		}
	}
	return formatted
}

// applyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func applyTransaction(config *params.ChainConfig, gp *GasPool, statedb *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx types.Transaction, usedGas *uint64, evm vm.VMInterface, cfg vm.Config) (*types.Receipt, []byte, error) {
	msg, err := tx.AsMessage(*types.MakeSigner(config, header.Number.Uint64()), header.BaseFee)
	if err != nil {
		return nil, nil, err
	}

	txContext := NewEVMTxContext(msg)
	if cfg.TraceJumpDest {
		txContext.TxHash = tx.Hash()
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, statedb)

	result, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, nil, err
	}
	// Update the state with pending changes
	if err = statedb.FinalizeTx(evm.ChainRules(), stateWriter); err != nil {
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
		receipt.Logs = statedb.GetLogs(tx.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(statedb.TxIndex())
	}

	return receipt, result.ReturnData, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, engine consensus.Engine, author *common.Address, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx types.Transaction, usedGas *uint64, cfg vm.Config, contractHasTEVM func(contractHash common.Hash) (bool, error)) (*types.Receipt, []byte, error) {
	// Create a new context to be used in the EVM environment

	var vmenv vm.VMInterface

	if tx.IsStarkNet() {
		vmenv = &vm.CVMAdapter{Cvm: vm.NewCVM(ibs)}
	} else {
		blockContext := NewEVMBlockContext(header, getHeader, engine, author, contractHasTEVM)
		vmenv = vm.NewEVM(blockContext, vm.TxContext{}, ibs, config, cfg)
	}

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	return applyTransaction(config, gp, ibs, stateWriter, header, tx, usedGas, vmenv, cfg)
}
