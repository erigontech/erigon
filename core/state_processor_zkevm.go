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
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon-lib/chain"

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
func applyTransaction_zkevm(config *chain.Config, engine consensus.EngineReader, gp *GasPool, ibs *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx types.Transaction, usedGas *uint64, evm *vm.EVM, cfg vm.Config, effectiveGasPricePercentage uint8) (*types.Receipt, *ExecutionResult, error) {
	rules := evm.ChainRules()

	msg, err := tx.AsMessage(*types.MakeSigner(config, header.Number.Uint64(), 0), header.BaseFee, rules)
	if err != nil {
		return nil, nil, err
	}
	msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)
	msg.SetCheckNonce(!cfg.StatelessExec)

	// apply effective gas percentage here, so it is actual for all further calculations
	if evm.ChainRules().IsForkID5Dragonfruit {
		msg.SetGasPrice(CalculateEffectiveGas(msg.GasPrice(), effectiveGasPricePercentage))
		msg.SetFeeCap(CalculateEffectiveGas(msg.FeeCap(), effectiveGasPricePercentage))
	}

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
		status := types.ReceiptStatusSuccessful
		if result.Failed() {
			status = types.ReceiptStatusFailed
		}
		// if the transaction created a contract, store the creation address in the receipt.
		var contractAddress libcommon.Address

		if msg.To() == nil {
			contractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.GetNonce())
		}

		// [hack][zkevm] - ignore the bloom at this point due to a bug in zknode where the bloom is not included
		// in the block during execution
		//receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

		// by the tx.
		receipt = &types.Receipt{
			Type:              tx.Type(),
			CumulativeGasUsed: *usedGas,
			TxHash:            tx.Hash(),
			GasUsed:           result.UsedGas,
			Status:            status,
			ContractAddress:   contractAddress,
			Logs:              ibs.GetLogs(tx.Hash()),
			BlockNumber:       header.Number,
			TransactionIndex:  uint(ibs.TxIndex()),
		}
	}

	return receipt, result, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction_zkevm(
	config *chain.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.EngineReader,
	author *libcommon.Address,
	gp *GasPool,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	header *types.Header,
	tx types.Transaction,
	usedGas *uint64,
	cfg vm.ZkConfig,
	effectiveGasPricePercentage uint8,
) (*types.Receipt, *ExecutionResult, error) {
	// Create a new context to be used in the EVM environment

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.Config.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author)
	vmenv := vm.NewZkEVM(blockContext, evmtypes.TxContext{}, ibs, config, cfg)

	return applyTransaction_zkevm(config, engine, gp, ibs, stateWriter, header, tx, usedGas, vmenv, cfg.Config, effectiveGasPricePercentage)
}
