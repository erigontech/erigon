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
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/consensus"
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

	//if msg.FeeCap().IsZero() && engine != nil {
	//	// Only zero-gas transactions may be service ones
	//	syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
	//		b, logs, err := SysCallContract(contract, data, config, ibs, header, engine, true /* constCall */, evm.Config().Tracer)
	//		_ = logs
	//		return b, err
	//	}
	//	msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
	//}

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
	// TODO add resultFilter from Arbitrum?

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
			receipt.ContractAddress = crypto.CreateAddress(evm.Origin, txn.GetNonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), blockNum, header.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(ibs.TxnIndex())

		// If the transaction created a contract, store the creation address in the receipt.
		if result.TopLevelDeployed != nil {
			receipt.ContractAddress = *result.TopLevelDeployed
		}
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

/// TODO move to separate file/package

// Arbiturm modifications.
// So applyArbTransaction all the same to applyTransaction but returns whole evm result and possibly get execution mode as parameter

// applyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func applyArbTransaction(config *chain.Config, engine consensus.EngineReader, gp *GasPool, ibs state.IntraBlockStateArbitrum,
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
	*usedGas += result.GasUsed
	if usedBlobGas != nil {
		*usedBlobGas += txn.GetBlobGas()
	}
	// TODO add resultFilter from Arbitrum?

	// Set the receipt logs and create the bloom filter.
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	if !cfg.NoReceipts {
		// by the txn
		receipt = &types.Receipt{Type: txn.Type(), CumulativeGasUsed: *usedGas}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = txn.Hash()
		receipt.GasUsed = result.GasUsed
		// if the transaction created a contract, store the creation address in the receipt.
		if msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(evm.Origin, txn.GetNonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = ibs.GetLogs(ibs.TxnIndex(), txn.Hash(), blockNum, header.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(ibs.TxnIndex())

		// If the transaction created a contract, store the creation address in the receipt.
		if result.TopLevelDeployed != nil {
			receipt.ContractAddress = *result.TopLevelDeployed
		}
	}

	return receipt, result, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyArbTransaction(config *chain.Config, blockHashFunc func(n uint64) (common.Hash, error), engine consensus.EngineReader,
	author *common.Address, gp *GasPool, ibs state.IntraBlockStateArbitrum, stateWriter state.StateWriter,
	header *types.Header, txn types.Transaction, usedGas, usedBlobGas *uint64, cfg vm.Config,
) (*types.Receipt, *evmtypes.ExecutionResult, error) {
	// Create a new context to be used in the EVM environment

	// Add addresses to access list if applicable
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, author, config)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs.(*state.IntraBlockState), config, cfg)

	// ibss := ibs.(*state.IntraBlockState)

	return applyArbTransaction(config, engine, gp, ibs, stateWriter, header, txn, usedGas, usedBlobGas, vmenv, cfg)
}

// ProcessBeaconBlockRoot applies the EIP-4788 system call to the beacon block root
// contract. This method is exported to be used in tests.
//
//	func ProcessBeaconBlockRoot(beaconRoot libcommon.Hash, evm *vm.EVM) {
//		//if tracer := evm.Config.Tracer; tracer != nil {
//		//	onSystemCallStart(tracer, evm.GetVMContext())
//		//	if tracer.OnSystemCallEnd != nil {
//		//		defer tracer.OnSystemCallEnd()
//		//	}
//		////}
//		//msg := &Message{
//		//	From:      params.SystemAddress,
//		//	GasLimit:  30_000_000,
//		//	GasPrice:  common.Big0,
//		//	GasFeeCap: common.Big0,
//		//	GasTipCap: common.Big0,
//		//	To:        &params.BeaconRootsAddress,
//		//	Data:      beaconRoot[:],
//		//}
//		gasLimit := hexutil.Uint64(30_000_000)
//		data := hexutil.Bytes(beaconRoot[:])
//		msg := ethapi.CallArgs{
//			From:                 &params.SystemAddress,
//			To:                   &params.BeaconRootsAddress,
//			Gas:                  &gasLimit,
//			GasPrice:             new(hexutil.Big),
//			MaxPriorityFeePerGas: new(hexutil.Big),
//			MaxFeePerGas:         new(hexutil.Big),
//			MaxFeePerBlobGas:     nil,
//			Value:                nil,
//			Nonce:                nil,
//			Data:                 &data,
//			Input:                nil,
//			AccessList:           nil,
//			//ChainID:              evm.ChainConfig().ChainID,
//			AuthorizationList: nil,
//			SkipL1Charging:    nil,
//		}
//		evm.SetTxContext(NewEVMTxContext(msg))
//		//evm.
//		evm.StateDB.AddAddressToAccessList(params.BeaconRootsAddress)
//		_, _, _ = evm.Call(msg.From, *msg.To, msg.Data, 30_000_000, common.U2560)
//		evm.StateDB.Finalise(true)
//	}
//
// ProcessParentBlockHash stores the parent block hash in the history storage contract
// as per EIP-2935/7709.
func ProcessParentBlockHash(prevHash common.Hash, evm *vm.EVM) {
	//if tracer := evm.Config.Tracer; tracer != nil {
	//	onSystemCallStart(tracer, evm.GetVMContext())
	//	if tracer.OnSystemCallEnd != nil {
	//		defer tracer.OnSystemCallEnd()
	//	}
	//}
	//tx
	//msg := &Message{
	//	From:      params.SystemAddress,
	//	GasLimit:  30_000_000,
	//	GasPrice:  common.Big0,
	//	GasFeeCap: common.Big0,
	//	GasTipCap: common.Big0,
	//	To:        &params.HistoryStorageAddress,
	//	Data:      prevHash.Bytes(),
	//}
	msg := types.NewMessage(
		state.SystemAddress,
		&params.HistoryStorageAddress,
		0,
		common.Num0,
		30_000_000,
		common.Num0,
		common.Num0,
		common.Num0,
		prevHash[:],
		types.AccessList{},
		false,
		false,
		common.Num0,
	)

	//msg, err := args.ToMessage(30_000_000, evm.Context.BaseFee)
	//evm
	//evm.SetTxContext(NewEVMTxContext(msg))
	//evm.StateDB.AddAddressToAccessList(params.HistoryStorageAddress)

	_, _, err := evm.Call(vm.AccountRef(msg.From()), *msg.To(), msg.Data(), msg.Gas(), common.Num0, false)
	if err != nil {
		panic(err)
	}
	//if evm.StateDB.AccessEvents() != nil {
	//	evm.StateDB.AccessEvents().Merge(evm.AccessEvents)
	//}
	//evm.StateDB.Finalise(true)
}

//
// ProcessWithdrawalQueue calls the EIP-7002 withdrawal queue contract.
// It returns the opaque request data returned by the contract.
//func ProcessWithdrawalQueue(requests *[][]byte, evm *vm.EVM) {
//	processRequestsSystemCall(requests, evm, 0x01, params.WithdrawalQueueAddress)
//}
//
//// ProcessConsolidationQueue calls the EIP-7251 consolidation queue contract.
//// It returns the opaque request data returned by the contract.
//func ProcessConsolidationQueue(requests *[][]byte, evm *vm.EVM) {
//	processRequestsSystemCall(requests, evm, 0x02, params.ConsolidationQueueAddress)
//}
//
//func processRequestsSystemCall(requests *[][]byte, evm *vm.EVM, requestType byte, addr libcommon.Address) {
//	//if tracer := evm.Config.Tracer; tracer != nil {
//	//	onSystemCallStart(tracer, evm.GetVMContext())
//	//	if tracer.OnSystemCallEnd != nil {
//	//		defer tracer.OnSystemCallEnd()
//	//	}
//	//}
//	msg := &Message{
//		From:      params.SystemAddress,
//		GasLimit:  30_000_000,
//		GasPrice:  common.Big0,
//		GasFeeCap: common.Big0,
//		GasTipCap: common.Big0,
//		To:        &addr,
//	}
//	evm.SetTxContext(NewEVMTxContext(msg))
//	evm.StateDB.AddAddressToAccessList(addr)
//	ret, _, _ := evm.Call(msg.From, *msg.To, msg.Data, 30_000_000, libcommon.U2560)
//	evm.StateDB.Finalise(true)
//	if len(ret) == 0 {
//		return // skip empty output
//	}
//
//	// Append prefixed requestsData to the requests list.
//	requestsData := make([]byte, len(ret)+1)
//	requestsData[0] = requestType
//	copy(requestsData[1:], ret)
//	*requests = append(*requests, requestsData)
//}

var depositTopic = common.HexToHash("0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5")

// ParseDepositLogs extracts the EIP-6110 deposit values from logs emitted by
// BeaconDepositContract.
//func ParseDepositLogs(requests *[][]byte, logs []*types.Log, config *params.ChainConfig) error {
//	deposits := make([]byte, 1) // note: first byte is 0x00 (== deposit request type)
//	for _, log := range logs {
//		if log.Address == config.DepositContractAddress && len(log.Topics) > 0 && log.Topics[0] == depositTopic {
//			request, err := types.DepositLogToRequest(log.Data)
//			if err != nil {
//				return fmt.Errorf("unable to parse deposit data: %v", err)
//			}
//			deposits = append(deposits, request...)
//		}
//	}
//	if len(deposits) > 1 {
//		*requests = append(*requests, deposits)
//	}
//	return nil
//}

//func onSystemCallStart(tracer *tracing.Hooks, ctx *tracing.VMContext) {
//	//if tracer.OnSystemCallStartV2 != nil {
//	//	tracer.OnSystemCallStartV2(ctx)
//	//} else if tracer.OnSystemCallStart != nil {
//	tracer.OnSystemCallStart()
//	//}
//}
