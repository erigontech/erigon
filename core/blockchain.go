// Copyright 2014 The go-ethereum Authors
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

// Package core implements the Ethereum consensus protocol.
package core

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/params"
)

var (
	blockExecutionTimer     = metrics.NewRegisteredTimer("chain/execution", nil)
	blockReorgInvalidatedTx = metrics.NewRegisteredMeter("chain/reorg/invalidTx", nil)
)

const (
	TriesInMemory = 128
)

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerally(
	chainConfig *params.ChainConfig,
	vmConfig *vm.Config,
	getHeader func(hash common.Hash, number uint64) *types.Header,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	epochReader consensus.EpochReader,
	checkTEVM func(codeHash common.Hash) (bool, error),
) (types.Receipts, error) {
	defer blockExecutionTimer.UpdateSince(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	var receipts types.Receipts
	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, epochReader, block.Header(), block.Transactions(), block.Uncles(), chainConfig, ibs); err != nil {
			return nil, err
		}
	}

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	noop := state.NewNoopWriter()
	//fmt.Printf("====txs processing start: %d====\n", block.NumberU64())
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			vmConfig.Tracer = vm.NewStructLogger(&vm.LogConfig{})
			writeTrace = true
		}

		receipt, _, err := ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig, checkTEVM)
		if writeTrace {
			w, err1 := os.Create(fmt.Sprintf("txtrace_%x.txt", tx.Hash()))
			if err1 != nil {
				panic(err1)
			}
			encoder := json.NewEncoder(w)
			logs := FormatLogs(vmConfig.Tracer.(*vm.StructLogger).StructLogs())
			if err2 := encoder.Encode(logs); err2 != nil {
				panic(err2)
			}
			if err2 := w.Close(); err2 != nil {
				panic(err2)
			}
			vmConfig.Tracer = nil
		}
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
		}
		if !vmConfig.NoReceipts {
			receipts = append(receipts, receipt)
		}
	}

	if chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts {
		receiptSha := types.DeriveSha(receipts)
		if receiptSha != block.Header().ReceiptHash {
			return nil, fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
		}
	}

	if *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}
	if !vmConfig.NoReceipts {
		bloom := types.CreateBloom(receipts)
		if bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	if !vmConfig.ReadOnly {
		if err := FinalizeBlockExecution(engine, block.Header(), block.Transactions(), block.Uncles(), stateWriter, chainConfig, ibs, receipts, epochReader); err != nil {
			return nil, err
		}
	}

	return receipts, nil
}

// SystemAddress - sender address for internal state updates.
var SystemAddress = common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")

func SysCallContract(contract common.Address, data []byte, chainConfig params.ChainConfig, ibs *state.IntraBlockState, header *types.Header, engine consensus.Engine) (result []byte, err error) {
	gp := new(GasPool)
	gp.AddGas(50_000_000)
	var gasUsed uint64

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	noop := state.NewNoopWriter()
	tx, err := SysCallContractTx(contract, data, ibs)
	if err != nil {
		return nil, fmt.Errorf("SysCallContract: %w ", err)
	}
	// Set infinite balance to the fake caller account.
	//fmt.Printf("call contract: %d,%x,%x\n", header.Number.Uint64(), contract, data)

	vmConfig := vm.Config{NoReceipts: true, Debug: true, Tracer: vm.NewStructLogger(&vm.LogConfig{})}
	_, result, err = ApplyTransaction(&chainConfig, nil, engine, &SystemAddress, gp, ibs, noop, header, tx, &gasUsed, vmConfig, nil)
	if err != nil {
		return result, fmt.Errorf("SysCallContract: %w ", err)
	}
	ibs.SetNonce(SystemAddress, 0) //hack - because syscall must use ApplyMessage instead of ApplyTx (and don't create tx at all). But CallContract must create tx.

	//w, err1 := os.Create(fmt.Sprintf("txtrace_before.json"))
	//if err1 != nil {
	//	panic(err1)
	//}
	//encoder := json.NewEncoder(w)
	//logs := FormatLogs(vmConfig.Tracer.(*vm.StructLogger).StructLogs())
	//if err2 := encoder.Encode(logs); err2 != nil {
	//	panic(err2)
	//}
	return result, nil
}

// from the null sender, with 50M gas.
func SysCallContractTx(contract common.Address, data []byte, ibs *state.IntraBlockState) (tx types.Transaction, err error) {
	//nonce := ibs.GetNonce(SystemAddress)
	tx = types.NewTransaction(0, contract, u256.Num0, 50_000_000, u256.Num0, data)
	return tx.FakeSign(SystemAddress)
}

func CallContract(contract common.Address, data []byte, chainConfig params.ChainConfig, ibs *state.IntraBlockState, header *types.Header, engine consensus.Engine) (result []byte, err error) {
	gp := new(GasPool)
	gp.AddGas(50_000_000)
	var gasUsed uint64

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	noop := state.NewNoopWriter()
	tx, err := CallContractTx(contract, data, ibs)
	if err != nil {
		return nil, fmt.Errorf("SysCallContract: %w ", err)
	}
	// Set infinite balance to the fake caller account.
	//fmt.Printf("call contract: %d,%x,%x\n", header.Number.Uint64(), contract, data)

	vmConfig := vm.Config{NoReceipts: true, Debug: true, Tracer: vm.NewStructLogger(&vm.LogConfig{})}
	_, result, err = ApplyTransaction(&chainConfig, nil, engine, &SystemAddress, gp, ibs, noop, header, tx, &gasUsed, vmConfig, nil)
	if err != nil {
		return result, fmt.Errorf("SysCallContract: %w ", err)
	}
	//w, err1 := os.Create(fmt.Sprintf("txtrace_before.json"))
	//if err1 != nil {
	//	panic(err1)
	//}
	//encoder := json.NewEncoder(w)
	//logs := FormatLogs(vmConfig.Tracer.(*vm.StructLogger).StructLogs())
	//if err2 := encoder.Encode(logs); err2 != nil {
	//	panic(err2)
	//}
	return result, nil
}

// from the null sender, with 50M gas.
func CallContractTx(contract common.Address, data []byte, ibs *state.IntraBlockState) (tx types.Transaction, err error) {
	from := common.Address{}
	nonce := ibs.GetNonce(from)
	tx = types.NewTransaction(nonce, contract, u256.Num0, 50_000_000, u256.Num0, data)
	return tx.FakeSign(from)
}

func FinalizeBlockExecution(engine consensus.Engine, header *types.Header,
	txs types.Transactions, uncles []*types.Header, stateWriter state.WriterWithChangeSets, cc *params.ChainConfig,
	ibs *state.IntraBlockState, receipts types.Receipts, e consensus.EpochReader,
) error {
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====tx processing end====\n")

	engine.Finalize(cc, header, ibs, txs, uncles, receipts, e, func(contract common.Address, data []byte) ([]byte, error) {
		return CallContract(contract, data, *cc, ibs, header, engine)
	})
	//fmt.Printf("====finalize start====\n")
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====finalize end====\n")

	if err := ibs.CommitBlock(cc.Rules(header.Number.Uint64()), stateWriter); err != nil {
		return fmt.Errorf("committing block %d failed: %v", header.Number.Uint64(), err)
	}
	if cc.ChainID.Uint64() == 77 { // hack for Sokol - don't understand why eip158 is enabled, but OE still save SystemAddress with nonce=0
		acc0 := accounts.NewAccount()
		acc0.Nonce = 1
		acc := accounts.NewAccount()
		acc.Nonce = 0
		stateWriter.UpdateAccountData(SystemAddress, &acc0, &acc)
	}

	if err := stateWriter.WriteChangeSets(); err != nil {
		return fmt.Errorf("writing changesets for block %d failed: %v", header.Number.Uint64(), err)
	}
	return nil
}

func InitializeBlockExecution(engine consensus.Engine, epochReader consensus.EpochReader, header *types.Header, txs types.Transactions, uncles []*types.Header, cc *params.ChainConfig, ibs *state.IntraBlockState) error {
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	engine.Initialize(cc, epochReader, header, txs, uncles, func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine)
	})
	//fmt.Printf("====InitializeBlockExecution start %d====\n", header.Number.Uint64())
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====InitializeBlockExecution end====\n")

	return nil
}
