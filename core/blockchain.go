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

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/mclock"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

var (
	blockExecutionTimer = metrics2.GetOrCreateSummary("chain_execution_seconds")
)

const (
	TriesInMemory = 128
)

// statsReportLimit is the time limit during import and export after which we
// always print out progress. This avoids the user wondering what's going on.
const statsReportLimit = 8 * time.Second

// report prints statistics if some number of blocks have been processed
// or more than a few seconds have passed since the last message.
func (st *InsertStats) Report(logPrefix string, chain []*types.Block, index int, toCommit bool) {
	// Fetch the timings for the batch
	var (
		now     = mclock.Now()
		elapsed = time.Duration(now) - time.Duration(st.StartTime)
	)
	// If we're at the last block of the batch or report period reached, log
	if index == len(chain)-1 || elapsed >= statsReportLimit || toCommit {
		// Count the number of transactions in this segment
		var txs int
		for _, block := range chain[st.lastIndex : index+1] {
			txs += len(block.Transactions())
		}
		end := chain[index]
		context := []interface{}{
			"blocks", st.Processed, "txs", txs,
			"elapsed", common.PrettyDuration(elapsed),
			"number", end.Number(), "hash", end.Hash(),
		}
		if timestamp := time.Unix(int64(end.Time()), 0); time.Since(timestamp) > time.Minute {
			context = append(context, []interface{}{"age", common.PrettyAge(timestamp)}...)
		}
		if st.queued > 0 {
			context = append(context, []interface{}{"queued", st.queued}...)
		}
		if st.ignored > 0 {
			context = append(context, []interface{}{"ignored", st.ignored}...)
		}
		log.Info(fmt.Sprintf("[%s] Imported new chain segment", logPrefix), context...)
		*st = InsertStats{StartTime: now, lastIndex: index + 1}
	}
}

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
	chainReader consensus.ChainHeaderReader,
	contractHasTEVM func(codeHash common.Hash) (bool, error),
) (types.Receipts, error) {
	defer blockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	var receipts types.Receipts
	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, epochReader, block.Header(), block.Transactions(), block.Uncles(), chainConfig, ibs); err != nil {
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

		receipt, _, err := ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig, contractHasTEVM)
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
		if receiptSha != block.ReceiptHash() {
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
		if err := FinalizeBlockExecution(engine, stateReader, block.Header(), block.Transactions(), block.Uncles(), stateWriter, chainConfig, ibs, receipts, epochReader, chainReader); err != nil {
			return nil, err
		}
	}

	return receipts, nil
}

func SysCallContract(contract common.Address, data []byte, chainConfig params.ChainConfig, ibs *state.IntraBlockState, header *types.Header, engine consensus.Engine) (result []byte, err error) {
	gp := new(GasPool).AddGas(50_000_000)

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	msg := types.NewMessage(
		state.SystemAddress,
		&contract,
		0, u256.Num0,
		50_000_000, u256.Num0,
		nil, nil,
		data, nil, false,
	)
	vmConfig := vm.Config{NoReceipts: true}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, nil, engine, &state.SystemAddress, nil)
	evm := vm.NewEVM(blockContext, NewEVMTxContext(msg), ibs, &chainConfig, vmConfig)
	res, err := ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, err
	}
	return res.ReturnData, nil
}

// from the null sender, with 50M gas.
func SysCallContractTx(contract common.Address, data []byte) (tx types.Transaction, err error) {
	//nonce := ibs.GetNonce(SystemAddress)
	tx = types.NewTransaction(0, contract, u256.Num0, 50_000_000, u256.Num0, data)
	return tx.FakeSign(state.SystemAddress)
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
	vmConfig := vm.Config{NoReceipts: true}
	_, result, err = ApplyTransaction(&chainConfig, nil, engine, &state.SystemAddress, gp, ibs, noop, header, tx, &gasUsed, vmConfig, nil)
	if err != nil {
		return result, fmt.Errorf("SysCallContract: %w ", err)
	}
	return result, nil
}

// from the null sender, with 50M gas.
func CallContractTx(contract common.Address, data []byte, ibs *state.IntraBlockState) (tx types.Transaction, err error) {
	from := common.Address{}
	nonce := ibs.GetNonce(from)
	tx = types.NewTransaction(nonce, contract, u256.Num0, 50_000_000, u256.Num0, data)
	return tx.FakeSign(from)
}

func FinalizeBlockExecution(engine consensus.Engine, stateReader state.StateReader, header *types.Header, txs types.Transactions, uncles []*types.Header, stateWriter state.WriterWithChangeSets, cc *params.ChainConfig, ibs *state.IntraBlockState, receipts types.Receipts, e consensus.EpochReader, headerReader consensus.ChainHeaderReader) error {
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====tx processing end====\n")

	if err := engine.Finalize(cc, header, ibs, txs, uncles, receipts, e, headerReader, func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine)
	}); err != nil {
		return err
	}

	//fmt.Printf("====finalize start %d====\n", header.Number.Uint64())
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====finalize end====\n")

	var originalSystemAcc *accounts.Account

	if cc.ChainID.Uint64() == 77 { // hack for Sokol - don't understand why eip158 is enabled, but OE still save SystemAddress with nonce=0
		n := ibs.GetNonce(state.SystemAddress) //hack - because syscall must use ApplyMessage instead of ApplyTx (and don't create tx at all). But CallContract must create tx.
		if n > 0 {
			var err error
			originalSystemAcc, err = stateReader.ReadAccountData(state.SystemAddress)
			if err != nil {
				return err
			}
		}
	}

	if err := ibs.CommitBlock(cc.Rules(header.Number.Uint64()), stateWriter); err != nil {
		return fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	}

	if originalSystemAcc != nil { // hack for Sokol - don't understand why eip158 is enabled, but OE still save SystemAddress with nonce=0
		acc := accounts.NewAccount()
		acc.Nonce = 0
		if err := stateWriter.UpdateAccountData(state.SystemAddress, originalSystemAcc, &acc); err != nil {
			return err
		}
	}

	if err := stateWriter.WriteChangeSets(); err != nil {
		return fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
	}
	return nil
}

func InitializeBlockExecution(engine consensus.Engine, chain consensus.ChainHeaderReader, epochReader consensus.EpochReader, header *types.Header, txs types.Transactions, uncles []*types.Header, cc *params.ChainConfig, ibs *state.IntraBlockState) error {
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	engine.Initialize(cc, chain, epochReader, header, txs, uncles, func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine)
	})
	//fmt.Printf("====InitializeBlockExecution start %d====\n", header.Number.Uint64())
	//ibs.Print(cc.Rules(header.Number.Uint64()))
	//fmt.Printf("====InitializeBlockExecution end====\n")

	return nil
}
