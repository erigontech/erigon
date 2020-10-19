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
	"bytes"
	"encoding/json"
	"os"

	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config      *params.ChainConfig // Chain configuration options
	bc          *BlockChain         // Canonical block chain
	engine      consensus.Engine    // Consensus engine used for block rewards
	txTraceHash []byte              // Hash of the transaction to trace (or nil if there nothing to trace)
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// SetTxTraceHash allows setting the hash of the transaction to trace
func (p *StateProcessor) SetTxTraceHash(txTraceHash common.Hash) {
	p.txTraceHash = txTraceHash[:]
}

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

// PreProcess processes the state changes according to the Ethereum rules by running
// the transaction messages using the IntraBlockState and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// PreProcess returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
//
// PreProcess does not calculate receipt roots (required pre-Byzantium)
// and does not update the TrieDbState. For those two call PostProcess afterwards.
func (p *StateProcessor) PreProcess(block *types.Block, ibs *state.IntraBlockState, tds *state.TrieDbState, cfg vm.Config) (
	receipts types.Receipts, allLogs []*types.Log, usedGas uint64, root common.Hash, err error) {

	header := block.Header()
	gp := new(GasPool).AddGas(block.GasLimit())

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	// Iterate over and process the individual transactions
	tds.StartNewBuffer()
	for i, tx := range block.Transactions() {
		txHash := tx.Hash()
		ibs.Prepare(txHash, block.Hash(), i)
		writeTrace := false
		if !cfg.Debug && p.txTraceHash != nil && bytes.Equal(p.txTraceHash, txHash[:]) {
			// This code is useful when debugging a certain transaction. If uncommented, together with the code
			// at the end of this function, after the execution of transaction with given hash, the file
			// structlogs.txt will contain full trace of the transactin in JSON format. This can be compared
			// to another trace, obtained from the correct version of the turbo-geth or go-ethereum
			cfg.Tracer = vm.NewStructLogger(&vm.LogConfig{})
			cfg.Debug = true
			writeTrace = true
		}
		var receipt *types.Receipt
		receipt, err = ApplyTransaction(p.config, p.bc, nil, gp, ibs, tds.TrieStateWriter(), header, tx, &usedGas, cfg)
		// This code is useful when debugging a certain transaction. If uncommented, together with the code
		// at the end of this function, after the execution of transaction with given hash, the file
		// structlogs.txt will contain full trace of the transactin in JSON format. This can be compared
		// to another trace, obtained from the correct version of the turbo-geth or go-ethereum
		if writeTrace {
			w, err1 := os.Create(fmt.Sprintf("txtrace_%x.txt", p.txTraceHash))
			if err1 != nil {
				panic(err1)
			}
			encoder := json.NewEncoder(w)
			logs := FormatLogs(cfg.Tracer.(*vm.StructLogger).StructLogs())
			if err2 := encoder.Encode(logs); err2 != nil {
				panic(err2)
			}
			if err2 := w.Close(); err2 != nil {
				panic(err2)
			}
			cfg.Debug = false
			cfg.Tracer = nil
		}
		if err != nil {
			return
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		if !p.config.IsByzantium(header.Number) {
			tds.StartNewBuffer()
		}
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.config, header, ibs, block.Transactions(), block.Uncles())
	ctx := p.config.WithEIPsFlags(context.Background(), header.Number)
	err = ibs.FinalizeTx(ctx, tds.TrieStateWriter())
	if err != nil {
		return
	}

	// Calculate the state root
	_, err = tds.ResolveStateTrie(false, false)
	if err != nil {
		return
	}
	root, err = tds.CalcTrieRoots(false)
	return receipts, allLogs, usedGas, root, err
}

// PostProcess calculates receipt roots (required pre-Byzantium) and updates the TrieDbState.
// PostProcess should be called after PreProcess.
func (p *StateProcessor) PostProcess(block *types.Block, tds *state.TrieDbState, receipts types.Receipts) error {
	roots, err := tds.UpdateStateTrie()
	if err != nil {
		return err
	}
	if !p.config.IsByzantium(block.Header().Number) {
		for i, receipt := range receipts {
			receipt.PostState = roots[i].Bytes()
		}
	}
	return nil
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.IntraBlockState, stateWriter state.StateWriter, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	ctx := config.WithEIPsFlags(context.Background(), header.Number)
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	if cfg.TraceJumpDest {
		context.TxHash = tx.Hash()
	}
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	cfg.SkipAnalysis = SkipAnalysis(config, header.Number.Uint64())
	vmenv := vm.NewEVM(context, statedb, config, cfg)
	// Apply the transaction to the current state (included in the env)
	result, err := ApplyOnlyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, err
	}
	// Update the state with pending changes
	if err = statedb.FinalizeTx(ctx, stateWriter); err != nil {
		return nil, err
	}

	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	var receipt *types.Receipt
	if !cfg.NoReceipts {
		receipt = types.NewReceipt(result.Failed(), *usedGas)
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas
		// if the transaction created a contract, store the creation address in the receipt.
		if msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = statedb.GetLogs(tx.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	}
	return receipt, err
}
