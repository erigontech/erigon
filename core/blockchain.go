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
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/rlp"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
)

var (
	blockExecutionTimer = metrics2.GetOrCreateSummary("chain_execution_seconds")
)

type SyncMode string

const (
	TriesInMemory = 128
)

type RejectedTx struct {
	Index int    `json:"index"    gencodec:"required"`
	Err   string `json:"error"    gencodec:"required"`
}

type RejectedTxs []*RejectedTx

type EphemeralExecResult struct {
	StateRoot         common.Hash              `json:"stateRoot"`
	TxRoot            common.Hash              `json:"txRoot"`
	ReceiptRoot       common.Hash              `json:"receiptsRoot"`
	LogsHash          common.Hash              `json:"logsHash"`
	Bloom             types.Bloom              `json:"logsBloom"        gencodec:"required"`
	Receipts          types.Receipts           `json:"receipts"`
	Rejected          RejectedTxs              `json:"rejected,omitempty"`
	Difficulty        *math.HexOrDecimal256    `json:"currentDifficulty" gencodec:"required"`
	GasUsed           math.HexOrDecimal64      `json:"gasUsed"`
	ReceiptForStorage *types.ReceiptForStorage `json:"-"`
}

func ExecuteBlockEphemerallyForBSC(
	chainConfig *params.ChainConfig,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) common.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	epochReader consensus.EpochReader,
	chainReader consensus.ChainHeaderReader,
	statelessExec bool, // for usage of this API via cli tools wherein some of the validations need to be relaxed.
	getTracer func(txIndex int, txHash common.Hash) (vm.Tracer, error),
) (*EphemeralExecResult, error) {
	defer blockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, epochReader, block.Header(), block.Transactions(), block.Uncles(), chainConfig, ibs); err != nil {
			return nil, err
		}
	}

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	systemcontracts.UpgradeBuildInSystemContract(chainConfig, header.Number, ibs)
	noop := state.NewNoopWriter()
	posa, isPoSA := engine.(consensus.PoSA)
	//fmt.Printf("====txs processing start: %d====\n", block.NumberU64())
	for i, tx := range block.Transactions() {
		if isPoSA {
			if isSystemTx, err := posa.IsSystemTransaction(tx, block.Header()); err != nil {
				return nil, err
			} else if isSystemTx {
				continue
			}
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(i, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		receipt, _, err := ApplyTransaction(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}
		if err != nil {
			if !statelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	var newBlock *types.Block
	var receiptSha common.Hash
	if !vmConfig.ReadOnly {
		// We're doing this hack for BSC to avoid changing consensus interfaces a lot. BSC modifies txs and receipts by appending
		// system transactions, and they increase used gas and write cumulative gas to system receipts, that's why we need
		// to deduct system gas before. This line is equal to "blockGas-systemGas", but since we don't know how much gas is
		// used by system transactions we just override. Of course, we write used by block gas back. It also always true
		// that used gas by block is always equal to original's block header gas, and it's checked by receipts root verification
		// otherwise it causes block verification error.
		header.GasUsed = *usedGas
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return SysCallContract(contract, data, *chainConfig, ibs, header, engine)
		}
		outTxs, outReceipts, err := engine.Finalize(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, epochReader, chainReader, syscall)
		if err != nil {
			return nil, err
		}
		*usedGas = header.GasUsed

		// We need repack this block because transactions and receipts might be changed by consensus, and
		// it won't pass receipts hash or bloom verification
		newBlock = types.NewBlock(block.Header(), outTxs, block.Uncles(), outReceipts)
		// Update receipts
		if !vmConfig.NoReceipts {
			receipts = outReceipts
		}
		receiptSha = newBlock.ReceiptHash()
	} else {
		newBlock = block
		receiptSha = types.DeriveSha(receipts)
	}

	if chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts {
		if !statelessExec && receiptSha != block.ReceiptHash() {
			return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
		}
	}
	if !statelessExec && newBlock.GasUsed() != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d, in new Block: %v", *usedGas, header.GasUsed, newBlock.GasUsed())
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = newBlock.Bloom()
		if !statelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	if err := ibs.CommitBlock(chainConfig.Rules(header.Number.Uint64()), stateWriter); err != nil {
		return nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	} else if err := stateWriter.WriteChangeSets(); err != nil {
		return nil, fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
	}

	execRs := &EphemeralExecResult{
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(block.Header().Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerally(
	chainConfig *params.ChainConfig,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) common.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	epochReader consensus.EpochReader,
	chainReader consensus.ChainHeaderReader,
	statelessExec bool, // for usage of this API via cli tools wherein some of the validations need to be relaxed.
	getTracer func(txIndex int, txHash common.Hash) (vm.Tracer, error),
) (*EphemeralExecResult, error) {

	defer blockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

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
			tracer, err := getTracer(i, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		receipt, _, err := ApplyTransaction(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}
		if err != nil {
			if !statelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	receiptSha := types.DeriveSha(receipts)
	if !statelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !statelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !statelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, epochReader, chainReader, false); err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	execRs := &EphemeralExecResult{
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    rlpHash(blockLogs),
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}

// ExecuteBlockEphemerallyBor runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerallyBor(
	chainConfig *params.ChainConfig,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) common.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	epochReader consensus.EpochReader,
	chainReader consensus.ChainHeaderReader,
	statelessExec bool, // for usage of this API via cli tools wherein some of the validations need to be relaxed.
	getTracer func(txIndex int, txHash common.Hash) (vm.Tracer, error),
) (*EphemeralExecResult, error) {

	defer blockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

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
			tracer, err := getTracer(i, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		receipt, _, err := ApplyTransaction(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}
		if err != nil {
			if !statelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	receiptSha := types.DeriveSha(receipts)
	if !statelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !statelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !statelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, epochReader, chainReader, false); err != nil {
			return nil, err
		}
	}

	var logs []*types.Log
	for _, receipt := range receipts {
		logs = append(logs, receipt.Logs...)
	}

	blockLogs := ibs.Logs()
	var stateSyncReceipt *types.ReceiptForStorage
	if chainConfig.Consensus == params.BorConsensus && len(blockLogs) > 0 {
		var stateSyncLogs []*types.Log
		slices.SortStableFunc(blockLogs, func(i, j *types.Log) bool { return i.Index < j.Index })

		if len(blockLogs) > len(logs) {
			stateSyncLogs = blockLogs[len(logs):] // get state-sync logs from `state.Logs()`

			types.DeriveFieldsForBorLogs(stateSyncLogs, block.Hash(), block.NumberU64(), uint(len(receipts)), uint(len(logs)))

			stateSyncReceipt = &types.ReceiptForStorage{
				Status: types.ReceiptStatusSuccessful, // make receipt status successful
				Logs:   stateSyncLogs,
			}
		}
	}

	execRs := &EphemeralExecResult{
		TxRoot:            types.DeriveSha(includedTxs),
		ReceiptRoot:       receiptSha,
		Bloom:             bloom,
		LogsHash:          rlpHash(blockLogs),
		Receipts:          receipts,
		Difficulty:        (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:           math.HexOrDecimal64(*usedGas),
		Rejected:          rejectedTxs,
		ReceiptForStorage: stateSyncReceipt,
	}

	return execRs, nil
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x) //nolint:errcheck
	hw.Sum(h[:0])
	return h
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
	isBor := chainConfig.Bor != nil
	var txContext vm.TxContext
	var author *common.Address
	if isBor {
		author = &header.Coinbase
		txContext = vm.TxContext{}
	} else {
		author = &state.SystemAddress
		txContext = NewEVMTxContext(msg)
	}
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, author)
	evm := vm.NewEVM(blockContext, txContext, ibs, &chainConfig, vmConfig)
	if isBor {
		ret, _, err := evm.Call(
			vm.AccountRef(msg.From()),
			*msg.To(),
			msg.Data(),
			msg.Gas(),
			msg.Value(),
			false,
		)
		if err != nil {
			return nil, nil
		}
		return ret, nil
	}
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
	_, result, err = ApplyTransaction(&chainConfig, GetHashFn(header, nil), engine, &state.SystemAddress, gp, ibs, noop, header, tx, &gasUsed, vmConfig)
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

func FinalizeBlockExecution(engine consensus.Engine, stateReader state.StateReader, header *types.Header,
	txs types.Transactions, uncles []*types.Header, stateWriter state.WriterWithChangeSets, cc *params.ChainConfig, ibs *state.IntraBlockState,
	receipts types.Receipts, e consensus.EpochReader, headerReader consensus.ChainHeaderReader, isMining bool,
) (newBlock *types.Block, newTxs types.Transactions, newReceipt types.Receipts, err error) {
	syscall := func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine)
	}
	if isMining {
		newBlock, newTxs, newReceipt, err = engine.FinalizeAndAssemble(cc, header, ibs, txs, uncles, receipts, e, headerReader, syscall, nil)
	} else {
		_, _, err = engine.Finalize(cc, header, ibs, txs, uncles, receipts, e, headerReader, syscall)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	var originalSystemAcc *accounts.Account
	if cc.ChainID.Uint64() == 77 { // hack for Sokol - don't understand why eip158 is enabled, but OE still save SystemAddress with nonce=0
		n := ibs.GetNonce(state.SystemAddress) //hack - because syscall must use ApplyMessage instead of ApplyTx (and don't create tx at all). But CallContract must create tx.
		if n > 0 {
			var err error
			originalSystemAcc, err = stateReader.ReadAccountData(state.SystemAddress)
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	if err := ibs.CommitBlock(cc.Rules(header.Number.Uint64()), stateWriter); err != nil {
		return nil, nil, nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	}

	if originalSystemAcc != nil { // hack for Sokol - don't understand why eip158 is enabled, but OE still save SystemAddress with nonce=0
		acc := accounts.NewAccount()
		acc.Nonce = 0
		if err := stateWriter.UpdateAccountData(state.SystemAddress, originalSystemAcc, &acc); err != nil {
			return nil, nil, nil, err
		}
	}

	if err := stateWriter.WriteChangeSets(); err != nil {
		return nil, nil, nil, fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
	}
	return newBlock, newTxs, newReceipt, nil
}

func InitializeBlockExecution(engine consensus.Engine, chain consensus.ChainHeaderReader, epochReader consensus.EpochReader, header *types.Header, txs types.Transactions, uncles []*types.Header, cc *params.ChainConfig, ibs *state.IntraBlockState) error {
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	engine.Initialize(cc, chain, epochReader, header, txs, uncles, func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine)
	})
	return nil
}
