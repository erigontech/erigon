// Copyright 2020 The go-ethereum Authors
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

package t8ntool

import (
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"golang.org/x/crypto/sha3"
)

type Prestate struct {
	Env stEnv             `json:"env"`
	Pre core.GenesisAlloc `json:"pre"`
}

// ExecutionResult contains the execution status after running a state test, any
// error that might have occurred and a dump of the final state if requested.
type ExecutionResult struct {
	StateRoot   common.Hash    `json:"stateRoot"`
	TxRoot      common.Hash    `json:"txRoot"`
	ReceiptRoot common.Hash    `json:"receiptRoot"`
	LogsHash    common.Hash    `json:"logsHash"`
	Bloom       types.Bloom    `json:"logsBloom"        gencodec:"required"`
	Receipts    types.Receipts `json:"receipts"`
	Rejected    []int          `json:"rejected,omitempty"`
}

type ommer struct {
	Delta   uint64         `json:"delta"`
	Address common.Address `json:"address"`
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go
type stEnv struct {
	Coinbase    common.Address                      `json:"currentCoinbase"   gencodec:"required"`
	Difficulty  *big.Int                            `json:"currentDifficulty" gencodec:"required"`
	GasLimit    uint64                              `json:"currentGasLimit"   gencodec:"required"`
	Number      uint64                              `json:"currentNumber"     gencodec:"required"`
	Timestamp   uint64                              `json:"currentTimestamp"  gencodec:"required"`
	BlockHashes map[math.HexOrDecimal64]common.Hash `json:"blockHashes,omitempty"`
	Ommers      []ommer                             `json:"ommers,omitempty"`
}

type stEnvMarshaling struct {
	Coinbase   common.UnprefixedAddress
	Difficulty *math.HexOrDecimal256
	GasLimit   math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
}

// Apply applies a set of transactions to a pre-state
func (pre *Prestate) Apply(vmConfig vm.Config, chainConfig *params.ChainConfig,
	txs types.Transactions, miningReward int64,
	getTracerFn func(txIndex int, txHash common.Hash) (tracer vm.Tracer, err error)) (ethdb.RwKV, *ExecutionResult, error) {

	// Capture errors for BLOCKHASH operation, if we haven't been supplied the
	// required blockhashes
	var hashError error
	getHash := func(num uint64) common.Hash {
		if pre.Env.BlockHashes == nil {
			hashError = fmt.Errorf("getHash(%d) invoked, no blockhashes provided", num)
			return common.Hash{}
		}
		h, ok := pre.Env.BlockHashes[math.HexOrDecimal64(num)]
		if !ok {
			hashError = fmt.Errorf("getHash(%d) invoked, blockhash for that block not provided", num)
		}
		return h
	}
	var (
		db          = ethdb.NewMemDatabase()
		ibs, tds    = MakePreState(db, pre.Pre)
		signer      = types.MakeSigner(chainConfig, pre.Env.Number)
		gaspool     = new(core.GasPool)
		blockHash   = common.Hash{0x13, 0x37}
		rejectedTxs []int
		includedTxs types.Transactions
		gasUsed     = uint64(0)
		receipts    = make(types.Receipts, 0)
		txIndex     = 0
	)
	gaspool.AddGas(pre.Env.GasLimit)
	vmContext := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		Coinbase:    pre.Env.Coinbase,
		BlockNumber: pre.Env.Number,
		Time:        pre.Env.Timestamp,
		Difficulty:  pre.Env.Difficulty,
		GasLimit:    pre.Env.GasLimit,
		GetHash:     getHash,
	}
	// If DAO is supported/enabled, we need to handle it here. In geth 'proper', it's
	// done in StateProcessor.Process(block, ...), right before transactions are applied.
	if chainConfig.DAOForkSupport &&
		chainConfig.DAOForkBlock != nil &&
		chainConfig.DAOForkBlock.Cmp(new(big.Int).SetUint64(pre.Env.Number)) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	tds.StartNewBuffer()

	for i, tx := range txs {
		msg, err := tx.AsMessage(nil, *signer)
		if err != nil {
			log.Info("rejected tx", "index", i, "hash", tx.Hash(), "error", err)
			rejectedTxs = append(rejectedTxs, i)
			continue
		}
		tracer, err := getTracerFn(txIndex, tx.Hash())
		if err != nil {
			return nil, nil, err
		}
		vmConfig.Tracer = tracer
		vmConfig.Debug = (tracer != nil)
		ibs.Prepare(tx.Hash(), blockHash, txIndex)
		txContext := core.NewEVMTxContext(msg)
		snapshot := ibs.Snapshot()
		evm := vm.NewEVM(vmContext, txContext, ibs, chainConfig, vmConfig)

		// (ret []byte, usedGas uint64, failed bool, err error)
		msgResult, err := core.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */)
		if err != nil {
			ibs.RevertToSnapshot(snapshot)
			log.Info("rejected tx", "index", i, "hash", tx.Hash(), "from", msg.From(), "error", err)
			rejectedTxs = append(rejectedTxs, i)
			continue
		}
		includedTxs = append(includedTxs, tx)
		if hashError != nil {
			return nil, nil, NewError(ErrorMissingBlockhash, hashError)
		}
		gasUsed += msgResult.UsedGas

		// Receipt:
		{
			if chainConfig.IsByzantium(vmContext.BlockNumber) {
				tds.StartNewBuffer()
			}

			// Create a new receipt for the transaction, storing the intermediate root and
			// gas used by the tx.
			receipt := &types.Receipt{Type: tx.Type(), CumulativeGasUsed: gasUsed}
			if msgResult.Failed() {
				receipt.Status = types.ReceiptStatusFailed
			} else {
				receipt.Status = types.ReceiptStatusSuccessful
			}
			receipt.TxHash = tx.Hash()
			receipt.GasUsed = msgResult.UsedGas

			// If the transaction created a contract, store the creation address in the receipt.
			if msg.To() == nil {
				receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.GetNonce())
			}

			// Set the receipt logs and create a bloom for filtering
			receipt.Logs = ibs.GetLogs(tx.Hash())
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
			// These three are non-consensus fields:
			//receipt.BlockHash
			//receipt.BlockNumber
			receipt.TransactionIndex = uint(txIndex)
			receipts = append(receipts, receipt)
		}

		txIndex++
	}
	// Add mining reward?
	if miningReward > 0 {
		// Add mining reward. The mining reward may be `0`, which only makes a difference in the cases
		// where
		// - the coinbase suicided, or
		// - there are only 'bad' transactions, which aren't executed. In those cases,
		//   the coinbase gets no txfee, so isn't created, and thus needs to be touched
		var (
			blockReward = uint256.NewInt().SetUint64(uint64(miningReward))
			minerReward = uint256.NewInt().Set(blockReward)
			perOmmer    = uint256.NewInt().Div(blockReward, uint256.NewInt().SetUint64(32))
		)
		for _, ommer := range pre.Env.Ommers {
			// Add 1/32th for each ommer included
			minerReward.Add(minerReward, perOmmer)
			// Add (8-delta)/8
			reward := uint256.NewInt().SetUint64(8)
			reward.Sub(reward, uint256.NewInt().SetUint64(ommer.Delta))
			reward.Mul(reward, blockReward)
			reward.Div(reward, uint256.NewInt().SetUint64(8))
			ibs.AddBalance(ommer.Address, reward)
		}
		ibs.AddBalance(pre.Env.Coinbase, minerReward)
	}

	err := ibs.FinalizeTx(context.Background(), tds.TrieStateWriter())
	if err != nil {
		return nil, nil, err
	}
	// Commit block
	roots, err := tds.ComputeTrieRoots()
	if err != nil {
		return nil, nil, err
	}
	root := roots[len(roots)-1]
	execRs := &ExecutionResult{
		StateRoot:   root,
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: types.DeriveSha(receipts),
		Bloom:       types.CreateBloom(receipts),
		LogsHash:    rlpHash(ibs.Logs()),
		Receipts:    receipts,
		Rejected:    rejectedTxs,
	}
	return db.RwKV(), execRs, nil
}

func MakePreState(db ethdb.Database, accounts core.GenesisAlloc) (*state.IntraBlockState, *state.TrieDbState) {
	tds := state.NewTrieDbState(common.Hash{}, db, 0)
	statedb := state.New(tds)
	for addr, a := range accounts {
		statedb.SetCode(addr, a.Code)
		statedb.SetNonce(addr, a.Nonce)
		b, o := uint256.FromBig(a.Balance)
		if o {
			panic(fmt.Errorf("unexpected overflow in MakePreState"))
		}
		statedb.SetBalance(addr, b)
		for k, v := range a.Storage {
			statedb.SetState(addr, &k, *uint256.NewInt().SetBytes(v.Bytes())) //nolint:scopelint
		}
	}
	err := statedb.FinalizeTx(context.Background(), tds.TrieStateWriter())
	if err != nil {
		panic(err)
	}
	_, err = tds.ComputeTrieRoots()
	if err != nil {
		panic(err)
	}

	return statedb, tds
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x) //nolint:errcheck
	hw.Sum(h[:0])
	return h
}
