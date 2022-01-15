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
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
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
	Rejected    []*rejectedTx  `json:"rejected,omitempty"`
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
	BaseFee     *big.Int                            `json:"currentBaseFee,omitempty"`
	Random      *common.Hash                        `json:"currentRandom,omitempty"`
}

type rejectedTx struct {
	Index int    `json:"index"`
	Err   string `json:"error"`
}

type stEnvMarshaling struct {
	Coinbase   common.UnprefixedAddress
	Difficulty *math.HexOrDecimal256
	GasLimit   math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	BaseFee    *math.HexOrDecimal256
}

// Apply applies a set of transactions to a pre-state
func (pre *Prestate) Apply(vmConfig vm.Config, chainConfig *params.ChainConfig,
	txs types.Transactions, miningReward int64,
	getTracerFn func(txIndex int, txHash common.Hash) (tracer vm.Tracer, err error)) (kv.RwDB, *ExecutionResult, error) {

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
	db := memdb.New()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	var (
		ibs         = MakePreState(chainConfig.Rules(0), tx, pre.Pre)
		signer      = types.MakeSigner(chainConfig, pre.Env.Number)
		gaspool     = new(core.GasPool)
		blockHash   = common.Hash{0x13, 0x37}
		rejectedTxs []*rejectedTx
		includedTxs types.Transactions
		gasUsed     = uint64(0)
		receipts    = make(types.Receipts, 0)
		txIndex     = 0
	)
	gaspool.AddGas(pre.Env.GasLimit)

	difficulty := new(big.Int)
	if pre.Env.Random == nil {
		difficulty = pre.Env.Difficulty
	} else {
		// We are on POS hence difficulty opcode is now supplant with RANDOM
		random := pre.Env.Random.Bytes()
		difficulty.SetBytes(random)
	}
	vmContext := vm.BlockContext{
		CanTransfer:     core.CanTransfer,
		Transfer:        core.Transfer,
		Coinbase:        pre.Env.Coinbase,
		BlockNumber:     pre.Env.Number,
		ContractHasTEVM: func(common.Hash) (bool, error) { return false, nil },
		Time:            pre.Env.Timestamp,
		Difficulty:      difficulty,
		GasLimit:        pre.Env.GasLimit,
		GetHash:         getHash,
	}
	// If currentBaseFee is defined, add it to the vmContext.
	if pre.Env.BaseFee != nil {
		vmContext.BaseFee = new(uint256.Int)
		overflow := vmContext.BaseFee.SetFromBig(pre.Env.BaseFee)
		if overflow {
			return nil, nil, fmt.Errorf("pre.Env.BaseFee higher than 2^256-1")
		}
	}
	// If DAO is supported/enabled, we need to handle it here. In geth 'proper', it's
	// done in StateProcessor.Process(block, ...), right before transactions are applied.
	if chainConfig.DAOForkSupport &&
		chainConfig.DAOForkBlock != nil &&
		chainConfig.DAOForkBlock.Cmp(new(big.Int).SetUint64(pre.Env.Number)) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	systemcontracts.UpgradeBuildInSystemContract(chainConfig, new(big.Int).SetUint64(pre.Env.Number), ibs)

	for i, txn := range txs {
		msg, err := txn.AsMessage(*signer, pre.Env.BaseFee)
		if err != nil {
			log.Warn("rejected txn", "index", i, "hash", txn.Hash(), "error", err)
			rejectedTxs = append(rejectedTxs, &rejectedTx{i, err.Error()})
			continue
		}
		tracer, err := getTracerFn(txIndex, txn.Hash())
		if err != nil {
			return nil, nil, err
		}
		vmConfig.Tracer = tracer
		vmConfig.Debug = (tracer != nil)
		ibs.Prepare(txn.Hash(), blockHash, txIndex)
		txContext := core.NewEVMTxContext(msg)
		snapshot := ibs.Snapshot()
		evm := vm.NewEVM(vmContext, txContext, ibs, chainConfig, vmConfig)

		// (ret []byte, usedGas uint64, failed bool, err error)
		msgResult, err := core.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */)
		if err != nil {
			ibs.RevertToSnapshot(snapshot)
			log.Info("rejected txn", "index", i, "hash", txn.Hash(), "from", msg.From(), "error", err)
			rejectedTxs = append(rejectedTxs, &rejectedTx{i, err.Error()})
			continue
		}
		includedTxs = append(includedTxs, txn)
		if hashError != nil {
			return nil, nil, NewError(ErrorMissingBlockhash, hashError)
		}
		gasUsed += msgResult.UsedGas

		// Receipt:
		{
			// Create a new receipt for the transaction, storing the intermediate root and
			// gas used by the txn.
			receipt := &types.Receipt{Type: txn.Type(), CumulativeGasUsed: gasUsed}
			if msgResult.Failed() {
				receipt.Status = types.ReceiptStatusFailed
			} else {
				receipt.Status = types.ReceiptStatusSuccessful
			}
			receipt.TxHash = txn.Hash()
			receipt.GasUsed = msgResult.UsedGas

			// If the transaction created a contract, store the creation address in the receipt.
			if msg.To() == nil {
				receipt.ContractAddress = crypto.CreateAddress(evm.TxContext().Origin, txn.GetNonce())
			}

			// Set the receipt logs and create a bloom for filtering
			receipt.Logs = ibs.GetLogs(txn.Hash())
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
			blockReward = uint256.NewInt(uint64(miningReward))
			minerReward = uint256.NewInt(0).Set(blockReward)
			perOmmer    = uint256.NewInt(0).Div(blockReward, uint256.NewInt(32))
		)
		for _, ommer := range pre.Env.Ommers {
			// Add 1/32th for each ommer included
			minerReward.Add(minerReward, perOmmer)
			// Add (8-delta)/8
			reward := uint256.NewInt(8)
			reward.Sub(reward, uint256.NewInt(ommer.Delta))
			reward.Mul(reward, blockReward)
			reward.Div(reward, uint256.NewInt(8))
			ibs.AddBalance(ommer.Address, reward)
		}
		ibs.AddBalance(pre.Env.Coinbase, minerReward)
	}

	// Commit block
	var root common.Hash
	if err = ibs.FinalizeTx(chainConfig.Rules(1), state.NewPlainStateWriter(tx, tx, 1)); err != nil {
		return nil, nil, err
	}
	root, err = trie.CalcRoot("", tx)
	if err != nil {
		return nil, nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, nil, err
	}

	execRs := &ExecutionResult{
		StateRoot:   root,
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: types.DeriveSha(receipts),
		Bloom:       types.CreateBloom(receipts),
		LogsHash:    rlpHash(ibs.Logs()),
		Receipts:    receipts,
		Rejected:    rejectedTxs,
	}
	return db, execRs, nil
}

func MakePreState(chainRules params.Rules, tx kv.RwTx, accounts core.GenesisAlloc) *state.IntraBlockState {
	var blockNr uint64 = 0
	r, _ := state.NewPlainStateReader(tx), state.NewPlainStateWriter(tx, tx, blockNr)
	statedb := state.New(r)
	for addr, a := range accounts {
		statedb.SetCode(addr, a.Code)
		statedb.SetNonce(addr, a.Nonce)
		balance, _ := uint256.FromBig(a.Balance)
		statedb.SetBalance(addr, balance)
		for k, v := range a.Storage {
			key := k
			val := uint256.NewInt(0).SetBytes(v.Bytes())
			statedb.SetState(addr, &key, *val)
		}

		if len(a.Code) > 0 || len(a.Storage) > 0 {
			statedb.SetIncarnation(addr, 1)
		}
	}
	// Commit and re-open to start with a clean state.
	if err := statedb.FinalizeTx(chainRules, state.NewPlainStateWriter(tx, tx, blockNr+1)); err != nil {
		panic(err)
	}
	if err := statedb.CommitBlock(chainRules, state.NewPlainStateWriter(tx, tx, blockNr+1)); err != nil {
		panic(err)
	}
	return statedb
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x) //nolint:errcheck
	hw.Sum(h[:0])
	return h
}
