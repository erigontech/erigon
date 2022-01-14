// Copyright 2015 The go-ethereum Authors
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

package tests

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/common"
)

// StateTest checks transaction processing without block context.
// See https://github.com/ethereum/EIPs/issues/176 for the test format specification.
type StateTest struct {
	json stJSON
}

// StateSubtest selects a specific configuration of a General State Test.
type StateSubtest struct {
	Fork  string
	Index int
}

func (t *StateTest) UnmarshalJSON(in []byte) error {
	return json.Unmarshal(in, &t.json)
}

type stJSON struct {
	Env  stEnv                    `json:"env"`
	Pre  core.GenesisAlloc        `json:"pre"`
	Out  hexutil.Bytes            `json:"out"`
	Post map[string][]stPostState `json:"post"`
}

type stPostState struct {
	Root common.Hash   `json:"hash"`
	Logs common.Hash   `json:"logs"`
	Tx   hexutil.Bytes `json:"txbytes"`
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go

type stEnv struct {
	Coinbase   common.Address `json:"currentCoinbase"   gencodec:"required"`
	Difficulty *big.Int       `json:"currentDifficulty" gencodec:"required"`
	GasLimit   uint64         `json:"currentGasLimit"   gencodec:"required"`
	Number     uint64         `json:"currentNumber"     gencodec:"required"`
	Timestamp  uint64         `json:"currentTimestamp"  gencodec:"required"`
	BaseFee    *big.Int       `json:"currentBaseFee"    gencodec:"optional"`
}

type stEnvMarshaling struct {
	Coinbase   common.UnprefixedAddress
	Difficulty *math.HexOrDecimal256
	GasLimit   math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	BaseFee    *math.HexOrDecimal256
}

// GetChainConfig takes a fork definition and returns a chain config.
// The fork definition can be
// - a plain forkname, e.g. `Byzantium`,
// - a fork basename, and a list of EIPs to enable; e.g. `Byzantium+1884+1283`.
func GetChainConfig(forkString string) (baseConfig *params.ChainConfig, eips []int, err error) {
	var (
		splitForks            = strings.Split(forkString, "+")
		ok                    bool
		baseName, eipsStrings = splitForks[0], splitForks[1:]
	)
	if baseConfig, ok = Forks[baseName]; !ok {
		return nil, nil, UnsupportedForkError{baseName}
	}
	for _, eip := range eipsStrings {
		if eipNum, err := strconv.Atoi(eip); err != nil {
			return nil, nil, fmt.Errorf("syntax error, invalid eip number %v", eipNum)
		} else {
			if !vm.ValidEip(eipNum) {
				return nil, nil, fmt.Errorf("syntax error, invalid eip number %v", eipNum)
			}
			eips = append(eips, eipNum)
		}
	}
	return baseConfig, eips, nil
}

// Subtests returns all valid subtests of the test.
func (t *StateTest) Subtests() []StateSubtest {
	var sub []StateSubtest
	for fork, pss := range t.json.Post {
		for i := range pss {
			sub = append(sub, StateSubtest{fork, i})
		}
	}
	return sub
}

// Run executes a specific subtest and verifies the post-state and logs
func (t *StateTest) Run(rules params.Rules, tx kv.RwTx, subtest StateSubtest, vmconfig vm.Config) (*state.IntraBlockState, error) {
	state, root, err := t.RunNoVerify(rules, tx, subtest, vmconfig)
	if err != nil {
		return state, err
	}
	post := t.json.Post[subtest.Fork][subtest.Index]
	// N.B: We need to do this in a two-step process, because the first Commit takes care
	// of suicides, and we need to touch the coinbase _after_ it has potentially suicided.
	if root != common.Hash(post.Root) {
		return state, fmt.Errorf("post state root mismatch: got %x, want %x", root, post.Root)
	}
	if logs := rlpHash(state.Logs()); logs != common.Hash(post.Logs) {
		return state, fmt.Errorf("post state logs hash mismatch: got %x, want %x", logs, post.Logs)
	}
	return state, nil
}

// RunNoVerify runs a specific subtest and returns the statedb and post-state root
func (t *StateTest) RunNoVerify(rules params.Rules, tx kv.RwTx, subtest StateSubtest, vmconfig vm.Config) (*state.IntraBlockState, common.Hash, error) {
	config, eips, err := GetChainConfig(subtest.Fork)
	if err != nil {
		return nil, common.Hash{}, UnsupportedForkError{subtest.Fork}
	}
	vmconfig.ExtraEips = eips
	block, _, err := t.genesis(config).ToBlock()
	if err != nil {
		return nil, common.Hash{}, UnsupportedForkError{subtest.Fork}
	}

	readBlockNr := block.NumberU64()
	writeBlockNr := readBlockNr + 1

	_, err = MakePreState(params.Rules{}, tx, t.json.Pre, readBlockNr)
	if err != nil {
		return nil, common.Hash{}, UnsupportedForkError{subtest.Fork}
	}
	statedb := state.New(state.NewPlainStateReader(tx))
	w := state.NewPlainStateWriter(tx, nil, writeBlockNr)

	var baseFee *big.Int
	if config.IsLondon(0) {
		baseFee = t.json.Env.BaseFee
		if baseFee == nil {
			// Retesteth uses `0x10` for genesis baseFee. Therefore, it defaults to
			// parent - 2 : 0xa as the basefee for 'this' context.
			baseFee = big.NewInt(0x0a)
		}
	}
	post := t.json.Post[subtest.Fork][subtest.Index]
	txn, err := types.UnmarshalTransactionFromBinary(post.Tx)
	if err != nil {
		return nil, common.Hash{}, err
	}
	msg, err := txn.AsMessage(*types.MakeSigner(config, 0), baseFee)
	if err != nil {
		return nil, common.Hash{}, err
	}

	// Prepare the EVM.
	txContext := core.NewEVMTxContext(msg)
	contractHasTEVM := func(common.Hash) (bool, error) { return false, nil }
	context := core.NewEVMBlockContext(block.Header(), nil, nil, &t.json.Env.Coinbase, contractHasTEVM)
	context.GetHash = vmTestBlockHash
	if baseFee != nil {
		context.BaseFee = new(uint256.Int)
		context.BaseFee.SetFromBig(baseFee)
	}
	evm := vm.NewEVM(context, txContext, statedb, config, vmconfig)

	// Execute the message.
	snapshot := statedb.Snapshot()
	gaspool := new(core.GasPool)
	gaspool.AddGas(block.GasLimit())
	if _, err = core.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */); err != nil {
		statedb.RevertToSnapshot(snapshot)
	}

	if err = statedb.FinalizeTx(evm.ChainRules(), w); err != nil {
		return nil, common.Hash{}, err
	}
	if err = statedb.CommitBlock(evm.ChainRules(), w); err != nil {
		return nil, common.Hash{}, err
	}
	// Generate hashed state
	c, err := tx.RwCursor(kv.PlainState)
	if err != nil {
		return nil, common.Hash{}, err
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, common.Hash{}, fmt.Errorf("interate over plain state: %w", err)
		}
		var newK []byte
		if len(k) == common.AddressLength {
			newK = make([]byte, common.HashLength)
		} else {
			newK = make([]byte, common.HashLength*2+common.IncarnationLength)
		}
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[:common.AddressLength])
		//nolint:errcheck
		h.Sha.Read(newK[:common.HashLength])
		if len(k) > common.AddressLength {
			copy(newK[common.HashLength:], k[common.AddressLength:common.AddressLength+common.IncarnationLength])
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(k[common.AddressLength+common.IncarnationLength:])
			//nolint:errcheck
			h.Sha.Read(newK[common.HashLength+common.IncarnationLength:])
			if err = tx.Put(kv.HashedStorage, newK, common.CopyBytes(v)); err != nil {
				return nil, common.Hash{}, fmt.Errorf("insert hashed key: %w", err)
			}
		} else {
			if err = tx.Put(kv.HashedAccounts, newK, common.CopyBytes(v)); err != nil {
				return nil, common.Hash{}, fmt.Errorf("insert hashed key: %w", err)
			}
		}
	}
	c.Close()

	root, err := trie.CalcRoot("", tx)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("error calculating state root: %w", err)
	}

	return statedb, root, nil
}

func MakePreState(rules params.Rules, tx kv.RwTx, accounts core.GenesisAlloc, blockNr uint64) (*state.IntraBlockState, error) {
	r := state.NewPlainStateReader(tx)
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
	if err := statedb.FinalizeTx(rules, state.NewPlainStateWriter(tx, nil, blockNr+1)); err != nil {
		return nil, err
	}
	if err := statedb.CommitBlock(rules, state.NewPlainStateWriter(tx, nil, blockNr+1)); err != nil {
		return nil, err
	}
	return statedb, nil
}

func (t *StateTest) genesis(config *params.ChainConfig) *core.Genesis {
	return &core.Genesis{
		Config:     config,
		Coinbase:   t.json.Env.Coinbase,
		Difficulty: t.json.Env.Difficulty,
		GasLimit:   t.json.Env.GasLimit,
		Number:     t.json.Env.Number,
		Timestamp:  t.json.Env.Timestamp,
		Alloc:      t.json.Pre,
	}
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	if err := rlp.Encode(hw, x); err != nil {
		panic(err)
	}
	hw.Sum(h[:0])
	return h
}

func vmTestBlockHash(n uint64) common.Hash {
	return common.BytesToHash(crypto.Keccak256([]byte(big.NewInt(int64(n)).String())))
}
