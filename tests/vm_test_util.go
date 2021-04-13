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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// VMTest checks EVM execution without block or transaction context.
// See https://github.com/ethereum/tests/wiki/VM-Tests for the test format specification.
type VMTest struct {
	json vmJSON
}

func (t *VMTest) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &t.json)
}

type vmJSON struct {
	Env           stEnv                 `json:"env"`
	Exec          vmExec                `json:"exec"`
	Logs          common.UnprefixedHash `json:"logs"`
	GasRemaining  *math.HexOrDecimal64  `json:"gas"`
	Out           hexutil.Bytes         `json:"out"`
	Pre           core.GenesisAlloc     `json:"pre"`
	Post          core.GenesisAlloc     `json:"post"`
	PostStateRoot common.Hash           `json:"postStateRoot"`
}

//go:generate gencodec -type vmExec -field-override vmExecMarshaling -out gen_vmexec.go

type vmExec struct {
	Address  common.Address `json:"address"  gencodec:"required"`
	Caller   common.Address `json:"caller"   gencodec:"required"`
	Origin   common.Address `json:"origin"   gencodec:"required"`
	Code     []byte         `json:"code"     gencodec:"required"`
	Data     []byte         `json:"data"     gencodec:"required"`
	Value    *big.Int       `json:"value"    gencodec:"required"`
	GasLimit uint64         `json:"gas"      gencodec:"required"`
	GasPrice *big.Int       `json:"gasPrice" gencodec:"required"`
}

type vmExecMarshaling struct {
	Address  common.UnprefixedAddress
	Caller   common.UnprefixedAddress
	Origin   common.UnprefixedAddress
	Code     hexutil.Bytes
	Data     hexutil.Bytes
	Value    *math.HexOrDecimal256
	GasLimit math.HexOrDecimal64
	GasPrice *math.HexOrDecimal256
}

func (t *VMTest) Run(vmconfig vm.Config, blockNr uint64) error {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	ctx := params.MainnetChainConfig.WithEIPsFlags(context.Background(), blockNr)
	state, err := MakePreState2(ctx, db, t.json.Pre, blockNr)
	if err != nil {
		return fmt.Errorf("error in MakePreState: %v", err)
	}
	ret, gasRemaining, err := t.exec(state, vmconfig)
	// err is not supposed to be checked here, because in VM tests, the failure
	// is indicated by the absence of the post-condition section.
	// In other words, when such section is not present, we expect an error
	if t.json.GasRemaining == nil {
		if err == nil {
			return fmt.Errorf("gas unspecified (indicating an error), but VM returned no error")
		}
		if gasRemaining > 0 {
			return fmt.Errorf("gas unspecified (indicating an error), but VM returned gas remaining > 0")
		}
		return nil
	}
	// Test declares gas, expecting outputs to match.
	if !bytes.Equal(ret, t.json.Out) {
		return fmt.Errorf("return data mismatch: got %x, want %x", ret, t.json.Out)
	}
	if gasRemaining != uint64(*t.json.GasRemaining) {
		return fmt.Errorf("remaining gas %v, want %v", gasRemaining, *t.json.GasRemaining)
	}
	var haveV uint256.Int
	for addr, account := range t.json.Post {
		for k, wantV := range account.Storage {
			key := k
			state.GetState(addr, &key, &haveV)
			if haveV.Bytes32() != wantV {
				return fmt.Errorf("wrong storage value at %x:\n  got  %x\n  want %x", k, haveV, wantV)
			}
		}
	}
	root, err := trie.CalcRoot("test", db)
	if err != nil {
		return fmt.Errorf("Error calculating state root: %v", err)
	}
	if t.json.PostStateRoot != (common.Hash{}) && root != t.json.PostStateRoot {
		return fmt.Errorf("post state root mismatch, got %x, want %x", root, t.json.PostStateRoot)
	}
	if logs := rlpHash(state.Logs()); logs != common.Hash(t.json.Logs) {
		return fmt.Errorf("post state logs hash mismatch: got %x, want %x", logs, t.json.Logs)
	}
	return nil
}

func (t *VMTest) exec(state vm.IntraBlockState, vmconfig vm.Config) ([]byte, uint64, error) {
	evm := t.newEVM(state, vmconfig)
	e := t.json.Exec
	value, _ := uint256.FromBig(e.Value)
	return evm.Call(vm.AccountRef(e.Caller), e.Address, e.Data, e.GasLimit, value, false /* bailout */)
}

func (t *VMTest) newEVM(state vm.IntraBlockState, vmconfig vm.Config) *vm.EVM {
	initialCall := true
	canTransfer := func(db vm.IntraBlockState, address common.Address, amount *uint256.Int) bool {
		if initialCall {
			initialCall = false
			return true
		}
		return core.CanTransfer(db, address, amount)
	}
	txContext := vm.TxContext{
		Origin:   t.json.Exec.Origin,
		GasPrice: t.json.Exec.GasPrice,
	}
	transfer := func(db vm.IntraBlockState, sender, recipient common.Address, amount *uint256.Int, bailout bool) {}
	context := vm.BlockContext{
		CanTransfer: canTransfer,
		Transfer:    transfer,
		GetHash:     vmTestBlockHash,
		Coinbase:    t.json.Env.Coinbase,
		BlockNumber: t.json.Env.Number,
		Time:        t.json.Env.Timestamp,
		GasLimit:    t.json.Env.GasLimit,
		Difficulty:  t.json.Env.Difficulty,
	}
	vmconfig.NoRecursion = true
	return vm.NewEVM(context, txContext, state, params.MainnetChainConfig, vmconfig)
}

func vmTestBlockHash(n uint64) common.Hash {
	return common.BytesToHash(crypto.Keccak256([]byte(big.NewInt(int64(n)).String())))
}
