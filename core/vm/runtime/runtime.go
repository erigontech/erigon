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

package runtime

import (
	"context"
	"math"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
)

// Config is a basic type specifying certain configuration flags for running
// the EVM.
type Config struct {
	ChainConfig *chain.Config
	Difficulty  *big.Int
	Origin      libcommon.Address
	Coinbase    libcommon.Address
	BlockNumber *big.Int
	Time        *big.Int
	GasLimit    uint64
	GasPrice    *uint256.Int
	Value       *uint256.Int
	Debug       bool
	EVMConfig   vm.Config
	BaseFee     *uint256.Int

	State     *state.IntraBlockState
	r         state.StateReader
	w         state.StateWriter
	GetHashFn func(n uint64) libcommon.Hash
}

// sets defaults on the config
func setDefaults(cfg *Config) {
	if cfg.ChainConfig == nil {
		cfg.ChainConfig = &chain.Config{
			ChainID:               big.NewInt(1),
			HomesteadBlock:        new(big.Int),
			TangerineWhistleBlock: new(big.Int),
			SpuriousDragonBlock:   new(big.Int),
			ByzantiumBlock:        new(big.Int),
			ConstantinopleBlock:   new(big.Int),
			PetersburgBlock:       new(big.Int),
			IstanbulBlock:         new(big.Int),
			MuirGlacierBlock:      new(big.Int),
			BerlinBlock:           new(big.Int),
			LondonBlock:           new(big.Int),
			ArrowGlacierBlock:     new(big.Int),
			GrayGlacierBlock:      new(big.Int),
			ShanghaiTime:          new(big.Int),
			CancunTime:            new(big.Int),
			PragueTime:            new(big.Int),
		}
	}

	if cfg.Difficulty == nil {
		cfg.Difficulty = new(big.Int)
	}
	if cfg.Time == nil {
		cfg.Time = big.NewInt(time.Now().Unix())
	}
	if cfg.GasLimit == 0 {
		cfg.GasLimit = math.MaxUint64
	}
	if cfg.GasPrice == nil {
		cfg.GasPrice = new(uint256.Int)
	}
	if cfg.Value == nil {
		cfg.Value = new(uint256.Int)
	}
	if cfg.BlockNumber == nil {
		cfg.BlockNumber = new(big.Int)
	}
	if cfg.GetHashFn == nil {
		cfg.GetHashFn = func(n uint64) libcommon.Hash {
			return libcommon.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String())))
		}
	}
}

// Execute executes the code using the input as call data during the execution.
// It returns the EVM's return value, the new state and an error if it failed.
//
// Execute sets up an in-memory, temporary, environment for the execution of
// the given code. It makes sure that it's restored to its original state afterwards.
func Execute(code, input []byte, cfg *Config, bn uint64) ([]byte, *state.IntraBlockState, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	setDefaults(cfg)

	externalState := cfg.State != nil
	var tx kv.RwTx
	var err error
	if !externalState {
		db := memdb.New("")
		defer db.Close()
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return nil, nil, err
		}
		defer tx.Rollback()
		cfg.r = state.NewPlainStateReader(tx)
		cfg.w = state.NewPlainStateWriter(tx, tx, 0)
		cfg.State = state.New(cfg.r)
	}
	var (
		address = libcommon.BytesToAddress([]byte("contract"))
		vmenv   = NewEnv(cfg)
		sender  = vm.AccountRef(cfg.Origin)
	)
	if rules := cfg.ChainConfig.Rules(vmenv.Context().BlockNumber, vmenv.Context().Time); rules.IsBerlin {
		cfg.State.PrepareAccessList(cfg.Origin, &address, vm.ActivePrecompiles(rules), nil)
	}
	cfg.State.CreateAccount(address, true)
	// set the receiver's (the executing contract) code for execution.
	cfg.State.SetCode(address, code)
	// Call the code with the given configuration.
	ret, _, err := vmenv.Call(
		sender,
		libcommon.BytesToAddress([]byte("contract")),
		input,
		cfg.GasLimit,
		cfg.Value,
		false, /* bailout */
	)

	return ret, cfg.State, err
}

// Create executes the code using the EVM create method
func Create(input []byte, cfg *Config, blockNr uint64) ([]byte, libcommon.Address, uint64, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	setDefaults(cfg)

	externalState := cfg.State != nil
	var tx kv.RwTx
	var err error
	if !externalState {
		db := memdb.New("")
		defer db.Close()
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return nil, [20]byte{}, 0, err
		}
		defer tx.Rollback()
		cfg.r = state.NewPlainStateReader(tx)
		cfg.w = state.NewPlainStateWriter(tx, tx, 0)
		cfg.State = state.New(cfg.r)
	}
	var (
		vmenv  = NewEnv(cfg)
		sender = vm.AccountRef(cfg.Origin)
	)
	if rules := cfg.ChainConfig.Rules(vmenv.Context().BlockNumber, vmenv.Context().Time); rules.IsBerlin {
		cfg.State.PrepareAccessList(cfg.Origin, nil, vm.ActivePrecompiles(rules), nil)
	}

	// Call the code with the given configuration.
	code, address, leftOverGas, err := vmenv.Create(
		sender,
		input,
		cfg.GasLimit,
		cfg.Value,
	)
	return code, address, leftOverGas, err
}

// Call executes the code given by the contract's address. It will return the
// EVM's return value or an error if it failed.
//
// Call, unlike Execute, requires a config and also requires the State field to
// be set.
func Call(address libcommon.Address, input []byte, cfg *Config) ([]byte, uint64, error) {
	setDefaults(cfg)

	vmenv := NewEnv(cfg)

	sender := cfg.State.GetOrNewStateObject(cfg.Origin)
	statedb := cfg.State
	if rules := cfg.ChainConfig.Rules(vmenv.Context().BlockNumber, vmenv.Context().Time); rules.IsBerlin {
		statedb.PrepareAccessList(cfg.Origin, &address, vm.ActivePrecompiles(rules), nil)
	}

	// Call the code with the given configuration.
	ret, leftOverGas, err := vmenv.Call(
		sender,
		address,
		input,
		cfg.GasLimit,
		cfg.Value,
		false, /* bailout */
	)

	return ret, leftOverGas, err
}
