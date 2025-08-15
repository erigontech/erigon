// Copyright 2015 The go-ethereum Authors
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

package runtime

import (
	"context"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// Config is a basic type specifying certain configuration flags for running
// the EVM.
type Config struct {
	ChainConfig *chain.Config
	Difficulty  *big.Int
	Origin      common.Address
	Coinbase    common.Address
	BlockNumber *big.Int
	Time        *big.Int
	GasLimit    uint64
	GasPrice    *uint256.Int
	Value       *uint256.Int
	EVMConfig   vm.Config
	BaseFee     *uint256.Int

	State *state.IntraBlockState

	evm       *vm.EVM
	GetHashFn func(n uint64) (common.Hash, error)
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
			OsakaTime:             new(big.Int),
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
		cfg.GetHashFn = func(n uint64) (common.Hash, error) {
			return common.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String()))), nil
		}
	}
}

// Execute executes the code using the input as call data during the execution.
// It returns the EVM's return value, the new state and an error if it failed.
//
// Execute sets up an in-memory, temporary, environment for the execution of
// the given code. It makes sure that it's restored to its original state afterwards.
func Execute(code, input []byte, cfg *Config, tempdir string) ([]byte, *state.IntraBlockState, error) {
	if cfg == nil {
		cfg = new(Config)
		setDefaults(cfg)
	}

	externalState := cfg.State != nil
	if !externalState {
		db := memdb.NewStateDB(tempdir)
		defer db.Close()
		dirs := datadir.New(tempdir)
		logger := log.New()
		salt, err := dbstate.GetStateIndicesSalt(dirs, true, logger)
		if err != nil {
			return nil, nil, err
		}
		agg, err := dbstate.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		if err != nil {
			return nil, nil, err
		}
		defer agg.Close()
		_db, err := temporal.New(db, agg)
		if err != nil {
			return nil, nil, err
		}
		tx, err := _db.BeginTemporalRw(context.Background()) //nolint:gocritic
		if err != nil {
			return nil, nil, err
		}
		defer tx.Rollback()
		sd, err := dbstate.NewSharedDomains(tx, log.New())
		if err != nil {
			return nil, nil, err
		}
		defer sd.Close()
		//cfg.w = state.NewWriter(sd, nil)
		cfg.State = state.New(state.NewReaderV3(sd.AsGetter(tx)))
	}
	var (
		address = common.BytesToAddress([]byte("contract"))
		vmenv   = NewEnv(cfg)
		sender  = vm.AccountRef(cfg.Origin)
		rules   = vmenv.ChainRules()
	)
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, &address, vm.ActivePrecompiles(rules), nil, nil)
	cfg.State.CreateAccount(address, true)
	// set the receiver's (the executing contract) code for execution.
	cfg.State.SetCode(address, code)
	// Call the code with the given configuration.
	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxStart != nil {
		cfg.EVMConfig.Tracer.OnTxStart(&tracing.VMContext{IntraBlockState: cfg.State}, nil, common.Address{})
	}
	ret, _, err := vmenv.Call(
		sender,
		common.BytesToAddress([]byte("contract")),
		input,
		cfg.GasLimit,
		cfg.Value,
		false, /* bailout */
	)
	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxEnd != nil {
		cfg.EVMConfig.Tracer.OnTxEnd(nil, err)
	}

	return ret, cfg.State, err
}

// Create executes the code using the EVM create method
func Create(input []byte, cfg *Config, blockNr uint64) ([]byte, common.Address, uint64, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	setDefaults(cfg)

	externalState := cfg.State != nil
	if !externalState {
		tmp := filepath.Join(os.TempDir(), "create-vm")
		defer dir.RemoveAll(tmp) //nolint

		db := memdb.NewStateDB(tmp)
		defer db.Close()
		agg, err := dbstate.NewAggregator(context.Background(), datadir.New(tmp), config3.DefaultStepSize, db, log.New())
		if err != nil {
			return nil, [20]byte{}, 0, err
		}
		defer agg.Close()
		_db, err := temporal.New(db, agg)
		if err != nil {
			return nil, [20]byte{}, 0, err
		}
		tx, err := _db.BeginTemporalRw(context.Background()) //nolint:gocritic
		if err != nil {
			return nil, [20]byte{}, 0, err
		}
		defer tx.Rollback()
		sd, err := dbstate.NewSharedDomains(tx, log.New())
		if err != nil {
			return nil, [20]byte{}, 0, err
		}
		defer sd.Close()
		//cfg.w = state.NewWriter(sd, nil)
		cfg.State = state.New(state.NewReaderV3(sd.AsGetter(tx)))
	}
	var (
		vmenv  = NewEnv(cfg)
		sender = vm.AccountRef(cfg.Origin)
		rules  = vmenv.ChainRules()
	)
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, nil, vm.ActivePrecompiles(rules), nil, nil)

	// Call the code with the given configuration.
	code, address, leftOverGas, err := vmenv.Create(
		sender,
		input,
		cfg.GasLimit,
		cfg.Value,
		false,
	)
	return code, address, leftOverGas, err
}

// Call executes the code given by the contract's address. It will return the
// EVM's return value or an error if it failed.
//
// Call, unlike Execute, requires a config and also requires the State field to
// be set.
func Call(address common.Address, input []byte, cfg *Config) ([]byte, uint64, error) {
	setDefaults(cfg)

	vmenv := NewEnv(cfg)

	sender, err := cfg.State.GetOrNewStateObject(cfg.Origin)
	if err != nil {
		return nil, 0, err
	}
	statedb := cfg.State
	rules := vmenv.ChainRules()
	statedb.Prepare(rules, cfg.Origin, cfg.Coinbase, &address, vm.ActivePrecompiles(rules), nil, nil)

	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxStart != nil {
		cfg.EVMConfig.Tracer.OnTxStart(&tracing.VMContext{IntraBlockState: cfg.State}, nil, common.Address{})
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

	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxEnd != nil {
		cfg.EVMConfig.Tracer.OnTxEnd(&types.Receipt{GasUsed: cfg.GasLimit - leftOverGas}, err)
	}

	return ret, leftOverGas, err
}
