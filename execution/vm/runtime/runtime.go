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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/fixedgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// Config is a basic type specifying certain configuration flags for running
// the EVM.
type Config struct {
	ChainConfig *chain.Config
	Difficulty  *uint256.Int
	Origin      accounts.Address
	Coinbase    accounts.Address
	BlockNumber uint64
	Time        uint64
	GasLimit    uint64
	GasPrice    uint256.Int
	Value       uint256.Int
	EVMConfig   vm.Config
	BaseFee     uint256.Int

	State     *state.IntraBlockState
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
			AmsterdamTime:         new(big.Int),
		}
	}

	if cfg.Origin.IsNil() {
		cfg.Origin = accounts.ZeroAddress
	}
	if cfg.Difficulty == nil {
		cfg.Difficulty = new(uint256.Int)
	}
	if cfg.GasLimit == 0 {
		cfg.GasLimit = math.MaxUint64
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

var contractAsAddress = accounts.InternAddress(common.BytesToAddress([]byte("contract")))

func Execute(code, input []byte, cfg *Config, tempdir string) ([]byte, *state.IntraBlockState, error) {
	if cfg == nil {
		cfg = new(Config)
		setDefaults(cfg)
	}

	externalState := cfg.State != nil
	if !externalState {
		dirs := datadir.New(tempdir)
		db := temporaltest.NewTestDB(nil, dirs)
		defer db.Close()

		tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
		if err != nil {
			return nil, nil, err
		}
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		if err != nil {
			return nil, nil, err
		}
		defer sd.Close()
		//cfg.w = state.NewWriter(sd, nil)
		cfg.State = state.New(state.NewReaderV3(sd.AsGetter(tx)))
	}
	var (
		address = contractAsAddress
		vmenv   = NewEnv(cfg)
		sender  = cfg.Origin
		rules   = vmenv.ChainRules()
	)
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, address, vm.ActivePrecompiles(rules), nil, nil)
	cfg.State.CreateAccount(address, true)
	// set the receiver's (the executing contract) code for execution.
	cfg.State.SetCode(address, code)
	// Call the code with the given configuration.
	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxStart != nil {
		cfg.EVMConfig.Tracer.OnTxStart(&tracing.VMContext{IntraBlockState: cfg.State}, nil, accounts.ZeroAddress)
	}
	ret, _, err := vmenv.Call(
		sender,
		contractAsAddress,
		input,
		protocol.NonIntrinsicMdGas(cfg.GasLimit, fixedgas.IntrinsicGasCalcResult{}, rules, cfg.EVMConfig.Tracer),
		cfg.Value,
		false, /* bailout */
	)
	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxEnd != nil {
		cfg.EVMConfig.Tracer.OnTxEnd(nil, err)
	}

	return ret, cfg.State, err
}

// Create executes the code using the EVM create method
func Create(input []byte, cfg *Config, blockNr uint64) ([]byte, common.Address, evmtypes.MdGas, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	setDefaults(cfg)

	externalState := cfg.State != nil
	if !externalState {
		tmp, err := os.MkdirTemp("", "erigon-create-vm-*")
		if err != nil {
			return nil, [20]byte{}, evmtypes.MdGas{}, err
		}
		defer dir.RemoveAll(tmp)

		dirs := datadir.New(tmp)
		db := temporaltest.NewTestDB(nil, dirs)
		defer db.Close()
		tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
		if err != nil {
			return nil, [20]byte{}, evmtypes.MdGas{}, err
		}
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		if err != nil {
			return nil, [20]byte{}, evmtypes.MdGas{}, err
		}
		defer sd.Close()
		//cfg.w = state.NewWriter(sd, nil)
		cfg.State = state.New(state.NewReaderV3(sd.AsGetter(tx)))
	}
	var (
		vmenv  = NewEnv(cfg)
		sender = cfg.Origin
		rules  = vmenv.ChainRules()
	)
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, accounts.NilAddress, vm.ActivePrecompiles(rules), nil, nil)

	// Call the code with the given configuration.
	code, address, leftOverGas, err := vmenv.Create(
		sender,
		input,
		protocol.NonIntrinsicMdGas(cfg.GasLimit, fixedgas.IntrinsicGasCalcResult{}, rules, cfg.EVMConfig.Tracer),
		cfg.Value,
		false,
	)
	return code, address.Value(), leftOverGas, err
}

// Call executes the code given by the contract's address. It will return the
// EVM's return value or an error if it failed.
//
// Call, unlike Execute, requires a config and also requires the State field to
// be set.
func Call(address accounts.Address, input []byte, cfg *Config) ([]byte, evmtypes.MdGas, error) {
	setDefaults(cfg)

	vmenv := NewEnv(cfg)

	sender, err := cfg.State.GetOrNewStateObject(cfg.Origin)
	if err != nil {
		return nil, evmtypes.MdGas{}, err
	}
	statedb := cfg.State
	rules := vmenv.ChainRules()
	statedb.Prepare(rules, cfg.Origin, cfg.Coinbase, address, vm.ActivePrecompiles(rules), nil, nil)

	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxStart != nil {
		cfg.EVMConfig.Tracer.OnTxStart(&tracing.VMContext{IntraBlockState: cfg.State}, nil, accounts.ZeroAddress)
	}

	// Call the code with the given configuration.
	ret, leftOverGas, err := vmenv.Call(
		sender.Address(),
		address,
		input,
		protocol.NonIntrinsicMdGas(cfg.GasLimit, fixedgas.IntrinsicGasCalcResult{}, rules, cfg.EVMConfig.Tracer),
		cfg.Value,
		false, /* bailout */
	)

	if cfg.EVMConfig.Tracer != nil && cfg.EVMConfig.Tracer.OnTxEnd != nil {
		cfg.EVMConfig.Tracer.OnTxEnd(&types.Receipt{GasUsed: cfg.GasLimit - leftOverGas.Total()}, err)
	}

	return ret, leftOverGas, err
}
