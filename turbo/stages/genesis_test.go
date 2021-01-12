// Copyright 2017 The go-ethereum Authors
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

package stages

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/process"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func TestDefaultGenesisBlock(t *testing.T) {
	block, _, tds, _ := core.DefaultGenesisBlock().ToBlock(nil, true)
	if block.Hash() != params.MainnetGenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), params.MainnetGenesisHash)
	}
	defer tds.Database().Close()
	var err error
	block, _, tds, err = core.DefaultRopstenGenesisBlock().ToBlock(nil, true)
	if err != nil {
		t.Errorf("error: %w", err)
	}
	defer tds.Database().Close()
	if block.Hash() != params.RopstenGenesisHash {
		t.Errorf("wrong ropsten genesis hash, got %v, want %v", block.Hash(), params.RopstenGenesisHash)
	}
}

func TestSetupGenesis(t *testing.T) {
	var (
		customghash = common.HexToHash("0x89c99d90b79719238d2645c7642f2c9295246e80775b38cfd162b696817fbd50")
		customg     = core.Genesis{
			Config: &params.ChainConfig{HomesteadBlock: big.NewInt(3)},
			Alloc: core.GenesisAlloc{
				{1}: {Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{{1}: {1}}},
			},
		}
		oldcustomg = customg
	)
	oldcustomg.Config = &params.ChainConfig{HomesteadBlock: big.NewInt(2)}

	fmt.Printf("config-new %p; config-old %p\n", customg.Config, oldcustomg.Config)

	tests := []struct {
		name       string
		fn         func(*ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error)
		wantConfig *params.ChainConfig
		wantHash   common.Hash
		wantErr    error
	}{
		{
			name: "genesis without ChainConfig",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				return core.SetupGenesisBlock(db, new(core.Genesis), true /* history */, false /* overwrite */)
			},
			wantErr:    core.ErrGenesisNoConfig,
			wantConfig: params.AllEthashProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				return core.SetupGenesisBlock(db, nil, true /* history */, false /* overwrite */)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				return core.SetupGenesisBlock(db, nil, true /* history */, false /* overwrite */)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				customg.MustCommit(db)
				return core.SetupGenesisBlock(db, nil, true /* history */, false /* overwrite */)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == ropsten",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				customg.MustCommit(db)
				return core.SetupGenesisBlock(db, core.DefaultRopstenGenesisBlock(), true /* history */, false /* overwrite */)
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.RopstenGenesisHash},
			wantHash:   params.RopstenGenesisHash,
			wantConfig: params.RopstenChainConfig,
		},
		{
			name: "compatible config in DB",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				oldcustomg.MustCommit(db)
				return core.SetupGenesisBlock(db, &customg, true /* history */, false /* overwrite */)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "incompatible config in DB",
			fn: func(db *ethdb.ObjectDatabase) (*params.ChainConfig, common.Hash, *state.IntraBlockState, error) {
				// Commit the 'old' genesis block with Homestead transition at #2.
				// Advance to block #4, past the homestead transition block of customg.
				genesis := oldcustomg.MustCommit(db)

				fmt.Println("YYYYY 1")
				fmt.Printf("YYYY 1.1 - config-new %p; config-old %p\n", customg.Config, oldcustomg.Config)
				blocks, _, err := core.GenerateChain(oldcustomg.Config, genesis, ethash.NewFaker(), db, 4, nil, false /* intermediateHashes */)
				if err != nil {
					return nil, common.Hash{}, nil, err
				}
				fmt.Printf("YYYY 1.2 - config-new %p; config-old %p\n", customg.Config, oldcustomg.Config)
				fmt.Println("YYYYY 2")
				exit := make(chan struct{})
				cons := ethash.NewFaker()
				eng := process.NewConsensusProcess(cons, oldcustomg.Config, exit)
				fmt.Println("YYYYY 3", len(blocks), blocks[0].Number().Uint64(), "-", blocks[len(blocks)-1].Number().Uint64(), genesis.Number().Uint64())
				defer common.SafeClose(exit)
				if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, oldcustomg.Config, &vm.Config{}, cons, eng, blocks, true /* checkRoot */); err != nil {
					return nil, common.Hash{}, nil, err
				}
				fmt.Println("YYYYY 4")
				// This should return a compatibility error.
				conf, hash, state, err := core.SetupGenesisBlock(db, &customg, true /* history */, false /* overwrite */)
				fmt.Println("YYYYY 5")
				return conf, hash, state, err
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
			wantErr: &params.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: big.NewInt(2),
				NewConfig:    big.NewInt(3),
				RewindTo:     1,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			db := ethdb.NewMemDatabase()
			defer db.Close()
			config, hash, _, err := test.fn(db)
			// Check the return values.
			if !reflect.DeepEqual(err, test.wantErr) {
				spew := spew.ConfigState{DisablePointerAddresses: true, DisableCapacities: true}
				t.Errorf("%s: returned error %#v, want %#v", test.name, spew.NewFormatter(err), spew.NewFormatter(test.wantErr))
			}
			if !reflect.DeepEqual(config, test.wantConfig) {
				t.Errorf("%s:\nreturned %v\nwant     %v", test.name, config, test.wantConfig)
			}
			if hash != test.wantHash {
				t.Errorf("%s: returned hash %s, want %s", test.name, hash.Hex(), test.wantHash.Hex())
			} else if err == nil {
				// Check database content.
				stored := rawdb.ReadBlock(db, test.wantHash, 0)
				if stored.Hash() != test.wantHash {
					t.Errorf("%s: block in DB has hash %s, want %s", test.name, stored.Hash(), test.wantHash)
				}
			}
		})
	}
}
