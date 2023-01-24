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

package stages_test

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestSetupGenesis(t *testing.T) {
	var (
		customghash = libcommon.HexToHash("0x89c99d90b79719238d2645c7642f2c9295246e80775b38cfd162b696817fbd50")
		customg     = core.Genesis{
			Config: &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(3)},
			Alloc: core.GenesisAlloc{
				{1}: {Balance: big.NewInt(1), Storage: map[libcommon.Hash]libcommon.Hash{{1}: {1}}},
			},
		}
		oldcustomg = customg
	)
	oldcustomg.Config = &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(2)}
	tests := []struct {
		wantErr    error
		fn         func(kv.RwDB) (*chain.Config, *types.Block, error)
		wantConfig *chain.Config
		name       string
		wantHash   libcommon.Hash
	}{
		{
			name: "genesis without ChainConfig",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, new(core.Genesis))
			},
			wantErr:    core.ErrGenesisNoConfig,
			wantConfig: params.AllProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				customg.MustCommit(db)
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == sepolia",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				customg.MustCommit(db)
				return core.CommitGenesisBlock(db, core.DefaultSepoliaGenesisBlock())
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.SepoliaGenesisHash},
			wantHash:   params.SepoliaGenesisHash,
			wantConfig: params.SepoliaChainConfig,
		},
		{
			name: "compatible config in DB",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				oldcustomg.MustCommit(db)
				return core.CommitGenesisBlock(db, &customg)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "incompatible config in DB",
			fn: func(db kv.RwDB) (*chain.Config, *types.Block, error) {
				// Commit the 'old' genesis block with Homestead transition at #2.
				// Advance to block #4, past the homestead transition block of customg.
				key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				m := stages.MockWithGenesis(t, &oldcustomg, key, false)

				chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, nil, false /* intermediateHashes */)
				if err != nil {
					return nil, nil, err
				}
				if err = m.InsertChain(chain); err != nil {
					return nil, nil, err
				}
				// This should return a compatibility error.
				return core.CommitGenesisBlock(m.DB, &customg)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
			wantErr: &chain.ConfigCompatError{
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
			db := memdb.NewTestDB(t)
			config, genesis, err := test.fn(db)
			// Check the return values.
			if !reflect.DeepEqual(err, test.wantErr) {
				spew := spew.ConfigState{DisablePointerAddresses: true, DisableCapacities: true}
				t.Fatalf("%s: returned error %#v, want %#v", test.name, spew.NewFormatter(err), spew.NewFormatter(test.wantErr))
			}
			if !reflect.DeepEqual(config, test.wantConfig) {
				t.Errorf("%s:\nreturned %v\nwant     %v", test.name, config, test.wantConfig)
			}

			if test.wantHash == (libcommon.Hash{}) {
				if genesis != nil {
					t.Fatalf("%s: returned non-nil genesis block, want nil", test.name)
				}
				return
			}

			if genesis.Hash() != test.wantHash {
				t.Errorf("%s: returned hash %s, want %s", test.name, genesis.Hash().Hex(), test.wantHash.Hex())
			} else if err == nil {
				if dbErr := db.View(context.Background(), func(tx kv.Tx) error {
					// Check database content.
					stored := rawdb.ReadBlock(tx, test.wantHash, 0)
					if stored.Hash() != test.wantHash {
						t.Errorf("%s: block in DB has hash %s, want %s", test.name, stored.Hash(), test.wantHash)
					}
					return nil
				}); dbErr != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
