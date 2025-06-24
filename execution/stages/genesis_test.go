// Copyright 2017 The go-ethereum Authors
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

package stages_test

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chainspec"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/params"
	polychain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func TestSetupGenesis(t *testing.T) {
	t.Parallel()
	var (
		customghash = common.HexToHash("0x89c99d90b79719238d2645c7642f2c9295246e80775b38cfd162b696817fbd50")
		customg     = types.Genesis{
			Config: &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(3)},
			Alloc: types.GenesisAlloc{
				{1}: {Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{{1}: {1}}},
			},
		}
		oldcustomg = customg
		tmpdir     = t.TempDir()
	)
	logger := log.New()
	oldcustomg.Config = &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(2)}
	tests := []struct {
		wantErr    error
		fn         func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error)
		wantConfig *chain.Config
		name       string
		wantHash   common.Hash
	}{
		{
			name: "genesis without ChainConfig",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, new(types.Genesis), datadir.New(tmpdir), logger)
			},
			wantErr:    types.ErrGenesisNoConfig,
			wantConfig: chain.AllProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				core.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return core.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == sepolia",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				core.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return core.CommitGenesisBlock(db, chainspec.SepoliaGenesisBlock(), datadir.New(tmpdir), logger)
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.SepoliaGenesisHash},
			wantHash:   params.SepoliaGenesisHash,
			wantConfig: params.SepoliaChainConfig,
		},
		{
			name: "custom block in DB, genesis == bor-mainnet",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				core.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return core.CommitGenesisBlock(db, polychain.BorMainnetGenesisBlock(), datadir.New(tmpdir), logger)
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.BorMainnetGenesisHash},
			wantHash:   params.BorMainnetGenesisHash,
			wantConfig: polychain.BorMainnetChainConfig,
		},
		{
			name: "custom block in DB, genesis == amoy",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				core.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return core.CommitGenesisBlock(db, polychain.AmoyGenesisBlock(), datadir.New(tmpdir), logger)
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.AmoyGenesisHash},
			wantHash:   params.AmoyGenesisHash,
			wantConfig: polychain.AmoyChainConfig,
		},
		{
			name: "compatible config in DB",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				core.MustCommitGenesis(&oldcustomg, db, datadir.New(tmpdir), logger)
				return core.CommitGenesisBlock(db, &customg, datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "incompatible config in DB",
			fn: func(t *testing.T, db kv.RwDB) (*chain.Config, *types.Block, error) {
				//if ethconfig.EnableHistoryV4InTest {
				//	t.Skip("fix me")
				//}
				// Commit the 'old' genesis block with Homestead transition at #2.
				// Advance to block #4, past the homestead transition block of customg.
				key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				m := mock.MockWithGenesis(t, &oldcustomg, key, false)

				chainBlocks, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, nil)
				if err != nil {
					return nil, nil, err
				}
				if err = m.InsertChain(chainBlocks); err != nil {
					return nil, nil, err
				}
				// This should return a compatibility error.
				return core.CommitGenesisBlock(m.DB, &customg, datadir.New(tmpdir), logger)
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
			t.Parallel()
			dirs := datadir.New(tmpdir)
			db := temporaltest.NewTestDB(t, dirs)
			//cc := tool.ChainConfigFromDB(db)
			freezingCfg := ethconfig.Defaults.Snapshot
			//freezingCfg.ChainName = cc.ChainName //TODO: nil-pointer?
			blockReader := freezeblocks.NewBlockReader(freezeblocks.NewRoSnapshots(freezingCfg, dirs.Snap, 0, log.New()), heimdall.NewRoSnapshots(freezingCfg, dirs.Snap, 0, log.New()), nil, nil)
			config, genesis, err := test.fn(t, db)
			// Check the return values.
			if !reflect.DeepEqual(err, test.wantErr) {
				spew := spew.ConfigState{DisablePointerAddresses: true, DisableCapacities: true}
				t.Fatalf("%s: returned error %#v, want %#v", test.name, spew.NewFormatter(err), spew.NewFormatter(test.wantErr))
			}
			if !reflect.DeepEqual(config, test.wantConfig) {
				t.Errorf("%s:\nreturned %v\nwant     %v", test.name, config, test.wantConfig)
			}

			if test.wantHash == (common.Hash{}) {
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
					stored, _, _ := blockReader.BlockWithSenders(context.Background(), tx, test.wantHash, 0)
					if stored.Hash() != test.wantHash {
						t.Errorf("%s: block in DB has hash %s, want %s", test.name, stored.Hash(), test.wantHash)
					}
					return nil
				}); dbErr != nil {
					t.Fatal(dbErr)
				}
			}
		})
	}
}
