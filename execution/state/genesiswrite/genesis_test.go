// Copyright 2024 The Erigon Authors
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

package genesiswrite_test

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestGenesisBlockHashes(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	check := func(network string) {
		spec, err := chainspec.ChainSpecByName(network)
		require.NoError(t, err)
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()

		_, block, err := genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, false, datadir.New(t.TempDir()), logger)
		require.NoError(t, err)

		expect, err := chainspec.ChainSpecByName(network)
		require.NoError(t, err)
		require.NotEmpty(t, expect.GenesisHash, network)
		require.Equal(t, block.Hash(), expect.GenesisHash, network)
	}
	for _, network := range networkname.All {
		check(network)
	}
}

func TestGenesisBlockRoots(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	block, _, err := genesiswrite.GenesisToBlock(t, chainspec.MainnetGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Hash() != chainspec.Mainnet.GenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), chainspec.Mainnet.GenesisHash)
	}
	for _, netw := range []string{
		networkname.Gnosis,
		networkname.Chiado,
		networkname.Test,
	} {
		spec, err := chainspec.ChainSpecByName(netw)
		require.NoError(err)
		require.False(spec.IsEmpty())

		block, _, err = genesiswrite.GenesisToBlock(t, spec.Genesis, datadir.New(t.TempDir()), log.Root())
		require.NoError(err)

		if block.Root() != spec.GenesisStateRoot {
			t.Errorf("wrong %s Chain genesis state root, got %v, want %v", netw, block.Root(), spec.GenesisStateRoot)
		}

		if block.Hash() != spec.GenesisHash {
			t.Errorf("wrong %s Chain genesis hash, got %v, want %v", netw, block.Hash(), spec.GenesisHash)
		}
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	spec := chainspec.Mainnet
	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, false, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, false, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}

func TestAllocConstructor(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)

	// This deployment code initially sets contract's 0th storage to 0x2a
	// and its 1st storage to 0x01c9.
	deploymentCode := common.FromHex("602a5f556101c960015560048060135f395ff35f355f55")

	funds := big.NewInt(1000000000)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	genSpec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			address: {Constructor: deploymentCode, Balance: funds},
		},
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := mock.MockWithGenesis(t, genSpec, key, false)

	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	//TODO: support historyV3
	reader, err := rpchelper.CreateHistoryStateReader(tx, 1, 0, rawdbv3.TxNums)
	require.NoError(err)
	state := state.New(reader)
	balance, err := state.GetBalance(address)
	require.NoError(err)
	assert.Equal(funds, balance.ToBig())
	code, err := state.GetCode(address)
	require.NoError(err)
	assert.Equal(common.FromHex("5f355f55"), code)

	key0 := common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000")
	storage0 := &uint256.Int{}
	state.GetState(address, key0, storage0)
	assert.Equal(uint256.NewInt(0x2a), storage0)
	key1 := common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001")
	storage1 := &uint256.Int{}
	state.GetState(address, key1, storage1)
	assert.Equal(uint256.NewInt(0x01c9), storage1)
}

// See https://github.com/erigontech/erigon/pull/11264
func TestDecodeBalance0(t *testing.T) {
	genesisData, err := os.ReadFile("./genesis_test.json")
	require.NoError(t, err)

	genesis := &types.Genesis{}
	err = json.Unmarshal(genesisData, genesis)
	require.NoError(t, err)
	_ = genesisData
}

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
	)
	logger := log.New()
	oldcustomg.Config = &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(2)}
	tests := []struct {
		wantErr    error
		fn         func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error)
		wantConfig *chain.Config
		name       string
		wantHash   common.Hash
	}{
		{
			name: "genesis without ChainConfig",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				return genesiswrite.CommitGenesisBlock(db, new(types.Genesis), datadir.New(tmpdir), logger)
			},
			wantErr:    types.ErrGenesisNoConfig,
			wantConfig: chain.AllProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				return genesiswrite.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   chainspec.Mainnet.GenesisHash,
			wantConfig: chainspec.Mainnet.Config,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				return genesiswrite.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   chainspec.Mainnet.GenesisHash,
			wantConfig: chainspec.Mainnet.Config,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, nil, datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == sepolia",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, chainspec.SepoliaGenesisBlock(), datadir.New(tmpdir), logger)
			},
			wantErr:    &genesiswrite.GenesisMismatchError{Stored: customghash, New: chainspec.Sepolia.GenesisHash},
			wantHash:   chainspec.Sepolia.GenesisHash,
			wantConfig: chainspec.Sepolia.Config,
		},
		{
			name: "compatible config in DB",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&oldcustomg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, &customg, datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "incompatible config in DB",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				//if ethconfig.EnableHistoryV4InTest {
				//	t.Skip("fix me")
				//}
				// Commit the 'old' genesis block with Homestead transition at #2.
				// Advance to block #4, past the homestead transition block of customg.
				key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				m := mock.MockWithGenesis(t, &oldcustomg, key, false)

				chainBlocks, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, nil)
				if err != nil {
					return nil, nil, err
				}
				if err = m.InsertChain(chainBlocks); err != nil {
					return nil, nil, err
				}
				// This should return a compatibility error.
				return genesiswrite.CommitGenesisBlock(m.DB, &customg, datadir.New(tmpdir), logger)
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
		t.Run(test.name, func(t *testing.T) {
			tmpdir := t.TempDir()
			dirs := datadir.New(tmpdir)
			db := temporaltest.NewTestDB(t, dirs)
			//cc := tool.ChainConfigFromDB(db)
			freezingCfg := ethconfig.Defaults.Snapshot
			//freezingCfg.ChainName = cc.ChainName //TODO: nil-pointer?
			blockReader := freezeblocks.NewBlockReader(freezeblocks.NewRoSnapshots(freezingCfg, dirs.Snap, log.New()), nil)
			config, genesis, err := test.fn(t, db, tmpdir)
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
