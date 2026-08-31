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
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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

		_, block, err := genesiswrite.WriteGenesisBlock(tx, spec.Genesis, network, nil, nil, false, datadir.New(t.TempDir()), logger)
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
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	require := require.New(t)

	block, ibs, err := genesiswrite.GenesisToBlock(chainspec.MainnetGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	ibs.Close()

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

		block, ibs, err = genesiswrite.GenesisToBlock(spec.Genesis, datadir.New(t.TempDir()), log.Root())
		require.NoError(err)
		ibs.Close()

		if block.Root() != spec.GenesisStateRoot {
			t.Errorf("wrong %s Chain genesis state root, got %v, want %v", netw, block.Root(), spec.GenesisStateRoot)
		}

		if block.Hash() != spec.GenesisHash {
			t.Errorf("wrong %s Chain genesis hash, got %v, want %v", netw, block.Hash(), spec.GenesisHash)
		}
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	spec := chainspec.Mainnet
	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, networkname.Mainnet, nil, nil, false, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, networkname.Mainnet, nil, nil, false, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}

// The fresh-DB path overrides genesis.Config, and MainnetGenesisBlock hands back the
// package-level config, so without a copy --override.osaka rewrites it process-wide.
func TestCommitGenesisBlockOverrideLeavesTheSpecConfigAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	// MainnetGenesisBlock's Config is the mainnetChainConfig singleton, which is a
	// different object from chainspec.Mainnet.Config -- that one is its own ReadChainConfig.
	orig := chainspec.MainnetGenesisBlock().Config.OsakaTime
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	cfg, _, err := genesiswrite.CommitGenesisBlockWithOverride(
		db, nil, "", common.NewUint64(1765000000), nil, false, dirs, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(1765000000), *cfg.OsakaTime, "the override must reach the returned config")
	require.Equal(t, orig, chainspec.MainnetGenesisBlock().Config.OsakaTime,
		"applyOverrides must not write through into the shared genesis config")
}

// The fresh-DB path reassigns genesis.Config on the caller's Genesis struct, and a named
// chain hands over the registered one, so an override there outlives the call.
func TestCommitGenesisBlockOverrideLeavesTheSpecGenesisAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	spec, err := chainspec.ChainSpecByName(networkname.Sepolia)
	require.NoError(t, err)
	orig := spec.Genesis.Config.OsakaTime

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(
		db, spec.Genesis, networkname.Sepolia, common.NewUint64(1760500000), nil, false, dirs, log.New())
	require.NoError(t, err)

	after, err := chainspec.ChainSpecByName(networkname.Sepolia)
	require.NoError(t, err)
	require.Equal(t, orig, after.Genesis.Config.OsakaTime,
		"applyOverrides must not reach the registered spec's Genesis")
}

func TestCommitGenesisBlockWithOverrideKeepStoredChainConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	// configOrDefault hands back chain.AllProtocolChanges for an empty chain name, so
	// this also pins that the overrides land on a copy of it rather than on the global.
	origOsakaTime := chain.AllProtocolChanges.OsakaTime

	logger := log.New()
	baseCfg := &chain.Config{ChainID: uint256.NewInt(1), OsakaTime: common.NewUint64(1)}
	gspec := &types.Genesis{Config: baseCfg}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	chainBlocks, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainBlocks))

	overrideOsakaTime := common.NewUint64(500)
	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(m.DB, nil, "", overrideOsakaTime, nil, true, datadir.New(t.TempDir()), logger)

	require.Error(t, err)
	var compatErr *chain.ConfigCompatError
	require.ErrorAs(t, err, &compatErr, "want *chain.ConfigCompatError, got %T: %v", err, err)
	require.Equal(t, "Osaka fork timestamp", compatErr.WhatTime)
	require.True(t, compatErr.HasTimestampConflict())
	require.Zero(t, compatErr.RewindToTime)
	require.Equal(t, origOsakaTime, chain.AllProtocolChanges.OsakaTime,
		"applyOverrides must not write through into the shared config")
}

// dropHeadHeader deletes only the kv.Headers row, leaving HeadHeaderKey and the
// kv.HeaderNumber marker as FillDBFromSnapshots leaves them.
func dropHeadHeader(t *testing.T, db kv.RwDB) {
	t.Helper()
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		height := rawdb.ReadHeaderNumber(tx, headHash)
		require.NotNil(t, height)
		require.NotZero(t, *height)
		require.NoError(t, tx.Delete(kv.Headers, dbutils.HeaderKey(*height, headHash)))
		require.Nil(t, rawdb.ReadHeader(tx, headHash, *height), "the header must be gone")
		require.NotNil(t, rawdb.ReadHeaderNumber(tx, headHash), "its number marker must survive")
		return nil
	}))
}

func readStoredChainConfig(t *testing.T, db kv.RoDB) *chain.Config {
	t.Helper()
	var cfg *chain.Config
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cfg, err = rawdb.ReadChainConfig(tx, genesisHash)
		return err
	}))
	require.NotNil(t, cfg)
	return cfg
}

// A snapshot-synced datadir carries the head's kv.HeaderNumber marker and HeadHeaderKey
// without the header itself: FillDBFromSnapshots writes the markers and never kv.Headers.
// Defaulting the head time to 0 there reads every post-genesis fork as inactive.
func TestCommitGenesisBlockHeadHeaderOutsideTheDB(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	logger := log.New()
	gspec := &types.Genesis{Config: &chain.Config{ChainID: uint256.NewInt(1), OsakaTime: common.NewUint64(1)}}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	chainBlocks, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainBlocks))

	dropHeadHeader(t, m.DB)

	// No block snapshots in this datadir either, so the head time is unknowable and the
	// rescheduled Osaka cannot be cleared.
	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(
		m.DB, nil, "", common.NewUint64(500), nil, true, datadir.New(t.TempDir()), logger)
	require.Error(t, err, "an unresolvable head time must not let a rescheduled fork through")

	storedCfg := readStoredChainConfig(t, m.DB)
	require.Equal(t, uint64(1), *storedCfg.OsakaTime, "the stored schedule must be left alone")
}

// An unreadable head header is only fatal to a fork that moved: with the timestamp
// schedule unchanged the head's time cannot change the answer, so the node still starts.
func TestCommitGenesisBlockHeadHeaderOutsideTheDBUnchangedSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	logger := log.New()
	gspec := &types.Genesis{Config: &chain.Config{ChainID: uint256.NewInt(1), OsakaTime: common.NewUint64(1)}}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	chainBlocks, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainBlocks))
	dropHeadHeader(t, m.DB)

	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(
		m.DB, nil, "", nil, nil, true, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
}

func TestAllocConstructor(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)

	// This deployment code initially sets contract's 0th storage to 0x2a
	// and its 1st storage to 0x01c9.
	deploymentCode := common.FromHex("602a5f556101c960015560048060135f395ff35f355f55")

	funds := big.NewInt(1000000000)
	address := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000001"))
	genSpec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			address.Value(): {Constructor: deploymentCode, Balance: funds},
		},
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genSpec), execmoduletester.WithKey(key))

	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(err)
	defer tx.Rollback()

	//TODO: support historyV3
	reader, err := rpchelper.CreateHistoryStateReader(ctx, tx, 1, 0, rawdbv3.TxNums)
	require.NoError(err)
	state := state.New(reader)
	defer state.Close()
	balance, err := state.GetBalance(address)
	require.NoError(err)
	assert.Equal(funds, balance.ToBig())
	code, err := state.GetCode(address)
	require.NoError(err)
	assert.Equal(common.FromHex("5f355f55"), code)

	key0 := accounts.InternKey(common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000"))
	storage0, err := state.GetState(address, key0)
	require.NoError(err)
	assert.Equal(u256.U64(0x2a), storage0)
	key1 := accounts.InternKey(common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001"))
	storage1, err := state.GetState(address, key1)
	require.NoError(err)
	assert.Equal(u256.U64(0x01c9), storage1)
}

// A genesis alloc carrying storage but otherwise EIP-161-empty must still be materialized as a present account (so its storage never sits under an absent account).
func TestGenesisStorageBearingEmptyAccountIsPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)

	address := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000c0ffee01"))
	slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	genSpec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			address.Value(): {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{slot: common.HexToHash("0x2a")}},
		},
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genSpec), execmoduletester.WithKey(key))

	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(err)
	defer tx.Rollback()

	// Ground truth for DomainDel's guard: the accounts domain must hold a
	// non-empty entry for this address (not dangling storage under an absent account).
	av := address.Value()
	acc, _, err := tx.GetLatest(kv.AccountsDomain, av[:], kv.GetLatestOptions{})
	require.NoError(err)
	require.NotEmpty(acc, "storage-bearing empty alloc must produce a present accounts-domain entry")

	// High-level view agrees: account exists, storage is set, balance/nonce/code stay empty.
	reader, err := rpchelper.CreateHistoryStateReader(ctx, tx, 1, 0, rawdbv3.TxNums)
	require.NoError(err)
	st := state.New(reader)

	exist, err := st.Exist(address)
	require.NoError(err)
	assert.True(exist, "storage-bearing empty account must exist")

	bal, err := st.GetBalance(address)
	require.NoError(err)
	assert.True(bal.IsZero(), "balance stays zero")

	nonce, err := st.GetNonce(address)
	require.NoError(err)
	assert.Zero(nonce, "nonce stays zero")

	got, err := st.GetState(address, accounts.InternKey(slot))
	require.NoError(err)
	assert.Equal(u256.U64(0x2a), got, "storage slot must be readable")
}

func TestAmsterdamGenesisCarriesSlotNumber(t *testing.T) {
	t.Parallel()

	// Deep copy: chain.Config carries a sync.Once and a memoized map, and its own doc
	// forbids copying it by value.
	var cfg chain.Config
	require.NoError(t, copier.CopyWithOption(&cfg, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
	zero := uint64(0)
	cfg.AmsterdamTime = &zero
	head, _ := genesiswrite.GenesisWithoutStateToBlock(&types.Genesis{Config: &cfg})

	// merge.VerifyHeader rejects an Amsterdam header without one (ErrMissingSlotNumber),
	// and the genesis hash depends on it.
	require.NotNil(t, head.SlotNumber, "Amsterdam genesis header must carry slotNumber")
	require.Zero(t, *head.SlotNumber)

	cfg.AmsterdamTime = nil
	head, _ = genesiswrite.GenesisWithoutStateToBlock(&types.Genesis{Config: &cfg})
	require.Nil(t, head.SlotNumber, "pre-Amsterdam genesis header must not carry slotNumber")
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
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	var (
		customghash = common.HexToHash("0x89c99d90b79719238d2645c7642f2c9295246e80775b38cfd162b696817fbd50")
		customg     = types.Genesis{
			Config: &chain.Config{ChainID: uint256.NewInt(1), HomesteadBlock: common.NewUint64(3)},
			Alloc: types.GenesisAlloc{
				{1}: {Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{{1}: {1}}},
			},
		}
		oldcustomg = customg
	)
	logger := log.New()
	oldcustomg.Config = &chain.Config{ChainID: uint256.NewInt(1), HomesteadBlock: common.NewUint64(2)}

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
				return genesiswrite.CommitGenesisBlock(db, new(types.Genesis), "", datadir.New(tmpdir), logger)
			},
			wantErr:    types.ErrGenesisNoConfig,
			wantConfig: chain.AllProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				return genesiswrite.CommitGenesisBlock(db, nil, networkname.Mainnet, datadir.New(tmpdir), logger)
			},
			wantHash:   chainspec.Mainnet.GenesisHash,
			wantConfig: chainspec.Mainnet.Config,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				return genesiswrite.CommitGenesisBlock(db, nil, networkname.Mainnet, datadir.New(tmpdir), logger)
			},
			wantHash:   chainspec.Mainnet.GenesisHash,
			wantConfig: chainspec.Mainnet.Config,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, nil, "", datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			// Reproduces the hive EEST consume-rlp scenario:
			// 1. `erigon init genesis.json` writes a custom genesis + config
			// 2. `erigon --import` reopens the DB with genesis=nil, chainName="mainnet" (default)
			// The custom config must be preserved, not overwritten with mainnet's.
			name: "custom block in DB, genesis == nil, chainName mainnet",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, nil, networkname.Mainnet, datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == sepolia",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&customg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, chainspec.SepoliaGenesisBlock(), networkname.Sepolia, datadir.New(tmpdir), logger)
			},
			wantErr:    &genesiswrite.GenesisMismatchError{Stored: customghash, New: chainspec.Sepolia.GenesisHash},
			wantHash:   chainspec.Sepolia.GenesisHash,
			wantConfig: chainspec.Sepolia.Config,
		},
		{
			name: "compatible config in DB",
			fn: func(t *testing.T, db kv.RwDB, tmpdir string) (*chain.Config, *types.Block, error) {
				genesiswrite.MustCommitGenesis(&oldcustomg, db, datadir.New(tmpdir), logger)
				return genesiswrite.CommitGenesisBlock(db, &customg, "", datadir.New(tmpdir), logger)
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
				m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&oldcustomg), execmoduletester.WithKey(key))

				chainBlocks, err := m.GenerateChain(4, nil)
				if err != nil {
					return nil, nil, err
				}
				if err := m.InsertChain(chainBlocks); err != nil {
					return nil, nil, err
				}
				// This should return a compatibility error.
				return genesiswrite.CommitGenesisBlock(m.DB, &customg, "", datadir.New(tmpdir), logger)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
			wantErr: &chain.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: common.NewUint64(2),
				NewConfig:    common.NewUint64(3),
				RewindTo:     1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpdir := t.TempDir()
			dirs := datadir.New(tmpdir)
			db := temporaltest.NewTestDB(t, dirs)
			blockReader := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles())
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
