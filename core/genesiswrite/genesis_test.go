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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
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

		_, block, err := genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, datadir.New(t.TempDir()), logger)
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

	block, _, err := genesiswrite.GenesisToBlock(chainspec.MainnetGenesisBlock(), datadir.New(t.TempDir()), log.Root())
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

		block, _, err = genesiswrite.GenesisToBlock(spec.Genesis, datadir.New(t.TempDir()), log.Root())
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
	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = genesiswrite.WriteGenesisBlock(tx, spec.Genesis, nil, datadir.New(t.TempDir()), logger)
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
