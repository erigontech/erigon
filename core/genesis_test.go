package core_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

func TestGenesisBlockHashes(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	check := func(network string) {
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := core.WriteGenesisBlock(tx, genesis, nil, "", logger)
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.EqualValues(t, block.Hash(), *expect, network)
	}
	for _, network := range networkname.All {
		check(network)
	}
}

func TestGenesisBlockRoots(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	var err error

	block, _, _ := core.GenesisToBlock(core.MainnetGenesisBlock(), "", log.Root())
	if block.Hash() != params.MainnetGenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), params.MainnetGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.GnosisGenesisBlock(), "", log.Root())
	require.NoError(err)
	if block.Root() != params.GnosisGenesisStateRoot {
		t.Errorf("wrong Gnosis Chain genesis state root, got %v, want %v", block.Root(), params.GnosisGenesisStateRoot)
	}
	if block.Hash() != params.GnosisGenesisHash {
		t.Errorf("wrong Gnosis Chain genesis hash, got %v, want %v", block.Hash(), params.GnosisGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.ChiadoGenesisBlock(), "", log.Root())
	require.NoError(err)
	if block.Root() != params.ChiadoGenesisStateRoot {
		t.Errorf("wrong Chiado genesis state root, got %v, want %v", block.Root(), params.ChiadoGenesisStateRoot)
	}
	if block.Hash() != params.ChiadoGenesisHash {
		t.Errorf("wrong Chiado genesis hash, got %v, want %v", block.Hash(), params.ChiadoGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.TestGenesisBlock(), "", log.Root())
	require.NoError(err)
	if block.Root() != params.TestGenesisStateRoot {
		t.Errorf("wrong Chiado genesis state root, got %v, want %v", block.Root(), params.TestGenesisStateRoot)
	}
	if block.Hash() != params.TestGenesisHash {
		t.Errorf("wrong Chiado genesis hash, got %v, want %v", block.Hash(), params.TestGenesisHash)
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	genesis := core.GenesisBlockByChainName(networkname.MainnetChainName)
	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "", logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "", logger)
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
	address := libcommon.HexToAddress("0x1000000000000000000000000000000000000001")
	genSpec := &types.Genesis{
		Config: params.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			address: {Constructor: deploymentCode, Balance: funds},
		},
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := mock.MockWithGenesis(t, genSpec, key, false)

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	//TODO: support historyV3
	reader, err := rpchelper.CreateHistoryStateReader(tx, 1, 0, m.HistoryV3, genSpec.Config.ChainName)
	require.NoError(err)
	state := state.New(reader)
	balance := state.GetBalance(address)
	assert.Equal(funds, balance.ToBig())
	code := state.GetCode(address)
	assert.Equal(common.FromHex("5f355f55"), code)

	key0 := libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000000")
	storage0 := &uint256.Int{}
	state.GetState(address, &key0, storage0)
	assert.Equal(uint256.NewInt(0x2a), storage0)
	key1 := libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000001")
	storage1 := &uint256.Int{}
	state.GetState(address, &key1, storage1)
	assert.Equal(uint256.NewInt(0x01c9), storage1)
}
