package core_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/rlp"
)

func TestGenesisBlockHashes(t *testing.T) {
	db := memdb.NewTestDB(t)
	check := func(network string) {
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := core.WriteGenesisBlock(tx, genesis, nil, "")
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.Equal(t, block.Hash().Bytes(), expect.Bytes(), network)
	}
	for _, network := range networkname.All {
		check(network)
	}
}

func TestGenesisBlockRoots(t *testing.T) {
	require := require.New(t)
	var err error

	block, _, _ := core.GenesisToBlock(core.MainnetGenesisBlock(), "")
	if block.Hash() != params.MainnetGenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), params.MainnetGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.SokolGenesisBlock(), "")
	require.NoError(err)
	if block.Root() != params.SokolGenesisStateRoot {
		t.Errorf("wrong Sokol genesis state root, got %v, want %v", block.Root(), params.SokolGenesisStateRoot)
	}
	if block.Hash() != params.SokolGenesisHash {
		t.Errorf("wrong Sokol genesis hash, got %v, want %v", block.Hash(), params.SokolGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.GnosisGenesisBlock(), "")
	require.NoError(err)
	if block.Root() != params.GnosisGenesisStateRoot {
		t.Errorf("wrong Gnosis Chain genesis state root, got %v, want %v", block.Root(), params.GnosisGenesisStateRoot)
	}
	if block.Hash() != params.GnosisGenesisHash {
		t.Errorf("wrong Gnosis Chain genesis hash, got %v, want %v", block.Hash(), params.GnosisGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.ChiadoGenesisBlock(), "")
	require.NoError(err)
	if block.Root() != params.ChiadoGenesisStateRoot {
		t.Errorf("wrong Chiado genesis state root, got %v, want %v", block.Root(), params.ChiadoGenesisStateRoot)
	}
	if block.Hash() != params.ChiadoGenesisHash {
		t.Errorf("wrong Chiado genesis hash, got %v, want %v", block.Hash(), params.ChiadoGenesisHash)
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	genesis := core.GenesisBlockByChainName(networkname.MainnetChainName)
	_, _, err := core.WriteGenesisBlock(tx, genesis, nil, "")
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "")
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}

func TestSokolHeaderRLP(t *testing.T) {
	require := require.New(t)
	{ //sokol
		expect := common.FromHex("f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
		block, _, err := core.GenesisToBlock(core.SokolGenesisBlock(), "")
		require.NoError(err)
		b, err := rlp.EncodeToBytes(block.Header())
		require.NoError(err)
		require.Equal(expect, b)
		h := &types.Header{}
		err = rlp.DecodeBytes(expect, h)
		require.NoError(err)

		expectSeal := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
		require.Equal(uint64(0), h.AuRaStep)
		require.Equal(expectSeal, h.AuRaSeal)

		enc, err := rlp.EncodeToBytes(h)
		require.NoError(err)
		require.Equal(expect, enc)
	}

	{ // sokol, more seals
		h := &types.Header{}
		enc := common.FromHex("f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808002b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")
		err := rlp.DecodeBytes(enc, h)
		require.NoError(err)
		require.Equal(uint64(2), h.AuRaStep)

		expectSeal := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")
		require.Equal(expectSeal, h.AuRaSeal)

		res, err := rlp.EncodeToBytes(h) // after encode getting source bytes
		require.NoError(err)
		require.Equal(enc, res)
	}

	{ // ethash
		expect := common.FromHex("f901f9a0d405da4e66f1445d455195229624e133f5baafe72b5cf7b3c36c12c8146e98b7a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a05fb2b4bfdef7b314451cb138a534d225c922fc0e5fbe25e451142732c3e25c25a088d2ec6b9860aae1a2c3b299f72b6a5d70d7f7ba4722c78f2c49ba96273c2158a007c6fdfa8eea7e86b81f5b0fc0f78f90cc19f4aa60d323151e0cac660199e9a1b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302008003832fefba82524d84568e932a80a0a0349d8c3df71f1a48a9df7d03fd5f14aeee7d91332c009ecaff0a71ead405bd88ab4e252a7e8c2a23")
		h := &types.Header{}
		err := rlp.DecodeBytes(expect, h)
		require.NoError(err)

		res, err := rlp.EncodeToBytes(h) // after encode getting source bytes
		require.NoError(err)
		require.Equal(expect, res)
	}
}

func TestAllocConstructor(t *testing.T) {
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
	db := memdb.NewTestDB(t)
	defer db.Close()
	_, _, err := core.CommitGenesisBlock(db, genSpec, "")
	require.NoError(err)

	tx, err := db.BeginRo(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	//TODO: support historyV3
	reader, err := rpchelper.CreateHistoryStateReader(tx, 1, 0, false, genSpec.Config.ChainName)
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
