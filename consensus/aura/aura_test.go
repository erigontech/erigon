package aura_test

import (
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

// Check that the first block of Gnosis Chain, which doesn't have any transactions,
// does not change the state root.
func TestEmptyBlock(t *testing.T) {
	require := require.New(t)
	genesis := core.GnosisGenesisBlock()
	genesisBlock, _, err := core.GenesisToBlock(genesis, "")
	require.NoError(err)

	genesis.Config.TerminalTotalDifficultyPassed = false

	chainConfig := genesis.Config
	auraDB := memdb.NewTestDB(t)
	engine, err := aura.NewAuRa(chainConfig.Aura, auraDB)
	require.NoError(err)
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, genesis, engine, false, checkStateRoot)

	time := uint64(1539016985)
	header := core.MakeEmptyHeader(genesisBlock.Header(), chainConfig, time, nil)
	header.UncleHash = types.EmptyUncleHash
	header.TxHash = trie.EmptyRoot
	header.ReceiptHash = trie.EmptyRoot
	header.Coinbase = libcommon.HexToAddress("0xcace5b3c29211740e595850e80478416ee77ca21")
	header.Difficulty = engine.CalcDifficulty(nil, time,
		0,
		genesisBlock.Difficulty(),
		genesisBlock.NumberU64(),
		genesisBlock.Hash(),
		genesisBlock.UncleHash(),
		genesisBlock.Header().AuRaStep,
	)

	block := types.NewBlockWithHeader(header)

	headers, blocks, receipts := make([]*types.Header, 1), make(types.Blocks, 1), make([]types.Receipts, 1)
	headers[0] = header
	blocks[0] = block

	chain := &core.ChainPack{Headers: headers, Blocks: blocks, Receipts: receipts, TopBlock: block}
	err = m.InsertChain(chain, nil)
	require.NoError(err)
}

func TestAuRaSkipGasLimit(t *testing.T) {
	require := require.New(t)
	genesis := core.GnosisGenesisBlock()
	genesis.Config.TerminalTotalDifficultyPassed = false
	genesis.Config.Aura.BlockGasLimitContractTransitions = map[uint64]libcommon.Address{0: libcommon.HexToAddress("0x4000000000000000000000000000000000000001")}

	chainConfig := genesis.Config
	auraDB := memdb.NewTestDB(t)
	engine, err := aura.NewAuRa(chainConfig.Aura, auraDB)
	require.NoError(err)
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, genesis, engine, false, checkStateRoot)

	difficlty, _ := new(big.Int).SetString("340282366920938463463374607431768211454", 10)
	//Populate a sample valid header for a Pre-merge block
	// - actually sampled from 5000th block in chiado
	validPreMergeHeader := &types.Header{
		ParentHash:  libcommon.HexToHash("0x102482332de853f2f8967263e77e71d4fddf68fd5d84b750b2ddb7e501052097"),
		UncleHash:   libcommon.HexToHash("0x0"),
		Coinbase:    libcommon.HexToAddress("0x14747a698Ec1227e6753026C08B29b4d5D3bC484"),
		Root:        libcommon.HexToHash("0x0"),
		TxHash:      libcommon.HexToHash("0x0"),
		ReceiptHash: libcommon.HexToHash("0x0"),
		Bloom:       types.BytesToBloom(nil),
		Difficulty:  difficlty,
		Number:      big.NewInt(5000),
		GasLimit:    12500000,
		GasUsed:     0,
		Time:        1664049551,
		Extra:       []byte{},
		Nonce:       [8]byte{0, 0, 0, 0, 0, 0, 0, 0},
	}

	syscallCustom := func(libcommon.Address, []byte, *state.IntraBlockState, *types.Header, bool) ([]byte, error) {
		//Packing as constructor gives the same effect as unpacking the returned value
		json := `[{"inputs": [{"internalType": "uint256","name": "blockGasLimit","type": "uint256"}],"stateMutability": "nonpayable","type": "constructor"}]`
		fakeAbi, err := abi.JSON(strings.NewReader(json))
		require.NoError(err)

		fakeVal, err := fakeAbi.Pack("", big.NewInt(12500000))
		return fakeVal, err
	}
	require.NotPanics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, validPreMergeHeader, nil, syscallCustom)
	})

	invalidPreMergeHeader := validPreMergeHeader
	invalidPreMergeHeader.GasLimit = 12_123456 //a different, wrong gasLimit
	require.Panics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, invalidPreMergeHeader, nil, syscallCustom)
	})

	invalidPostMergeHeader := invalidPreMergeHeader
	invalidPostMergeHeader.Difficulty = big.NewInt(0) //zero difficulty detected as PoS
	require.NotPanics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, invalidPostMergeHeader, nil, syscallCustom)
	})
}
