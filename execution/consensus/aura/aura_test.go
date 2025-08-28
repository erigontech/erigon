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

package aura_test

import (
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/abi"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/consensus/aura"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/execution/types"
)

// Check that the first block of Gnosis Chain, which doesn't have any transactions,
// does not change the state root.
func TestEmptyBlock(t *testing.T) {
	require := require.New(t)
	genesis := chainspec.GnosisGenesisBlock()
	genesisBlock, _, err := genesiswrite.GenesisToBlock(genesis, datadir.New(t.TempDir()), log.Root())
	require.NoError(err)

	genesis.Config.TerminalTotalDifficultyPassed = false

	chainConfig := genesis.Config
	auraDB := memdb.NewTestDB(t, kv.ChainDB)
	engine, err := aura.NewAuRa(chainConfig.Aura, auraDB)
	require.NoError(err)
	m := mock.MockWithGenesisEngine(t, genesis, engine, false)

	time := uint64(1539016985)
	header := core.MakeEmptyHeader(genesisBlock.Header(), chainConfig, time, nil)
	header.UncleHash = empty.UncleHash
	header.TxHash = trie.EmptyRoot
	header.ReceiptHash = trie.EmptyRoot
	header.Coinbase = common.HexToAddress("0xcace5b3c29211740e595850e80478416ee77ca21")
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
	err = m.InsertChain(chain)
	require.NoError(err)
}

func TestAuRaSkipGasLimit(t *testing.T) {
	require := require.New(t)
	genesis := chainspec.GnosisGenesisBlock()
	genesis.Config.TerminalTotalDifficultyPassed = false
	genesis.Config.Aura.BlockGasLimitContractTransitions = map[uint64]common.Address{0: common.HexToAddress("0x4000000000000000000000000000000000000001")}

	chainConfig := genesis.Config
	auraDB := memdb.NewTestDB(t, kv.ChainDB)
	engine, err := aura.NewAuRa(chainConfig.Aura, auraDB)
	require.NoError(err)
	m := mock.MockWithGenesisEngine(t, genesis, engine, false)

	difficlty, _ := new(big.Int).SetString("340282366920938463463374607431768211454", 10)
	//Populate a sample valid header for a Pre-merge block
	// - actually sampled from 5000th block in chiado
	validPreMergeHeader := &types.Header{
		ParentHash:  common.HexToHash("0x102482332de853f2f8967263e77e71d4fddf68fd5d84b750b2ddb7e501052097"),
		UncleHash:   common.HexToHash("0x0"),
		Coinbase:    common.HexToAddress("0x14747a698Ec1227e6753026C08B29b4d5D3bC484"),
		Root:        common.HexToHash("0x0"),
		TxHash:      common.HexToHash("0x0"),
		ReceiptHash: common.HexToHash("0x0"),
		Bloom:       types.BytesToBloom(nil),
		Difficulty:  difficlty,
		Number:      big.NewInt(5000),
		GasLimit:    12500000,
		GasUsed:     0,
		Time:        1664049551,
		Extra:       []byte{},
		Nonce:       [8]byte{0, 0, 0, 0, 0, 0, 0, 0},
	}

	syscallCustom := func(common.Address, []byte, *state.IntraBlockState, *types.Header, bool) ([]byte, error) {
		//Packing as constructor gives the same effect as unpacking the returned value
		json := `[{"inputs": [{"internalType": "uint256","name": "blockGasLimit","type": "uint256"}],"stateMutability": "nonpayable","type": "constructor"}]`
		fakeAbi, err := abi.JSON(strings.NewReader(json))
		require.NoError(err)

		fakeVal, err := fakeAbi.Pack("", big.NewInt(12500000))
		return fakeVal, err
	}
	require.NotPanics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, validPreMergeHeader, nil, syscallCustom, nil, nil)
	})

	invalidPreMergeHeader := validPreMergeHeader
	invalidPreMergeHeader.GasLimit = 12_123456 //a different, wrong gasLimit
	require.Panics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, invalidPreMergeHeader, nil, syscallCustom, nil, nil)
	})

	invalidPostMergeHeader := invalidPreMergeHeader
	invalidPostMergeHeader.Difficulty = big.NewInt(0) //zero difficulty detected as PoS
	require.NotPanics(func() {
		m.Engine.Initialize(chainConfig, &core.FakeChainReader{}, invalidPostMergeHeader, nil, syscallCustom, nil, nil)
	})
}
