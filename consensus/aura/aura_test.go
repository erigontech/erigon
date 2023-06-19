package aura_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages"
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
	m := stages.MockWithGenesisEngine(t, genesis, engine, false)

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
