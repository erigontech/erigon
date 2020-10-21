package stagedsync

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/assert"
)

func generateFakeBlocks(from, to int) (*types.Header, []*types.Header) {
	parentHash := types.EmptyRootHash
	origin := &types.Header{
		ParentHash: parentHash,
		UncleHash:  types.EmptyUncleHash,
		Root:       types.EmptyRootHash,
		Difficulty: big.NewInt(int64(0)),
		Number:     big.NewInt(int64(0)),
		GasLimit:   6000,
	}
	parentHash = origin.Hash()
	parent := origin
	result := make([]*types.Header, 0)
	for i := 1; i <= to; i++ {
		time := uint64(i)
		difficulty := ethash.CalcDifficulty(
			params.AllEthashProtocolChanges,
			time,
			parent.Time,
			parent.Difficulty,
			parent.Number,
			parent.UncleHash,
		)

		header := &types.Header{
			ParentHash: parentHash,
			UncleHash:  types.EmptyUncleHash,
			Root:       types.EmptyRootHash,
			Difficulty: difficulty,
			Number:     big.NewInt(int64(i)),
			GasLimit:   6000,
			Time:       time,
		}
		parentHash = header.Hash()
		parent = header
		if i >= from {
			result = append(result, header)
		}
	}
	return origin, result
}

func TestInsertHeaderChainTotalDifficulty(t *testing.T) {
	// this test makes sure that we calculate total difficulty correctly even when
	// we try to insert overlapping chains of headers

	origin, headers := generateFakeBlocks(1, 4)

	headers1 := headers[:3] // 1,2,3
	headers2 := headers[1:] // 2,3,4

	lastHeader1 := headers1[len(headers1)-1]
	expectedTdBlock3 := big.NewInt(0) // blocks 1,2,3
	for _, h := range headers1 {
		expectedTdBlock3.Add(expectedTdBlock3, h.Difficulty)
	}

	lastHeader2 := headers2[len(headers2)-1]
	expectedTdBlock4 := big.NewInt(0) // blocks 1,2,3,4
	for _, h := range headers {
		expectedTdBlock4.Add(expectedTdBlock4, h.Difficulty)
	}

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty)
	rawdb.WriteHeader(context.TODO(), db, origin)
	rawdb.WriteHeadHeaderHash(db, origin.Hash())
	rawdb.WriteCanonicalHash(db, origin.Hash(), 0)

	reorg, _, err := InsertHeaderChain("logPrefix", db, headers1, params.AllEthashProtocolChanges, ethash.NewFaker(), 0)
	assert.NoError(t, err)
	assert.False(t, reorg)

	td := rawdb.ReadTd(db, lastHeader1.Hash(), lastHeader1.Number.Uint64())
	assert.Equal(t, expectedTdBlock3, td)

	reorg, _, err = InsertHeaderChain("logPrefix", db, headers2, params.AllEthashProtocolChanges, ethash.NewFaker(), 0)
	assert.False(t, reorg)
	assert.NoError(t, err)

	td = rawdb.ReadTd(db, lastHeader2.Hash(), lastHeader2.Number.Uint64())

	assert.Equal(t, expectedTdBlock4, td)

	reorg, _, err = InsertHeaderChain("logPrefix", db, headers2, params.AllEthashProtocolChanges, ethash.NewFaker(), 0)
	assert.False(t, reorg)
	assert.NoError(t, err)

	td = rawdb.ReadTd(db, lastHeader2.Hash(), lastHeader2.Number.Uint64())
	assert.Equal(t, expectedTdBlock4, td)
}
