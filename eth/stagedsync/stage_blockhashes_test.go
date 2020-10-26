package stagedsync

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashStage(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty)
	rawdb.WriteHeadHeaderHash(db, origin.Hash())
	rawdb.WriteCanonicalHeader(db, origin)

	_, _, err := InsertHeaderChain(db, headers, params.AllEthashProtocolChanges, ethash.NewFaker(), 0)
	assert.NoError(t, err)
	err = SpawnBlockHashStage(&StageState{Stage: stages.BlockHashes}, db, "", nil)
	assert.NoError(t, err)
	for _, h := range headers {
		n := rawdb.ReadHeaderNumber(db, h.Hash())
		assert.Equal(t, *n, h.Number.Uint64())
	}

}
