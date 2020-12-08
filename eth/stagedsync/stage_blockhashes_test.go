package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashStage(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	if err := rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty); err != nil {
		panic(err)
	}
	rawdb.WriteHeader(context.TODO(), db, origin)
	rawdb.WriteHeadHeaderHash(db, origin.Hash())
	if err := rawdb.WriteCanonicalHash(db, origin.Hash(), 0); err != nil {
		panic(err)
	}

	_, _, _, err := InsertHeaderChain("logPrefix", db, headers)
	assert.NoError(t, err)
	err = SpawnBlockHashStage(&StageState{Stage: stages.BlockHashes}, db, "", nil)
	assert.NoError(t, err)
	for _, h := range headers {
		n := rawdb.ReadHeaderNumber(db, h.Hash())
		assert.Equal(t, *n, h.Number.Uint64())
	}

}
