package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/process"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func TestBlockHashStage(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty)
	rawdb.WriteHeader(context.TODO(), db, origin)
	rawdb.WriteHeadHeaderHash(db, origin.Hash())
	rawdb.WriteCanonicalHash(db, origin.Hash(), 0)

	eng := process.NewRemoteEngine(ethash.NewFaker(), params.AllEthashProtocolChanges)
	defer eng.Close()

	_, _, err := InsertHeaderChain(db, headers, eng, 0)
	assert.NoError(t, err)
	err = SpawnBlockHashStage(&StageState{Stage: stages.BlockHashes}, db, "", nil)
	assert.NoError(t, err)
	for _, h := range headers {
		n := rawdb.ReadHeaderNumber(db, h.Hash())
		assert.Equal(t, *n, h.Number.Uint64())
	}
}
